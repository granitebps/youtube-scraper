"""Read-only MCP adapter for the existing YouTube scraper functions."""
import argparse
import asyncio
import base64
import json
import logging
import math
import os
import threading
import time
from collections import OrderedDict
from typing import Annotated, Literal

import anyio
from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse

import config
from schema_fields import load_query
from scraper_errors import BadRequest, Blocked, NotFound, UpstreamError
from youtube import cache_config, channels, discover, playlists, search, schemas, videos
from youtube.fetch import YoutubeTimeout, deadline, _remaining


def _setting(name, default, cast, minimum):
    try:
        value = cast(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a valid number")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


HOST = os.environ.get("MCP_HOST", "127.0.0.1")
TOOL_TIMEOUT = _setting("MCP_TOOL_TIMEOUT", 45, float, 0.001)
MAX_CONCURRENCY = _setting("MCP_MAX_CONCURRENCY", 4, int, 1)
CACHE_SIZE = _setting("MCP_CACHE_SIZE", 500, int, 0)
DEFAULT_LIMIT = 5
MAX_LIMIT = 12
MAX_TRANSCRIPT_CHARS = 20_000
MAX_TRANSCRIPT_SEGMENTS = 200
log = logging.getLogger("youtube_mcp")


class TTLCache:
    def __init__(self, size):
        self.size = size
        self._data = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            hit = self._data.get(key)
            if hit is None:
                return None
            if hit[0] <= time.monotonic():
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return hit[1]

    def set(self, key, value, ttl):
        if self.size <= 0:
            return
        with self._lock:
            self._data[key] = (time.monotonic() + ttl.total_seconds(), value)
            self._data.move_to_end(key)
            while len(self._data) > self.size:
                self._data.popitem(last=False)


_cache = TTLCache(CACHE_SIZE)
_limiter = None


def _get_limiter():
    global _limiter
    if _limiter is None:
        _limiter = anyio.CapacityLimiter(MAX_CONCURRENCY)
    return _limiter


class ToolFailure(Exception):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable


def _result(data, is_error=False):
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(data, ensure_ascii=False, separators=(",", ":")))],
        structuredContent=data,
        isError=is_error,
    )


def _error(code, message, retryable=False):
    return _result({"error": {"code": code, "message": message, "retryable": retryable}}, is_error=True)


def _validate(schema, params):
    data, error = load_query(schema, {k: v for k, v in params.items() if v is not None})
    if error:
        raise ToolFailure("invalid_input", error.get("error", "invalid input"))
    return data


def _copy(value):
    return json.loads(json.dumps(value, ensure_ascii=False))


def _fetch(tool, fn, data, ttl, *, cache=True):
    key = (tool, json.dumps(data, sort_keys=True, ensure_ascii=False, default=str))
    hit = _cache.get(key) if cache else None
    if hit is not None:
        return _copy(hit)
    value = fn(**data)
    _remaining()
    if tool == "get_video_transcript" and not any(
        (segment.get("text") or "").strip() for segment in value.get("segments") or []
    ):
        raise ToolFailure("no_transcript", "No usable transcript segments were returned")
    if cache:
        _cache.set(key, value, ttl)
    return value


def _split_cursor(cursor):
    if not cursor:
        return None, 0
    if not cursor.startswith("p:"):
        return cursor, 0
    try:
        _, offset, upstream = cursor.split(":", 2)
        if not offset.isdecimal():
            raise ValueError
        return upstream or None, int(offset)
    except ValueError:
        raise ToolFailure("invalid_input", "Pass next_cursor from the previous response unchanged")


def _paged(tool, fn, schema, params, ttl, items_key, limit):
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_LIMIT:
        raise ToolFailure("invalid_input", f"limit must be between 1 and {MAX_LIMIT}")
    upstream, offset = _split_cursor(params.pop("cursor", None))
    data = _validate(schema, {**params, "cursor": upstream})
    page = _copy(_fetch(tool, fn, data, ttl))
    all_items = page.get(items_key) or []
    if offset and offset >= len(all_items):
        raise ToolFailure("invalid_input", "This cached page changed; restart without cursor")
    items = all_items[offset:offset + limit]
    end = offset + len(items)
    upstream_next = page.pop("next_cursor", None)
    page["has_more"] = False
    if end < len(all_items):
        page["next_cursor"] = f"p:{end}:{upstream or ''}"
        page["has_more"] = True
    elif upstream_next and upstream_next != upstream:
        page["next_cursor"] = f"p:0:{upstream_next}"
        page["has_more"] = True
    page[items_key] = [_summary(item) for item in items]
    page["count"] = len(items)
    return page


def _summary(value):
    if isinstance(value, dict):
        return {key: _summary(item) for key, item in value.items()
                if key not in {"thumbnail", "profile_picture", "avatar", "banner"} and item is not None}
    if isinstance(value, list):
        return [_summary(item) for item in value]
    return value


def _encode_cursor(data):
    raw = json.dumps(data, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(value, video, language):
    if not value:
        return 0, 0
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        cursor = json.loads(raw)
        if (cursor["video"], cursor["language"]) != (video, language):
            raise ValueError
        index, offset = cursor["index"], cursor["offset"]
        if any(isinstance(n, bool) or not isinstance(n, int) or n < 0 for n in (index, offset)):
            raise ValueError
        return index, offset
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        raise ToolFailure("invalid_input", "Pass next_cursor from the previous response unchanged")


async def _run(tool, work):
    try:
        with deadline(TOOL_TIMEOUT), anyio.fail_after(TOOL_TIMEOUT):
            async with _get_limiter():
                result = await anyio.to_thread.run_sync(work)
        return _result(result)
    except (YoutubeTimeout, TimeoutError):
        return _error("timeout", "The YouTube request deadline expired", True)
    except ToolFailure as e:
        return _error(e.code, e.message, e.retryable)
    except ValueError as e:
        return _error("invalid_input", str(e))
    except NotFound:
        return _error("not_found", "The requested YouTube content was not found")
    except BadRequest:
        return _error("upstream_rejected", "YouTube rejected the request")
    except Blocked:
        return _error("upstream_blocked", "YouTube blocked the request; retry later or check server egress", True)
    except UpstreamError:
        return _error("upstream_unavailable", "YouTube did not return usable data", True)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        log.error("%s failed (%s)", tool, type(error).__name__)
        return _error("internal_error", f"{tool} failed unexpectedly")


READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True)
VideoRef = Annotated[str, Field(min_length=1, description="YouTube video or Short ID or link.")]
ChannelRef = Annotated[str, Field(min_length=1, description="Channel ID, @handle, custom URL or YouTube channel link.")]
PlaylistRef = Annotated[str, Field(min_length=1, description="Playlist ID or YouTube link containing it.")]
Cursor = Annotated[str | None, Field(description="Pass next_cursor unchanged with the same inputs.")]
Limit = Annotated[int, Field(ge=1, le=MAX_LIMIT, description="Items per response, 1-12; default 5.")]
server = MCPServer(
    name="youtube-scraper",
    version="0.1.0",
    instructions=("Read-only YouTube data through public logged-out endpoints. Lists are paginated; "
                  "pass next_cursor unchanged with the same filters. Video and subtitle links may expire."),
)


@server.tool(annotations=READ_ONLY, description="Search YouTube videos, Shorts, channels, playlists or movies with optional filters.")
async def search_videos(
    query: Annotated[str, Field(min_length=1, max_length=300)],
    type: Literal["all", "video", "short", "channel", "playlist", "movie"] = "video",
    upload_date: Literal["hour", "today", "week", "month", "year"] | None = None,
    duration: Literal["under_4_minutes", "4_to_20_minutes", "over_20_minutes"] | None = None,
    sort: Literal["relevance", "popularity"] = "relevance",
    features: list[Literal["hd", "subtitles", "creative_commons", "3d", "live", "purchased", "4k",
                           "360", "location", "hdr", "vr180"]] | None = None,
    country: str = "US",
    language: str = "en",
    cursor: Cursor = None,
    limit: Limit = DEFAULT_LIMIT,
) -> CallToolResult:
    def work():
        params = {"query": query, "type": type, "upload_date": upload_date, "duration": duration,
                  "sort": sort, "features": ",".join(features) if features else None,
                  "country": country, "language": language, "cursor": cursor}
        return _paged("search_videos", search.search, schemas.SearchSchema, params,
                      cache_config.SEARCH_CACHE, "results", limit)
    return await _run("search_videos", work)


@server.tool(annotations=READ_ONLY, description="Get video metadata, counts, captions, chapters and channel details.")
async def get_video(video: VideoRef, country: str = "US", language: str = "en") -> CallToolResult:
    return await _run("get_video", lambda: _fetch("get_video", videos.details,
                       _validate(schemas.VideoSchema, {"video": video, "country": country, "language": language}),
                       cache_config.VIDEO_CACHE))


@server.tool(annotations=READ_ONLY, description="Get available captions as bounded text or timed segments. Follow next_cursor for the rest.")
async def get_video_transcript(
    video: VideoRef,
    language: str | None = None,
    format: Literal["text", "segments"] = "text",
    cursor: Cursor = None,
    max_chars: Annotated[int, Field(ge=1, le=MAX_TRANSCRIPT_CHARS)] = 8000,
) -> CallToolResult:
    def work():
        data = _validate(schemas.TranscriptSchema, {"video": video, "language": language, "format": "json"})
        result = _fetch("get_video_transcript", videos.transcript, data, cache_config.TRANSCRIPT_CACHE)
        segments = result.get("segments") or []
        if not segments or not any(segment.get("text") for segment in segments):
            raise ToolFailure("no_transcript", "No usable transcript segments were returned")
        index, char_offset = _decode_cursor(cursor, data["video"], data.get("language"))
        if index >= len(segments):
            raise ToolFailure("invalid_input", "Pass next_cursor from the previous response unchanged")
        def chunk_at(position):
            # Include inter-segment spaces in the cursor budget so concatenating
            # pages reconstructs the same text even at a one-character limit.
            return (segments[position].get("text") or "") + (" " if position + 1 < len(segments) else "")
        if char_offset and char_offset >= len(chunk_at(index)):
            raise ToolFailure("invalid_input", "Pass next_cursor from the previous response unchanged")
        selected, remaining = [], max_chars
        while index < len(segments) and remaining > 0 and len(selected) < MAX_TRANSCRIPT_SEGMENTS:
            segment = dict(segments[index])
            text = chunk_at(index)
            piece = text[char_offset:char_offset + remaining]
            if piece:
                segment["text"] = piece
                if char_offset:
                    segment["text_offset"] = char_offset
                selected.append(segment)
                remaining -= len(piece)
            if char_offset + len(piece) >= len(text):
                index += 1
                char_offset = 0
            else:
                char_offset += len(piece)
                break
        has_more = index < len(segments)
        out = {k: v for k, v in result.items() if k not in ("segments", "text")}
        out.update({"format": format, "has_more": has_more,
                    "total_segment_count": len(segments), "count": len(selected)})
        if format == "segments":
            out["segments"] = selected
        else:
            out["text"] = "".join(s["text"] for s in selected)
        if has_more:
            out["next_cursor"] = _encode_cursor({"video": data["video"], "language": data.get("language"),
                                                 "index": index, "offset": char_offset})
        return out
    return await _run("get_video_transcript", work)


@server.tool(annotations=READ_ONLY, description="List subtitle tracks and their WebVTT links.")
async def get_video_subtitles(video: VideoRef) -> CallToolResult:
    return await _run("get_video_subtitles", lambda: _fetch("get_video_subtitles", videos.subtitles,
                       _validate(schemas.SubtitlesSchema, {"video": video}), cache_config.SUBTITLES_CACHE))


@server.tool(annotations=READ_ONLY, description="Get available audio and video stream links. Links are signed and expire.")
async def get_video_streams(video: VideoRef, country: str = "US") -> CallToolResult:
    return await _run("get_video_streams", lambda: _fetch("get_video_streams", videos.streams,
                       _validate(schemas.StreamsSchema, {"video": video, "country": country}),
                       cache_config.VIDEO_CACHE, cache=False))


@server.tool(annotations=READ_ONLY, description="Get one page of video comments; follow next_cursor for more.")
async def get_video_comments(video: VideoRef, sort: Literal["top", "newest"] = "top",
                             cursor: Cursor = None, country: str = "US", language: str = "en",
                             limit: Limit = DEFAULT_LIMIT) -> CallToolResult:
    def work():
        return _paged("get_video_comments", videos.comments, schemas.CommentsSchema,
                      {"video": video, "sort": sort, "cursor": cursor, "country": country, "language": language},
                      cache_config.COMMENTS_CACHE, "comments", limit)
    return await _run("get_video_comments", work)


@server.tool(annotations=READ_ONLY, description="Get one page of replies using a comment's replies_cursor.")
async def get_comment_replies(cursor: Annotated[str, Field(min_length=1)], country: str = "US",
                              language: str = "en", limit: Limit = DEFAULT_LIMIT) -> CallToolResult:
    def work():
        return _paged("get_comment_replies", videos.replies, schemas.RepliesSchema,
                      {"cursor": cursor, "country": country, "language": language},
                      cache_config.REPLIES_CACHE, "replies", limit)
    return await _run("get_comment_replies", work)


@server.tool(annotations=READ_ONLY, description="Get channel details and available tabs.")
async def get_channel(channel: ChannelRef, country: str = "US", language: str = "en") -> CallToolResult:
    return await _run("get_channel", lambda: _fetch("get_channel", channels.details,
                       _validate(schemas.ChannelSchema, {"channel": channel, "country": country, "language": language}),
                       cache_config.CHANNEL_CACHE))


@server.tool(annotations=READ_ONLY, description="Get channel videos, Shorts or live streams, with cursor pagination.")
async def get_channel_videos(
    channel: ChannelRef,
    tab: Literal["videos", "shorts", "live"] = "videos",
    sort: Literal["latest", "popular", "oldest"] = "latest",
    country: str = "US", language: str = "en", cursor: Cursor = None, limit: Limit = DEFAULT_LIMIT,
) -> CallToolResult:
    def work():
        fn = {"videos": channels.videos, "shorts": channels.shorts, "live": channels.live}[tab]
        return _paged(f"get_channel_{tab}", fn, schemas.ChannelVideosSchema,
                      {"channel": channel, "sort": sort, "country": country, "language": language, "cursor": cursor},
                      cache_config.CHANNEL_LIST_CACHE, "videos" if tab == "live" else tab, limit)
    return await _run("get_channel_videos", work)


@server.tool(annotations=READ_ONLY, description="Get playlist details.")
async def get_playlist(playlist: PlaylistRef, country: str = "US", language: str = "en") -> CallToolResult:
    return await _run("get_playlist", lambda: _fetch("get_playlist", playlists.details,
                       _validate(schemas.PlaylistSchema, {"playlist": playlist, "country": country, "language": language}),
                       cache_config.PLAYLIST_CACHE))


@server.tool(annotations=READ_ONLY, description="Get playlist videos with cursor pagination.")
async def get_playlist_videos(playlist: PlaylistRef, cursor: Cursor = None, country: str = "US",
                              language: str = "en", limit: Limit = DEFAULT_LIMIT) -> CallToolResult:
    def work():
        return _paged("get_playlist_videos", playlists.videos, schemas.PlaylistVideosSchema,
                      {"playlist": playlist, "cursor": cursor, "country": country, "language": language},
                      cache_config.PLAYLIST_CACHE, "videos", limit)
    return await _run("get_playlist_videos", work)


@server.tool(annotations=READ_ONLY, description="Resolve a YouTube link and return its type, ID and relevant metadata.")
async def resolve_link(link: Annotated[str, Field(min_length=1)]) -> CallToolResult:
    return await _run("resolve_link", lambda: _fetch("resolve_link", discover.resolve,
                       _validate(schemas.ResolveSchema, {"link": link}), cache_config.RESOLVE_CACHE))


@server.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


def main():
    parser = argparse.ArgumentParser(description="YouTube Scraper MCP server")
    parser.add_argument("--stdio", action="store_true", help="serve over stdio for a local MCP client")
    args = parser.parse_args()
    if args.stdio:
        server.run("stdio")
    else:
        server.run("streamable-http", host=HOST, port=config.PORT, stateless_http=True, json_response=True)


if __name__ == "__main__":
    main()
