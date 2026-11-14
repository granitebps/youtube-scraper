"""Qualify MCP discovery and representative YouTube calls over stdio or HTTP."""
import argparse
import json
import os
from pathlib import Path
import sys
import urllib.request

import anyio
from mcp import Client, StdioServerParameters

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TOOLS = {
    "search_videos", "get_video", "get_video_transcript", "get_video_subtitles", "get_video_streams",
    "get_video_comments", "get_comment_replies", "get_channel", "get_channel_videos", "get_playlist",
    "get_playlist_videos", "resolve_link",
}


def media_probe(url, headers, kind):
    request = urllib.request.Request(url, headers={"Range": "bytes=0-1023", **headers})
    with urllib.request.urlopen(request, timeout=15) as response:
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
        chunk = response.read(1024)
        if not 200 <= response.status < 300 or not content_type.startswith(kind + "/") or not chunk:
            raise RuntimeError("media response was not usable")
        return {"http_status": response.status, "content_type": content_type, "bytes_read": len(chunk)}


async def qualify(url, video, query, probe_media):
    target = url or StdioServerParameters(
        command=sys.executable, args=[str(ROOT / "mcp_server.py"), "--stdio"],
        cwd=str(ROOT), env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    async with Client(target, read_timeout_seconds=60) as client:
        tools = (await client.list_tools()).tools
        names = {tool.name for tool in tools}
        if names != EXPECTED_TOOLS:
            raise RuntimeError(f"Unexpected tool set: {sorted(names)}")
        if not all(tool.annotations.read_only_hint for tool in tools):
            raise RuntimeError("At least one tool lacks the read-only annotation")
        print(json.dumps({"check": "discovery", "status": "pass", "tools": sorted(names)}), flush=True)

        async def invoke(name, params):
            result = await client.call_tool(name, params)
            data = result.structured_content
            if result.is_error:
                code = (data or {}).get("error", {}).get("code", "tool_error")
                raise RuntimeError(code)
            if not isinstance(data, dict) or not data or json.loads(result.content[0].text) != data:
                raise RuntimeError("Invalid structured result")
            return data

        resolved = await invoke("resolve_link", {"link": f"https://youtu.be/{video}"})
        assert resolved.get("id") == video
        print(json.dumps({"check": "resolve_link", "status": "pass", "type": resolved.get("type")}), flush=True)

        details = await invoke("get_video", {"video": video})
        record = details.get("video") or {}
        assert record.get("id") == video and record.get("title")
        print(json.dumps({"check": "get_video", "status": "pass", "has_subtitles": bool(record.get("subtitles"))}), flush=True)

        page = await invoke("search_videos", {"query": query, "type": "video", "limit": 2})
        rows = page.get("results") or []
        assert rows and len(rows) <= 2
        if page.get("has_more"):
            next_page = await invoke("search_videos", {"query": query, "type": "video", "limit": 2,
                                                          "cursor": page["next_cursor"]})
            next_ids = {row.get("id") for row in next_page.get("results", [])}
            if next_ids.intersection(row.get("id") for row in rows):
                raise RuntimeError("Search pages overlap")
        print(json.dumps({"check": "search_videos", "status": "pass", "items": len(rows)}), flush=True)

        if probe_media:
            streams = await invoke("get_video_streams", {"video": video})
            media = next((item for item in streams.get("muxed", []) if item.get("link")), None)
            if media is None:
                raise RuntimeError("No muxed stream link was returned")
            probe = await anyio.to_thread.run_sync(lambda: media_probe(media["link"], {}, "video"))
            print(json.dumps({"check": "stream_probe", "status": "pass", **probe}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="HTTP MCP endpoint; omit to run the local stdio server")
    parser.add_argument("--video", default="dQw4w9WgXcQ")
    parser.add_argument("--query", default="lofi")
    parser.add_argument("--probe-media", action="store_true", help="request only the first 1 KiB of a video stream")
    args = parser.parse_args()
    try:
        anyio.run(qualify, args.url, args.video, args.query, args.probe_media)
    except Exception as error:
        message = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        print(json.dumps({"check": "qualification", "status": "fail", "error": message}), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
