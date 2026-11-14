# YouTube Scraper MCP

This fork keeps the upstream YouTube scraper's REST API and adds a read-only MCP server for local clients and Fami/OpenClaw on the homeserver. It calls YouTube's public logged-out JSON endpoints through the existing scraper modules. It does not require a browser or a YouTube account.

## MCP tools

| Tool | Inputs | Result |
| --- | --- | --- |
| `search_videos` | `query`, `type`, optional `upload_date`, `duration`, `sort`, `features`, `country`, `language`, `cursor`, `limit` | Search results for videos, Shorts, channels, playlists or movies |
| `get_video` | `video`, `country`, `language` | Metadata, counts, captions, chapters and channel |
| `get_video_transcript` | `video`, optional `language`, `format` (`text` or `segments`), `cursor`, `max_chars` | Bounded transcript text or timed segments |
| `get_video_subtitles` | `video` | Subtitle tracks and WebVTT links |
| `get_video_streams` | `video`, `country` | Available audio/video stream links |
| `get_video_comments` | `video`, `sort`, `country`, `language`, `cursor`, `limit` | Comments with reply cursors |
| `get_comment_replies` | `cursor`, `country`, `language`, `limit` | One page of replies |
| `get_channel` | `channel`, `country`, `language` | Channel profile and available tabs |
| `get_channel_videos` | `channel`, `tab` (`videos`, `shorts`, `live`), `sort`, `country`, `language`, `cursor`, `limit` | Channel uploads, Shorts or live streams |
| `get_playlist` | `playlist`, `country`, `language` | Playlist details |
| `get_playlist_videos` | `playlist`, `country`, `language`, `cursor`, `limit` | Playlist entries |
| `resolve_link` | `link` | YouTube link type and resolved identifier |

Video, channel and playlist inputs accept IDs or YouTube links. Lists default to 5 items and accept a `limit` from 1 to 12. Pass `next_cursor` unchanged with the same filters. Cursors retain the remaining items on the current YouTube page before moving to the next page. Pages come from live upstream data; after cache expiry or a server restart, restart paging if a cursor is rejected.

Transcripts default to text, up to 8,000 characters per call. `max_chars` accepts 1 to 20,000. Each response also includes at most 200 caption segments. Select `format: "segments"` to receive timestamps with the text. Both formats return `has_more` and `next_cursor` when more transcript remains. Pass the cursor with the same video and language. Concatenating text pages reconstructs the transcript, including spaces between segments. A split segment keeps its original timestamps and reports its character offset. Available captions vary by video. Stream and subtitle links are signed and may expire; a returned link does not guarantee that another network can download it.

Errors are MCP tool errors with `{ "error": { "code", "message", "retryable" } }`. A block or timeout is an error, not an empty result.

## Run locally

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements-dev.txt
.venv/bin/python mcp_server.py --stdio
```

Configure a local harness to launch the absolute path to this checkout's `.venv/bin/python`, with arguments `["/absolute/path/to/youtube-scraper/mcp_server.py", "--stdio"]` and the checkout as its working directory. The MCP server can also run as HTTP locally with `.venv/bin/python mcp_server.py`; it listens on port 8000 by default.

Configuration environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MCP_HOST` | `127.0.0.1` locally, `0.0.0.0` in Docker | HTTP bind address |
| `PORT` | `8000` | HTTP port |
| `MCP_TOOL_TIMEOUT` | `45` | Overall seconds per MCP call, including queue time and scraper retries |
| `MCP_MAX_CONCURRENCY` | `4` | Simultaneous MCP calls |
| `MCP_CACHE_SIZE` | `500` | In-memory result pages; `0` disables caching |
| `YOUTUBE_PROXY` | unset | Optional proxy URL used for all scraper requests |

The transport's `youtube_proxy()` and `youtube_fallback_proxy()` helpers both read `YOUTUBE_PROXY`; there is no separate automatic fallback-proxy setting. Compose forwards this optional setting from the environment or the checkout's `.env` file. Without a proxy, blocked requests return a retryable tool error. REST users can continue to start the server with `python run.py`.

## Homeserver deployment

The Compose service joins the external `ai-routing` Docker network and publishes no host ports. Fami/OpenClaw connects to `http://youtube-scraper:8000/mcp` over Streamable HTTP. The endpoint has no application authentication; keep it inside the trusted Docker network or put an authenticated gateway in front of it before exposing it elsewhere.

Build and start from the deployed checkout:

```bash
docker network inspect ai-routing >/dev/null 2>&1 || docker network create ai-routing
docker compose up -d --build
```

The Compose file starts with provisional limits of 256 MiB memory and 0.5 CPU. Measure on the homeserver and adjust from observed peak usage. Its health check verifies the service process, not YouTube reachability.

Configure Fami/OpenClaw with Streamable HTTP and this tool filter:

```json
{
  "youtube-scraper": {
    "enabled": true,
    "url": "http://youtube-scraper:8000/mcp",
    "transport": "streamable-http",
    "toolFilter": {
      "include": [
        "search_videos", "get_video", "get_video_transcript", "get_video_subtitles",
        "get_video_streams", "get_video_comments", "get_comment_replies", "get_channel",
        "get_channel_videos", "get_playlist", "get_playlist_videos", "resolve_link"
      ]
    }
  }
}
```

Use `scripts/deploy` on the homeserver to build this checkout, start the service, compare the running adapter and transport source against the checkout, and verify health plus the exact tool set. Add `--pull` to fast-forward from the configured Git upstream first; the script refuses to pull a dirty checkout. Before rebuilding, it tags the running container's image as `youtube-scraper:rollback` and keeps that image for manual recovery. On a first deployment, no rollback image exists unless a previous local image is present. Deployment and rollback are explicit operator actions.

To restore the previous image without rebuilding:

```bash
docker image tag youtube-scraper:rollback youtube-scraper:local
docker compose up -d --no-build --force-recreate youtube-scraper
```

## Verify

```bash
.venv/bin/python -m pytest youtube/test_parsers.py test_mcp_server.py -q
.venv/bin/python scripts/smoke_mcp.py
.venv/bin/python scripts/smoke_mcp.py --url http://127.0.0.1:8000/mcp
```

The smoke script makes live YouTube requests. Add `--probe-media` to request only the first 1 KiB of a sample video stream. Run qualification from the homeserver as well because its egress may receive different results.

This fork is based on [omkarcloud/youtube-scraper](https://github.com/omkarcloud/youtube-scraper). See [LICENSE](LICENSE).
