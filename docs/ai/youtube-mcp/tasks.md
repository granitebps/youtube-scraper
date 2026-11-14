# YouTube MCP tasks

Source: the six-part plan approved in conversation on 2026-11-14. Add a focused MCP adapter to the existing YouTube scraper for local stdio and private homeserver Streamable HTTP. Preserve REST access. No client configuration edits, actual deployment, commit, push, or publication are authorized.

## Execution Guard

Stop and report if repository reality contradicts the approved scope or completing it requires another extraction backend, browser login, or materially different architecture. Preserve pre-existing changes. Do not create branches or worktrees.

## Tasks

- [x] T001 — Deliver and verify the MCP adjustment
  - Depends on: none
  - Parallel: No
  - Scope: `mcp_server.py`, `youtube/fetch.py`, focused MCP/transport tests, `requirements.txt`, `requirements-dev.txt`, `Dockerfile`, `docker-compose.yaml`, `.dockerignore`, `scripts/deploy`, `scripts/smoke_mcp.py`, `README.md` and this execution record.
  - Implementation: follow TikTok/Instagram's separate MCP entry point, direct Python calls, existing Marshmallow schemas, structured JSON/text results, read-only annotations, and sanitized failure taxonomy. Expose the approved 12 tools: search_videos, get_video, get_video_transcript, get_video_subtitles, get_video_streams, get_video_comments, get_comment_replies, get_channel, get_channel_videos, get_playlist, get_playlist_videos, resolve_link. Lists default to 5, maximum 12, and preserve upstream-page remainders in continuation cursors. Transcript text/timed-segment output must have bounded responses and explicit lossless continuation. Propagate overall deadlines through queue time, requests, retry waits, client refresh and nested thread pools. Add bounded TTL/LRU caching using existing cache constants; do not cache failures or stream links. Preserve `python run.py` for REST.
  - Operations: pin compatible runtime requirements, move pytest to development requirements, retain Debian slim, run MCP by default as non-root with read-only filesystem, health check, dropped capabilities and provisional resource limits. Join external `ai-routing` without published ports. Provide deployment/rollback instructions, local stdio and Fami HTTP examples with an explicit tool filter, and a smoke script for both transports including optional bounded media probes. Document that proxy helpers use the same YOUTUBE_PROXY setting.
  - Verify: existing offline parser suite; focused fixtures/transport tests for no-skips pagination, transcript continuation, deadline propagation and retry bounds, errors and cache behavior; actual MCP stdio/HTTP discovery and representative calls; container build/config checks when Docker is available; live upstream qualification separately from adapter tests. Record environmental blockers honestly. Actual homeserver qualification remains a deployment gate outside this authorized run.
  - Evidence: 83 offline parser/MCP tests passed. Controller exercised all 12 tools live through a stdio client, including transcript, stream and reply calls. HTTP discovery returned 12 read-only tools; protocol resolution and invalid-input handling passed. REST health, prefixed resolution and invalid-input HTTP 400 passed. Optional live media probe returned HTTP 206, video/mp4, 1,024 bytes. Compose config, shell syntax, git diff whitespace checks and dependency compatibility passed (40 installed packages). Docker daemon unavailable locally, so container build/runtime and deployment helper execution were not verified. No homeserver deployment or client configuration edits performed.

## Open Items

None for the repository adjustment. Homeserver egress, resource measurements, and Fami media download qualification are runtime deployment checks, not claims established by this implementation.

## Retired Tasks

None.
