"""Focused MCP behavior checks using the repository's stable scraper seams."""
import json
import time
from datetime import timedelta

import anyio
import pytest
from mcp import Client

import mcp_server as mcp
from youtube import fetch, search, videos


def call(tool, **params):
    result = anyio.run(lambda: tool(**params))
    assert json.loads(result.content[0].text) == result.structured_content
    return result


@pytest.fixture(autouse=True)
def fresh_state(monkeypatch):
    monkeypatch.setattr(mcp, "_cache", mcp.TTLCache(100))
    monkeypatch.setattr(mcp, "_limiter", None)


def test_tools_are_bounded_and_read_only():
    tools = mcp.server._tool_manager.list_tools()
    assert {t.name for t in tools} == {
        "search_videos", "get_video", "get_video_transcript", "get_video_subtitles", "get_video_streams",
        "get_video_comments", "get_comment_replies", "get_channel", "get_channel_videos", "get_playlist",
        "get_playlist_videos", "resolve_link",
    }
    assert all(t.annotations.read_only_hint and t.annotations.destructive_hint is False for t in tools)


def test_playlist_paging_preserves_page_remainders_and_upstream_cursor(monkeypatch):
    seen_cursors = []

    def page(**params):
        cursor = params.get("cursor")
        seen_cursors.append(cursor)
        start = 0 if cursor is None else 5
        end = 5 if cursor is None else 7
        rows = [{"id": str(i)} for i in range(start, end)]
        return {"videos": rows, "count": len(rows), "next_cursor": "next-page-token" if cursor is None else None}

    monkeypatch.setattr("mcp_server.playlists.videos", page)
    params = {"playlist": "PLFgquLnL59alCl_2TQvOiD5Vgm1hCaGSI", "limit": 2}
    ids, cursor = [], None
    for _ in range(5):
        result = call(mcp.get_playlist_videos, **params, cursor=cursor)
        assert not result.is_error
        data = result.structured_content
        ids.extend(row["id"] for row in data["videos"])
        if not data["has_more"]:
            break
        cursor = data["next_cursor"]
    assert ids == ["0", "1", "2", "3", "4", "5", "6"]
    assert seen_cursors == [None, "next-page-token"]


def test_search_schema_validates_typed_features(monkeypatch):
    calls = []

    def page(**params):
        calls.append(params)
        return {"results": [{"id": "abc"}], "next_cursor": None}

    monkeypatch.setattr(search, "search", page)
    assert not call(mcp.search_videos, query="music", features=["hd", "4k"]).is_error
    assert calls[0]["features"] == ["hd", "4k"]
    invalid = call(mcp.search_videos, query="music", features=["8k"])
    assert invalid.is_error and invalid.structured_content["error"]["code"] == "invalid_input"


def test_transcript_continues_inside_oversized_segment(monkeypatch):
    monkeypatch.setattr(videos, "transcript", lambda **params: {
        "video": {"id": params["video"]}, "language_code": "en", "language": "English",
        "count": 1, "segments": [{"start_seconds": 2.0, "duration_seconds": 5.0, "text": "abcdefghij"}],
    })
    first = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", format="segments", max_chars=4)
    assert first.structured_content["segments"][0]["text"] == "abcd"
    assert first.structured_content["has_more"]
    second = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", format="segments", max_chars=4,
                  cursor=first.structured_content["next_cursor"])
    assert second.structured_content["segments"][0]["text"] == "efgh"
    third = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", format="text", max_chars=4,
                 cursor=second.structured_content["next_cursor"])
    assert third.structured_content["text"] == "ij"
    assert not third.structured_content["has_more"]


def test_transcript_cursor_is_bound_to_reference(monkeypatch):
    monkeypatch.setattr(videos, "transcript", lambda **params: {"segments": [{"text": "hello"}]})
    first = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", max_chars=2)
    second = call(mcp.get_video_transcript, video="kJQP7kiw5Fk", max_chars=2,
                  cursor=first.structured_content["next_cursor"])
    assert second.is_error and second.structured_content["error"]["code"] == "invalid_input"


def test_transcript_segments_do_not_leak_full_text(monkeypatch):
    monkeypatch.setattr(videos, "transcript", lambda **params: {
        "text": "secret-full-transcript", "segments": [{"text": "hello"}],
    })
    result = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", format="segments", max_chars=2)
    assert "text" not in result.structured_content
    assert "secret-full-transcript" not in result.content[0].text
    assert result.structured_content["segments"][0]["text"] == "he"


def test_transcript_rejects_invalid_offset_and_empty_segments(monkeypatch):
    monkeypatch.setattr(videos, "transcript", lambda **params: {"segments": [{"text": "hello"}]})
    bad = mcp._encode_cursor({"video": "dQw4w9WgXcQ", "language": None, "index": 0, "offset": 50})
    assert call(mcp.get_video_transcript, video="dQw4w9WgXcQ", cursor=bad).structured_content["error"]["code"] == "invalid_input"
    mcp._cache = mcp.TTLCache(100)
    monkeypatch.setattr(videos, "transcript", lambda **params: {"segments": []})
    empty = call(mcp.get_video_transcript, video="dQw4w9WgXcQ")
    assert empty.is_error and empty.structured_content["error"]["code"] == "no_transcript"


def test_streams_are_not_cached(monkeypatch):
    calls = []
    monkeypatch.setattr(videos, "streams", lambda **params: calls.append(params) or {"muxed": [{"link": "signed"}]})
    call(mcp.get_video_streams, video="dQw4w9WgXcQ")
    call(mcp.get_video_streams, video="dQw4w9WgXcQ")
    assert len(calls) == 2


def test_deadline_caps_retries_and_propagates_to_fanout(monkeypatch):
    monkeypatch.setattr(fetch.config, "MAX_RETRIES", 3)
    monkeypatch.setattr(fetch.config, "RETRY_BACKOFF", 2)
    attempts = []

    def fail():
        attempts.append(1)
        raise fetch.YoutubeUpstreamError("temporary")

    started = time.monotonic()
    with pytest.raises(fetch.YoutubeTimeout):
        with fetch.deadline(0.03):
            fetch._retrying(fail)
    assert len(attempts) == 1
    assert time.monotonic() - started < 0.5

    started_calls = []

    def queued():
        fetch._remaining()
        started_calls.append(1)
        time.sleep(0.03)
        fetch._remaining()
        return True

    with pytest.raises(fetch.YoutubeTimeout):
        with fetch.deadline(0.01):
            fetch.run_parallel([queued] * 16)
    assert len(started_calls) <= fetch.FANOUT_WORKERS


def test_cache_is_bounded_and_ttl_expires():
    cache = mcp.TTLCache(1)
    cache.set("a", {"id": 1}, timedelta(seconds=0.01))
    assert cache.get("a") == {"id": 1}
    time.sleep(0.02)
    assert cache.get("a") is None
    cache.set("a", 1, timedelta(seconds=10))
    cache.set("b", 2, timedelta(seconds=10))
    assert cache.get("a") is None and cache.get("b") == 2


def test_protocol_client_lists_tools_and_resolves_link():
    async def exercise():
        async with Client(mcp.server) as client:
            tools = (await client.list_tools()).tools
            assert {tool.name for tool in tools} == {t.name for t in mcp.server._tool_manager.list_tools()}
            result = await client.call_tool("resolve_link", {"link": "https://youtu.be/dQw4w9WgXcQ"})
            assert not result.is_error
            assert result.structured_content["id"] == "dQw4w9WgXcQ"
    anyio.run(exercise)


def test_transcript_text_keeps_word_boundaries_across_every_page(monkeypatch):
    monkeypatch.setattr(videos, "transcript", lambda **params: {
        "text": "hello world again", "segments": [{"text": text, "start_seconds": i}
                                                       for i, text in enumerate(["hello", "world", "again"])],
    })
    for size in (1, 5, 6, 8, 20):
        chunks, cursor = [], None
        for _ in range(30):
            result = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", cursor=cursor, max_chars=size)
            assert not result.is_error
            page = result.structured_content
            assert len(page["text"]) <= size
            chunks.append(page["text"])
            if not page["has_more"]:
                break
            cursor = page["next_cursor"]
        else:
            pytest.fail("Transcript cursor did not advance")
        assert "".join(chunks) == "hello world again"


def test_bot_check_retry_respects_remaining_budget(monkeypatch):
    monkeypatch.setattr(fetch, "innertube", lambda *args, **kw: {
        "playabilityStatus": {"reason": "Sign in to confirm you're not a bot"},
    })
    monkeypatch.setattr(fetch.config, "MAX_RETRIES", 2)
    waits = []
    monkeypatch.setattr(fetch.time, "sleep", waits.append)
    with pytest.raises(fetch.YoutubeBotCheck):
        with fetch.deadline(0.03):
            fetch.player("dQw4w9WgXcQ")
    assert waits and all(0 <= wait <= 0.03 for wait in waits)


@pytest.mark.parametrize("failure,code,retryable", [
    (fetch.YoutubeNotFound, "not_found", False),
    (fetch.YoutubeBadRequest, "upstream_rejected", False),
    (fetch.YoutubeBlocked, "upstream_blocked", True),
    (fetch.YoutubeUpstreamError, "upstream_unavailable", True),
    (fetch.YoutubeTimeout, "timeout", True),
    (RuntimeError, "internal_error", False),
])
def test_failures_are_sanitized_and_never_cached(monkeypatch, caplog, failure, code, retryable):
    attempts = []
    def fail(**kwargs):
        attempts.append(1)
        raise failure("private-proxy-credential-or-signed-url")
    monkeypatch.setattr(videos, "details", fail)
    for _ in range(2):
        result = call(mcp.get_video, video="dQw4w9WgXcQ")
        assert result.is_error
        assert result.structured_content["error"]["code"] == code
        assert result.structured_content["error"]["retryable"] is retryable
        assert "private-proxy" not in result.content[0].text
    assert len(attempts) == 2
    assert "private-proxy" not in caplog.text


def test_empty_transcript_does_not_poison_cache(monkeypatch):
    calls = []
    def transcript(**kwargs):
        calls.append(1)
        return {"segments": [] if len(calls) == 1 else [{"text": "usable"}]}
    monkeypatch.setattr(videos, "transcript", transcript)
    assert call(mcp.get_video_transcript, video="dQw4w9WgXcQ").is_error
    result = call(mcp.get_video_transcript, video="dQw4w9WgXcQ")
    assert not result.is_error and result.structured_content["text"] == "usable"
    assert len(calls) == 2


def test_queued_call_expires_without_starting_worker(monkeypatch):
    monkeypatch.setattr(mcp, "TOOL_TIMEOUT", 0.02)
    async def exercise():
        limiter = anyio.CapacityLimiter(1)
        monkeypatch.setattr(mcp, "_limiter", limiter)
        started = []
        acquired, release = anyio.Event(), anyio.Event()
        async def hold_capacity():
            async with limiter:
                acquired.set()
                await release.wait()
        async with anyio.create_task_group() as group:
            group.start_soon(hold_capacity)
            await acquired.wait()
            result = await mcp._run("queued", lambda: started.append(1) or {})
            release.set()
        assert result.is_error and result.structured_content["error"]["code"] == "timeout"
        assert not started
    anyio.run(exercise)


def test_expired_worker_result_is_not_cached_and_limiter_is_held(monkeypatch):
    monkeypatch.setattr(mcp, "TOOL_TIMEOUT", 0.01)
    def slow(**kwargs):
        time.sleep(0.03)
        return {"video": {"id": kwargs["video"]}}
    monkeypatch.setattr(videos, "details", slow)
    started = time.monotonic()
    result = call(mcp.get_video, video="dQw4w9WgXcQ")
    assert result.structured_content["error"]["code"] == "timeout"
    assert time.monotonic() - started >= 0.03
    assert not mcp._cache._data


def test_client_refresh_lock_obeys_deadline(monkeypatch):
    monkeypatch.setitem(fetch._client, "at", 0)
    with fetch._client_lock:
        with pytest.raises(fetch.YoutubeTimeout):
            with fetch.deadline(0.01):
                fetch.client_config()


def test_reply_cursor_keeps_browse_prefix_and_long_tokens(monkeypatch):
    calls = []
    token = "b." + "x" * 2000
    monkeypatch.setattr(videos, "replies", lambda **kwargs: calls.append(kwargs["cursor"]) or {
        "replies": [{"id": str(i)} for i in range(3)], "next_cursor": None,
    })
    first = call(mcp.get_comment_replies, cursor=token, limit=2).structured_content
    second = call(mcp.get_comment_replies, cursor=first["next_cursor"], limit=2).structured_content
    assert [row["id"] for row in first["replies"] + second["replies"]] == ["0", "1", "2"]
    assert calls == [token]


def test_transcript_segment_limit_preserves_remaining_content(monkeypatch):
    monkeypatch.setattr(videos, "transcript", lambda **kwargs: {
        "segments": [{"text": "x", "start_seconds": i} for i in range(205)],
    })
    first = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", format="segments").structured_content
    assert first["count"] == 200 and first["has_more"]
    second = call(mcp.get_video_transcript, video="dQw4w9WgXcQ", format="segments",
                  cursor=first["next_cursor"]).structured_content
    assert second["count"] == 5 and not second["has_more"]
    assert [s["start_seconds"] for s in first["segments"] + second["segments"]] == list(range(205))
