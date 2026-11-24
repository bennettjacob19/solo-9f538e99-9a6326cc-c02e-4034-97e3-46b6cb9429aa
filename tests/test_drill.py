"""End-to-end drill tests.

The tests run the real FastAPI application over a live server and let the
drill talk to its own origin endpoint via HTTP, so the whole path (main
download, retention, conditional backfill, SHA-256 verification) is covered.

Run with:  python -m pytest -q
"""

from __future__ import annotations

import socket
import threading
import time
from typing import Optional

import pytest
import uvicorn
from starlette.requests import Request as StarletteRequest

from app.drill import DrillParams, DrillRun
from app.main import create_app
from app.origin import OriginStore
from app.retention import RetentionBuffer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def server():
    port = _free_port()
    app = create_app(origin_base=f"http://127.0.0.1:{port}")
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.02)
    assert srv.started, "server did not start"
    try:
        yield app, f"http://127.0.0.1:{port}"
    finally:
        srv.should_exit = True
        thread.join(timeout=10)


def _run_drill(server_base: str, origin_store: OriginStore, **overrides) -> dict:
    import asyncio

    payload = {
        "size_bytes": 512 * 1024,
        "chunk_size": 32 * 1024,
        "chunk_delay_ms": 1,
        "retention_bytes": 32 * 1024,
        "retention_memory_bytes": 16 * 1024,
        "backfill_bytes": 256 * 1024,
        "pause_fraction": 0.75,
    }
    payload.update(overrides)
    params = DrillParams.from_payload(payload)

    async def go():
        run = DrillRun(params, server_base)
        run.start()
        deadline = time.time() + 30
        while run.status == "running" and time.time() < deadline:
            await asyncio.sleep(0.02)
        assert run.status != "running", "drill timed out"
        snap = run.snapshot()
        snap["origin"] = origin_store.stats()
        return snap

    return asyncio.run(go())


# ------------------------------------------------------------------ unit tests
def test_retention_buffer_spills_and_caps():
    buf = RetentionBuffer(capacity_bytes=10, memory_limit_bytes=4)
    assert buf.append(b"abcdef") == 6
    assert buf.stats()["used_memory_bytes"] == 4
    assert buf.stats()["used_disk_bytes"] == 2
    assert buf.append(b"ghijklmn") == 4  # only 4 bytes of room left
    assert buf.dropped_bytes == 4
    assert buf.used_bytes == 10
    assert buf.read(6) == b"abcdef"
    assert buf.read(100) == b"ghij"
    assert buf.used_bytes == 0
    assert buf.stats()["peak_bytes"] == 10
    buf.close()


def test_retention_buffer_memory_only():
    buf = RetentionBuffer(capacity_bytes=8, memory_limit_bytes=8)
    buf.append(b"12345678")
    assert buf.stats()["spilled"] is False
    assert buf.stats()["peak_disk_bytes"] == 0
    buf.close()


def test_origin_versions_are_equal_length_but_different():
    store = OriginStore(size=4096, chunk_size=1024)
    v1, v2 = store.get("v1"), store.get("v2")
    assert v1.size == v2.size == 4096
    assert v1.sha256 != v2.sha256
    assert v1.etag.startswith('"') and not v1.etag.startswith("W/")


# --------------------------------------------------------------- drill tests
def test_both_routes_complete_when_combined_quota_covers_tail(server):
    app, base = server
    snap = _run_drill(
        base,
        app.state.origin_store,
        retention_bytes=32 * 1024,
        backfill_bytes=256 * 1024,
    )
    assert snap["outcome"] == "both_complete"
    assert snap["fast"]["complete"] is True
    assert snap["slow"]["received_bytes"] == snap["total_bytes"]
    assert snap["slow"]["missing_segments"] == []
    assert snap["fast"]["verification"]["result"] == "match"
    assert snap["slow"]["verification"]["result"] == "match"
    assert snap["slow"]["backfill"]["etag_verified"] is True
    assert snap["slow"]["backfill"]["range_verified"] is True
    assert snap["main"]["request_count"] == 1
    assert snap["origin"]["main_requests"] == 1


def test_missing_segment_reported_when_quota_too_small(server):
    app, base = server
    snap = _run_drill(
        base,
        app.state.origin_store,
        retention_bytes=8 * 1024,
        retention_memory_bytes=4 * 1024,
        backfill_bytes=8 * 1024,
    )
    assert snap["outcome"] == "slow_missing"
    assert snap["fast"]["complete"] is True
    assert snap["fast"]["verification"]["result"] == "match"
    assert snap["slow"]["missing_segments"]
    assert snap["slow"]["verification"]["result"] == "incomplete"


def test_version_swap_during_pause_still_serves_old_version(server):
    app, base = server
    snap = _run_drill(
        base,
        app.state.origin_store,
        retention_bytes=8 * 1024,
        retention_memory_bytes=4 * 1024,
        backfill_bytes=8 * 1024,
        auto_switch_on_pause=True,
    )
    # Origin flipped to v2, but the main download and the If-Range backfill both
    # answer for v1 -- so the backfill fails the ETag check and the slow route
    # honestly reports a missing segment rather than mixing versions.
    assert snap["origin_switch"]["triggered"] is True
    assert snap["origin"]["current_version"] == "v2"
    assert snap["slow"]["backfill"]["attempted"] is True
    assert snap["slow"]["backfill"]["status"] == "rejected"
    assert snap["slow"]["backfill"]["etag_verified"] is False
    assert snap["outcome"] == "slow_missing"
    assert snap["fast"]["verification"]["result"] == "match"


def test_version_swap_with_full_retention_still_completes(server):
    app, base = server
    size = 512 * 1024
    snap = _run_drill(
        base,
        app.state.origin_store,
        size_bytes=size,
        retention_bytes=size,
        retention_memory_bytes=size,
        backfill_bytes=0,
        auto_switch_on_pause=True,
    )
    assert snap["origin_switch"]["triggered"] is True
    assert snap["slow"]["backfill"]["attempted"] is False
    assert snap["outcome"] == "both_complete"
    assert snap["slow"]["verification"]["result"] == "match"
    assert snap["fast"]["verification"]["result"] == "match"


def test_origin_serves_200_when_if_range_stale(server):
    import httpx

    app, base = server
    store: OriginStore = app.state.origin_store
    old = store.get("v1")
    with httpx.Client(base_url=base, timeout=10) as client:
        store.switch("v2")
        resp = client.get(
            "/origin/layer",
            headers={"Range": "bytes=0-99", "If-Range": old.etag},
        )
        assert resp.status_code == 200
        assert resp.headers["etag"] == store.get("v2").etag
        assert len(resp.content) == store.get("v2").size
        assert resp.headers["x-if-range-matched"] == "false"


# ----------------------------------------------------- backfill quota accounting
def test_rejected_full_layer_body_counts_against_backfill_quota(server):
    """A 1 KiB capped backfill against a swapped origin must not leak the layer.

    The origin answers the stale If-Range with a full 200; the gateway reads at
    most the round cap, disconnects, and the origin's actually-sent bytes
    (including the rejected body) are charged against the backfill quota.
    """
    import httpx

    app, base = server
    store: OriginStore = app.state.origin_store
    # Small streaming chunks so the origin counters stay fine-grained.
    store.chunk_size = 256
    cap = 1024
    snap = _run_drill(
        base,
        store,
        retention_bytes=8 * 1024,
        retention_memory_bytes=4 * 1024,
        backfill_bytes=cap,
        auto_switch_on_pause=True,
    )
    bf = snap["slow"]["backfill"]
    assert bf["attempted"] is True
    assert bf["status"] == "rejected"
    # Exactly one version check, before the single fetch.
    assert bf["version_check"]["attempted"] is True
    assert bf["version_check"]["requests"] == 1
    assert bf["version_check"]["matches_main"] is False
    assert bf["capped_at_bytes"] == cap
    # The slow route kept none of the rejected new-version body.
    assert bf["received_bytes"] == 0
    # The origin really sent up to the cap (never the whole layer).
    assert bf["settlement"] == "settled"
    assert bf["origin_sent_bytes"] is not None
    assert 0 < bf["origin_sent_bytes"] <= cap
    # Quota is charged against bytes actually sent, including the rejection.
    assert bf["used_bytes"] == bf["origin_sent_bytes"]
    range_req = next(
        r for r in snap["origin"]["requests"] if r["kind"] == "range"
    )
    assert range_req["bytes_sent"] <= cap
    assert range_req["outcome"] in ("capped", "aborted")
    assert snap["outcome"] == "slow_missing"
    assert snap["fast"]["verification"]["result"] == "match"

    # And the origin's response really would have been the whole layer had the
    # gateway not capped it (old-client direct If-Range behavior unchanged).
    with httpx.Client(base_url=base, timeout=10) as client:
        resp = client.get(
            "/origin/layer",
            headers={
                "Range": f"bytes={cap}-{cap + 99}",
                "If-Range": store.get("v1").etag,
            },
        )
        assert resp.status_code == 200
        assert len(resp.content) == store.get("v2").size


def test_no_swap_and_enough_quota_completes_slow_with_original_version(server):
    app, base = server
    store: OriginStore = app.state.origin_store
    store.chunk_size = 256
    snap = _run_drill(
        base,
        store,
        retention_bytes=8 * 1024,
        retention_memory_bytes=4 * 1024,
        backfill_bytes=256 * 1024,
        auto_switch_on_pause=False,
    )
    bf = snap["slow"]["backfill"]
    assert snap["outcome"] == "both_complete"
    assert snap["slow"]["verification"]["result"] == "match"
    assert bf["status"] == "accepted"
    assert bf["version_check"]["matches_main"] is True
    assert bf["version_check"]["requests"] == 1
    # Tail minus what the retention buffer held is the single backfill fetch;
    # origin-sent and slow-kept coincide for an accepted 206.
    retained = snap["retention"]["peak_bytes"]
    expected_tail = snap["total_bytes"] - snap["slow"]["paused_at_bytes"]
    expected_backfill = expected_tail - retained
    assert bf["settlement"] == "settled"
    assert bf["origin_sent_bytes"] == expected_backfill
    assert bf["received_bytes"] == expected_backfill
    assert bf["used_bytes"] == bf["origin_sent_bytes"]
    assert snap["origin"]["main_requests"] == 1
    assert snap["origin"]["range_requests"] == 1


def _legacy_origin_app(size: int, chunk_size: int):
    """A minimal origin without the X-Request-Id header (pre-upgrade build).

    It keeps the exact If-Range/Range contract an old client relies on: stale
    If-Range -> full 200 of the current version; valid -> 206.  State is exposed
    through the same /origin/state shape, but responses carry no request id and
    the X-Backfill-Cap header is ignored, so the gateway must fall back to
    matching the newest range record and cap reads on its own side.
    """
    import hashlib
    import random

    from fastapi import FastAPI
    from fastapi.responses import StreamingResponse

    def build(seed: int, layer_size: int) -> tuple[bytes, str, str]:
        data = random.Random(seed).randbytes(layer_size)
        digest = hashlib.sha256(data).hexdigest()
        return data, digest, f'"v{seed}-{digest[:12]}"'

    versions = {
        "v1": build(20261123, size),
        "v2": build(20261124, size),
    }
    current = {"name": "v1"}
    records: list[dict] = []
    counters = {"main": 0, "range": 0, "seq": 0}
    built_size = {"value": size}

    async def stream(data: bytes, start: int, end: int, record: dict, req):
        # A pre-upgrade origin has no cap negotiation and no disconnect-aware
        # streaming: it just produces the full representation and honestly
        # counts what it produced.  The gateway is responsible for capping its
        # own reads and closing the connection.
        pos = start
        try:
            while pos <= end:
                take = min(chunk_size, end + 1 - pos)
                yield data[pos : pos + take]
                pos += take
                record["bytes_sent"] += take
            record["outcome"] = "ok"
        except Exception:
            record["outcome"] = "aborted"
            raise

    legacy = FastAPI()

    @legacy.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @legacy.post("/origin/reset")
    async def reset(size_bytes: Optional[int] = None) -> dict:
        nonlocal versions
        target = size_bytes if size_bytes is not None else built_size["value"]
        if target != built_size["value"]:
            versions = {
                "v1": build(20261123, target),
                "v2": build(20261124, target),
            }
            built_size["value"] = target
        current["name"] = "v1"
        records.clear()
        counters.update(main=0, range=0, seq=0)
        return {"ok": True}

    @legacy.post("/origin/switch")
    async def switch(version: str = "v2") -> dict:
        previous = current["name"]
        current["name"] = version
        return {"from": previous, "to": version, "changed": previous != version}

    @legacy.get("/origin/state")
    async def state() -> dict:
        return {
            "state": {
                "current_version": current["name"],
                "current_etag": versions[current["name"]][2],
                "current_sha256": versions[current["name"]][1],
                "main_requests": counters["main"],
                "range_requests": counters["range"],
                "requests": list(reversed(records)),
            },
            "versions": [
                {"name": name, "size": len(versions[name][0]),
                 "sha256": versions[name][1], "etag": versions[name][2]}
                for name in versions
            ],
        }

    @legacy.get("/origin/layer")
    async def layer(request: StarletteRequest):
        data, sha, etag = versions[current["name"]]
        range_header = request.headers.get("range")
        if_range = request.headers.get("if-range")
        counters["seq"] += 1
        record = {
            "id": counters["seq"],
            "kind": "range" if range_header else "main",
            "bytes_sent": 0,
            "outcome": "in_flight",
        }
        if not range_header:
            counters["main"] += 1
            records.append(record)
            return StreamingResponse(
                stream(data, 0, len(data) - 1, record, request),
                headers={
                    "ETag": etag,
                    "Content-Length": str(len(data)),
                    "X-Content-SHA256": sha,
                },
            )
        counters["range"] += 1
        records.append(record)
        if if_range is not None and if_range != etag:
            return StreamingResponse(
                stream(data, 0, len(data) - 1, record, request),
                status_code=200,
                headers={
                    "ETag": etag,
                    "Content-Length": str(len(data)),
                    "X-Content-SHA256": sha,
                    "X-If-Range-Matched": "false",
                },
            )
        parsed = range_header.replace("bytes=", "").split("-")
        start = int(parsed[0])
        end = int(parsed[1])
        return StreamingResponse(
            stream(data, start, end, record, request),
            status_code=206,
            headers={
                "ETag": etag,
                "Content-Length": str(end - start + 1),
                "Content-Range": f"bytes {start}-{end}/{len(data)}",
                "X-Content-SHA256": sha,
            },
        )

    return legacy


def _settling_origin_app(size: int, delay_s: float = 1.2):
    """Origin whose state endpoint keeps a finished range record in_flight.

    Simulates "the origin is still sending / final count not yet available":
    the bytes are fully delivered (so the drill itself completes), but the
    accounting record only settles after ``delay_s`` seconds.
    """
    import hashlib
    import random

    from fastapi import FastAPI
    from fastapi.responses import JSONResponse, StreamingResponse

    def build(seed: int) -> tuple[bytes, str, str]:
        data = random.Random(seed).randbytes(size)
        digest = hashlib.sha256(data).hexdigest()
        return data, digest, f'"v1-{digest[:12]}"'

    data, sha, etag = build(20261123)
    record = {"id": 1, "kind": "range", "bytes_sent": 0, "outcome": "in_flight"}
    settle_at = {"t": None}

    app = FastAPI()

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.post("/origin/reset")
    async def reset(size_bytes: Optional[int] = None) -> dict:
        record.update(bytes_sent=0, outcome="in_flight")
        settle_at["t"] = None
        return {"ok": True}

    @app.post("/origin/switch")
    async def switch(version: str = "v2") -> dict:
        return {"from": "v1", "to": "v1", "changed": False}

    @app.get("/origin/state")
    async def state():
        if (
            record["outcome"] == "in_flight"
            and settle_at["t"] is not None
            and time.time() >= settle_at["t"]
        ):
            record["outcome"] = "ok"
        return JSONResponse(
            {
                "state": {
                    "current_version": "v1",
                    "current_etag": etag,
                    "current_sha256": sha,
                    "main_requests": 1,
                    "range_requests": 1,
                    "requests": [dict(record)],
                },
                "versions": [
                    {"name": "v1", "size": len(data), "sha256": sha, "etag": etag}
                ],
            }
        )

    @app.get("/origin/layer")
    async def layer(request: StarletteRequest):
        if request.headers.get("range"):
            parsed = request.headers["range"].replace("bytes=", "").split("-")
            r_start, r_end = int(parsed[0]), int(parsed[1])
            body = data[r_start : r_end + 1]

            async def gen_range():
                # Accounting stays in_flight until a timer observed via
                # /origin/state elapses, even though delivery is immediate.
                settle_at["t"] = time.time() + delay_s
                record["bytes_sent"] = len(body)
                yield body

            record["id"] = 1
            return StreamingResponse(
                gen_range(),
                status_code=206,
                headers={
                    "ETag": etag,
                    "Content-Length": str(len(body)),
                    "Content-Range": (
                        f"bytes {r_start}-{r_end}/{len(data)}"
                    ),
                    "X-Content-SHA256": sha,
                    "X-Request-Id": "1",
                },
            )

        async def gen_main():
            yield data

        return StreamingResponse(
            gen_main(),
            headers={
                "ETag": etag,
                "Content-Length": str(len(data)),
                "X-Content-SHA256": sha,
                "X-Request-Id": "0",
            },
        )

    return app


def test_backfill_shows_pending_until_origin_final_count_settles():
    import uvicorn

    from app.drill import DrillParams, DrillRun

    port = _free_port()
    config = uvicorn.Config(
        _settling_origin_app(size=32 * 1024, delay_s=2.5),
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.02)
    assert srv.started
    try:
        import asyncio

        base = f"http://127.0.0.1:{port}"
        params = DrillParams.from_payload(
            {
                "size_bytes": 32 * 1024,
                "chunk_size": 4 * 1024,
                "chunk_delay_ms": 0,
                "retention_bytes": 2 * 1024,
                "retention_memory_bytes": 1 * 1024,
                "backfill_bytes": 64 * 1024,
                "pause_fraction": 0.75,
            }
        )

        async def go():
            run = DrillRun(params, base)
            run.start()
            # The drill logic may finish while the origin record is still
            # in_flight; keep observing settlement afterwards.
            running_limit = time.time() + 20
            while run.status == "running" and time.time() < running_limit:
                await asyncio.sleep(0.02)
            assert run.status != "running"
            saw_pending = False
            settle_deadline = time.time() + 15
            while time.time() < settle_deadline:
                bf = run.backfill
                if bf["settlement"] == "settling":
                    saw_pending = True
                    assert bf["origin_sent_bytes"] is None
                    assert bf["used_bytes"] is None
                if bf["settlement"] == "settled":
                    return saw_pending, run.snapshot()
                await asyncio.sleep(0.05)
            raise AssertionError("backfill never settled")

        saw_pending, snap = asyncio.run(go())
        assert saw_pending, "expected at least one 待结算 observation"
        bf = snap["slow"]["backfill"]
        assert bf["settlement"] == "settled"
        # The origin's final counter covers exactly the requested tail slice.
        expected_len = (
            bf["requested_end"] - bf["requested_start"] + 1
        )
        assert bf["origin_sent_bytes"] == expected_len
        assert bf["used_bytes"] == expected_len
        # Fast route verification is unaffected by the pending settlement.
        assert snap["fast"]["verification"]["result"] == "match"
        assert snap["backfill_settling"] is False
    finally:
        srv.should_exit = True
        thread.join(timeout=10)


def test_gateway_settles_against_unupgraded_origin_without_request_id():
    import uvicorn

    from app.drill import DrillParams, DrillRun

    port = _free_port()
    config = uvicorn.Config(
        _legacy_origin_app(size=64 * 1024, chunk_size=256),
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.02)
    assert srv.started
    try:
        import asyncio
        import httpx

        base = f"http://127.0.0.1:{port}"
        # Swap the legacy origin before the drill starts; the main download
        # snapshots v1 anyway, then the paused backfill hits a stale If-Range.
        payload = {
            "size_bytes": 64 * 1024,
            "chunk_size": 8 * 1024,
            "chunk_delay_ms": 0,
            "retention_bytes": 4 * 1024,
            "retention_memory_bytes": 2 * 1024,
            "backfill_bytes": 1024,
            "pause_fraction": 0.75,
            "auto_switch_on_pause": True,
        }
        params = DrillParams.from_payload(payload)

        async def go():
            run = DrillRun(params, base)
            run.start()
            limit = time.time() + 30
            while run.status == "running" and time.time() < limit:
                await asyncio.sleep(0.02)
            assert run.status != "running"
            # No X-Request-Id on a legacy origin.
            assert run.backfill["origin_request_id"] is None
            # Settlement still works via newest-range-record fallback.
            settle_deadline = time.time() + 10
            while (
                run.backfill["settlement"] == "settling"
                and time.time() < settle_deadline
            ):
                await asyncio.sleep(0.1)
            return run.snapshot()

        snap = asyncio.run(go())
        bf = snap["slow"]["backfill"]
        assert bf["status"] == "rejected"
        assert bf["settlement"] == "settled"
        assert bf["origin_sent_bytes"] is not None
        # The unupgraded origin ignores the cap header and honestly reports
        # the full rejected body it produced; the gateway still charges that
        # number against the backfill quota.
        assert bf["origin_sent_bytes"] == 64 * 1024
        assert bf["used_bytes"] == bf["origin_sent_bytes"]
        # Crucially the slow route kept zero new-version bytes, and the
        # gateway's own hard read cap was never exceeded on its side.
        assert bf["received_bytes"] == 0
        assert bf["capped_at_bytes"] == 1024
        assert snap["fast"]["verification"]["result"] == "match"
        assert snap["outcome"] == "slow_missing"

        # Direct legacy-client If-Range semantics remain untouched.
        with httpx.Client(base_url=base, timeout=10) as client:
            state = client.get("/origin/state").json()["state"]
            v1 = next(v for v in client.get("/origin/state").json()["versions"]
                      if v["name"] == "v1")
            assert state["current_version"] == "v2"
            resp = client.get(
                "/origin/layer",
                headers={"Range": "bytes=0-99", "If-Range": v1["etag"]},
            )
            assert resp.status_code == 200
            assert len(resp.content) == 64 * 1024
    finally:
        srv.should_exit = True
        thread.join(timeout=10)
