import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cache import APICache, RequestDeduplicator  # noqa: E402


def test_api_cache_set_get_and_invalidate():
    async def _run():
        cache = APICache()
        await cache.set("player:#AAA", {"name": "Alice"})
        hit = await cache.get("player:#AAA", ttl=60.0)
        assert hit == {"name": "Alice"}

        await cache.invalidate("player:#AAA")
        miss = await cache.get("player:#AAA", ttl=60.0)
        assert miss is None

    asyncio.run(_run())


def test_api_cache_ttl_expiry_removes_stale_entry():
    async def _run():
        cache = APICache()
        await cache.set("clan:#PQUCURCQ", {"members": 50})
        await asyncio.sleep(0.03)
        out = await cache.get("clan:#PQUCURCQ", ttl=0.01)
        assert out is None
        assert cache.get_stats()["total_keys"] == 0

    asyncio.run(_run())


def test_request_deduplicator_collapses_concurrent_requests():
    async def _run():
        dedup = RequestDeduplicator()
        call_count = 0

        async def factory():
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.02)
            return "ok"

        results = await asyncio.gather(
            dedup.get_or_create("war:#AAA", factory),
            dedup.get_or_create("war:#AAA", factory),
            dedup.get_or_create("war:#AAA", factory),
        )
        assert results == ["ok", "ok", "ok"]
        assert call_count == 1

    asyncio.run(_run())


def test_request_deduplicator_clear_removes_done_futures():
    async def _run():
        dedup = RequestDeduplicator()
        loop = asyncio.get_running_loop()
        done_future = loop.create_future()
        done_future.set_result("done")
        dedup._pending["player:#BBB"] = done_future

        out = await dedup.clear()
        assert out["total"] == 1
        assert out["removed_done"] == 1
        assert "player:#BBB" not in dedup._pending

    asyncio.run(_run())
