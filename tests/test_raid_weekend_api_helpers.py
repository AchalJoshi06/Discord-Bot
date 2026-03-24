import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.raids import (  # noqa: E402
    _parse_coc_timestamp,
    get_raid_weekends,
    is_raid_weekend_active,
)


class _FakeBot:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def coc_get(self, path: str):
        self.calls.append(path)
        return self.responses.get(path)


def test_parse_coc_timestamp_supports_compact_and_iso():
    a = _parse_coc_timestamp("20260321T070000.000Z")
    b = _parse_coc_timestamp("2026-03-21T07:00:00+00:00")
    assert a is not None
    assert b is not None
    assert a.tzinfo is not None
    assert b.tzinfo is not None


def test_is_raid_weekend_active_uses_time_window():
    raid = {
        "startTime": "20260321T070000.000Z",
        "endTime": "20260325T070000.000Z",
    }
    now = datetime(2026, 3, 23, 12, 0, tzinfo=timezone.utc)
    assert is_raid_weekend_active(raid, now=now) is True


def test_get_raid_weekends_prefers_documented_plural_endpoint():
    tag_q = "%232JJJCCRQR"
    plural_path = f"/clans/{tag_q}/capitalraidseasons?limit=2"
    payload = {
        "items": [
            {"startTime": "20260321T070000.000Z", "members": []},
            {"startTime": "20260314T070000.000Z", "members": []},
        ]
    }
    bot = _FakeBot({plural_path: payload})

    rows = asyncio.run(get_raid_weekends(bot, "#2JJJCCRQR", limit=2))
    assert len(rows) == 2
    assert bot.calls
    assert "capitalraidseasons" in bot.calls[0]


def test_get_raid_weekends_falls_back_to_legacy_singular_endpoint():
    tag_q = "%232JJJCCRQR"
    plural_path = f"/clans/{tag_q}/capitalraidseasons?limit=1"
    singular_path = f"/clans/{tag_q}/capitalraidseason?limit=1"
    bot = _FakeBot({
        plural_path: None,
        singular_path: {"items": [{"startTime": "20260321T070000.000Z", "members": []}]},
    })

    rows = asyncio.run(get_raid_weekends(bot, "#2JJJCCRQR", limit=1))
    assert len(rows) == 1
    assert bot.calls[0] == plural_path
    assert bot.calls[1] == singular_path
