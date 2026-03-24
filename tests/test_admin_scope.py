import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _resolve_scope_clans  # noqa: E402


class _DummyBot:
    def __init__(self):
        self._guild = [{"name": "Guild Clan", "tag": "#GUILD1"}]
        self._family = [
            {"name": "Guild Clan", "tag": "#GUILD1"},
            {"name": "Family Clan", "tag": "#FAMILY1"},
        ]

    def get_scoped_clans(self, guild_id=None):
        return list(self._guild)

    def get_all_monitored_clans(self):
        return list(self._family)


def test_resolve_scope_clans_guild_vs_family():
    bot = _DummyBot()
    guild = _resolve_scope_clans(bot, guild_id=123, scope="guild")
    family = _resolve_scope_clans(bot, guild_id=123, scope="family")

    assert len(guild) == 1
    assert guild[0]["tag"] == "#GUILD1"

    assert len(family) == 2
    tags = {c["tag"] for c in family}
    assert "#GUILD1" in tags
    assert "#FAMILY1" in tags
