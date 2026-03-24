import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _resolve_scope_clans  # noqa: E402


class _BotScopeStub:
    def __init__(self):
        self.guild_clans = [{"name": "Guild One", "tag": "#AAA111"}]
        self.family_clans = [
            {"name": "Guild One", "tag": "#AAA111"},
            {"name": "Family Two", "tag": "#BBB222"},
        ]

    def get_scoped_clans(self, guild_id=None):
        return list(self.guild_clans)

    def get_all_monitored_clans(self):
        return list(self.family_clans)


def test_resolve_scope_clans_for_donation_commands():
    bot = _BotScopeStub()

    guild_rows = _resolve_scope_clans(bot, guild_id=123, scope="guild")
    family_rows = _resolve_scope_clans(bot, guild_id=123, scope="family")

    assert guild_rows == [{"name": "Guild One", "tag": "#AAA111"}]
    assert len(family_rows) == 2
    tags = {c["tag"] for c in family_rows}
    assert "#AAA111" in tags
    assert "#BBB222" in tags
