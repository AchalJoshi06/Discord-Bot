import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from discordwelcomebot import CC2Bot  # noqa: E402
from storage import save_settings, save_guild_settings, save_clans, save_guild_clans  # noqa: E402


def test_resolve_effective_setting_prefers_guild_override(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert save_settings({"announce_channel_id": 111}) is True
    assert save_guild_settings(123, {"announce_channel_id": 222}) is True

    bot = CC2Bot()
    assert bot.resolve_effective_setting("announce_channel_id", guild_id=123) == 222
    assert bot.resolve_effective_setting("announce_channel_id", guild_id=999) == 111


def test_get_announce_channel_uses_resolved_id(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert save_settings({"announce_channel_id": 111}) is True

    bot = CC2Bot()

    class DummyChannel:
        def __init__(self, channel_id):
            self.id = channel_id

    def fake_get_channel(cid):
        return DummyChannel(cid)

    monkeypatch.setattr(bot, "get_channel", fake_get_channel)

    import asyncio

    ch = asyncio.run(bot.get_announce_channel())
    assert ch is not None
    assert ch.id == 111


def test_get_scoped_clans_prefers_guild_override(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert save_clans([{"name": "Global Clan", "tag": "#AAA111"}]) is True
    assert save_guild_clans(123, [{"name": "Guild Clan", "tag": "#BBB222"}]) is True

    bot = CC2Bot()
    guild_scoped = bot.get_scoped_clans(123)
    global_scoped = bot.get_scoped_clans(999)

    assert guild_scoped == [{"name": "Guild Clan", "tag": "#BBB222"}]
    assert global_scoped == [{"name": "Global Clan", "tag": "#AAA111"}]


def test_all_monitored_clans_union_and_guild_mapping(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert save_clans([{"name": "Global Clan", "tag": "#AAA111"}]) is True

    bot = CC2Bot()

    class DummyGuild:
        def __init__(self, gid):
            self.id = gid

    # One guild overrides clans, one guild falls back to global.
    assert save_guild_clans(10, [{"name": "Guild Clan", "tag": "#BBB222"}]) is True
    monkeypatch.setattr(CC2Bot, "guilds", property(lambda self: [DummyGuild(10), DummyGuild(20)]))

    union = bot.get_all_monitored_clans()
    union_tags = {c["tag"] for c in union}
    assert "#AAA111" in union_tags
    assert "#BBB222" in union_tags

    mapped = bot.get_guild_ids_for_clan("#BBB222")
    assert 10 in mapped
    assert 20 not in mapped
