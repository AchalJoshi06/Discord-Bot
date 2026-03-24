import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.membership import MembershipCog  # noqa: E402


class _DummyGuild:
    def __init__(self, gid: int, name: str = "Test Guild"):
        self.id = gid
        self.name = name


class _DummyDMChannel:
    def __init__(self):
        self.sent_embeds = []

    async def send(self, embed=None, **kwargs):
        self.sent_embeds.append(embed)
        return embed


class _DummyMember:
    def __init__(self, uid: int, gid: int = 123, bot: bool = False):
        self.id = uid
        self.name = f"User{uid}"
        self.bot = bot
        self.guild = _DummyGuild(gid)
        self.mention = f"<@{uid}>"
        self.dm_channel = None

    async def create_dm(self):
        self.dm_channel = _DummyDMChannel()
        return self.dm_channel


class _DummyBot:
    def __init__(self, enabled=True, announce_channel_id=777):
        self._enabled = enabled
        self._announce_channel_id = announce_channel_id

    def resolve_effective_setting(self, key, default=None, guild_id=None):
        if key == "onboarding_dm_enabled":
            return self._enabled
        if key == "announce_channel_id":
            return self._announce_channel_id
        return default


def test_onboarding_dm_sent_when_enabled():
    async def _run():
        cog = MembershipCog(_DummyBot(enabled=True, announce_channel_id=999))
        member = _DummyMember(uid=42)

        await cog.on_member_join(member)

        assert member.dm_channel is not None
        assert len(member.dm_channel.sent_embeds) == 1
        emb = member.dm_channel.sent_embeds[0]
        assert emb is not None
        assert "Welcome to CC2 Academy" in (emb.title or "")
        assert "Step 1" in (emb.fields[0].name if emb.fields else "")
        where_to_post_field = next((f for f in emb.fields if f.name == "Where to post"), None)
        assert where_to_post_field is not None
        assert "<#999>" in (where_to_post_field.value or "")

    asyncio.run(_run())


def test_onboarding_dm_not_sent_when_disabled():
    async def _run():
        cog = MembershipCog(_DummyBot(enabled=False))
        member = _DummyMember(uid=43)

        await cog.on_member_join(member)

        assert member.dm_channel is None

    asyncio.run(_run())


def test_onboarding_dm_is_deduplicated_for_recent_rejoin():
    async def _run():
        cog = MembershipCog(_DummyBot(enabled=True))
        member = _DummyMember(uid=44)

        await cog.on_member_join(member)
        await cog.on_member_join(member)

        assert member.dm_channel is not None
        assert len(member.dm_channel.sent_embeds) == 1

    asyncio.run(_run())


def test_onboarding_skips_bot_accounts():
    async def _run():
        cog = MembershipCog(_DummyBot(enabled=True))
        member = _DummyMember(uid=45, bot=True)

        await cog.on_member_join(member)

        assert member.dm_channel is None

    asyncio.run(_run())


def test_send_onboarding_dm_force_overrides_disabled_setting():
    async def _run():
        cog = MembershipCog(_DummyBot(enabled=False, announce_channel_id=888))
        member = _DummyMember(uid=46)

        delivered = await cog.send_onboarding_dm(member, force=True)

        assert delivered is True
        assert member.dm_channel is not None
        assert len(member.dm_channel.sent_embeds) == 1
        where_to_post_field = next((f for f in member.dm_channel.sent_embeds[0].fields if f.name == "Where to post"), None)
        assert where_to_post_field is not None
        assert "<#888>" in (where_to_post_field.value or "")

    asyncio.run(_run())


def test_send_onboarding_dm_returns_false_when_disabled_without_force():
    async def _run():
        cog = MembershipCog(_DummyBot(enabled=False))
        member = _DummyMember(uid=47)

        delivered = await cog.send_onboarding_dm(member, force=False)

        assert delivered is False
        assert member.dm_channel is None

    asyncio.run(_run())
