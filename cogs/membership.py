"""Join / leave tracking with anti-spam guards (per-clan background task)."""
import re
import logging
import asyncio
import urllib.parse
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands

from config import CHECK_INTERVAL
from storage import (
    load_strict_cache,
    save_strict_cache,
    load_member_activity,
    save_member_activity,
    load_transfers_data,
    save_transfers_data,
)
from embeds import build_join_embed, build_leave_embed

logger = logging.getLogger("cc2bot.cogs.membership")


# ────────────────────────────────────────────
# Persistent view: Compact / Detailed toggle for join embeds
# ────────────────────────────────────────────

class JoinEmbedView(discord.ui.View):
    """Compact / Detailed toggle buttons for join announcement embeds.

    Uses `timeout=None` + hard-coded `custom_id` so buttons survive
    bot restarts (persistent view).
    """

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="\U0001f4f1 Compact",
        style=discord.ButtonStyle.blurple,
        custom_id="persistent:join_compact",
    )
    async def compact_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if not embed:
            return await interaction.response.defer()

        tag = self._extract_tag(embed)
        clan_name = self._extract_clan(embed)
        member_count, member_cap = self._extract_member_counts(embed)
        if not tag:
            return await interaction.response.defer()

        player = await interaction.client.get_player(tag)
        if not player:
            # Already in compact → just acknowledge
            if self._is_layout(embed, "compact"):
                return await interaction.response.defer()
            return await interaction.response.send_message(
                "⚠️ Could not fetch player data to switch layout.", ephemeral=True
            )

        new_embed = build_join_embed(
            player,
            tag,
            clan_name,
            member_count=member_count,
            member_cap=member_cap,
            layout="compact",
        )
        self._update_button_styles("compact")
        await interaction.response.edit_message(embed=new_embed, view=self)

    @discord.ui.button(
        label="\U0001f5a5\ufe0f Detailed",
        style=discord.ButtonStyle.gray,
        custom_id="persistent:join_detailed",
    )
    async def detailed_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0] if interaction.message.embeds else None
        if not embed:
            return await interaction.response.defer()

        tag = self._extract_tag(embed)
        clan_name = self._extract_clan(embed)
        member_count, member_cap = self._extract_member_counts(embed)
        if not tag:
            return await interaction.response.defer()

        player = await interaction.client.get_player(tag)
        if not player:
            return await interaction.response.send_message(
                "⚠️ Could not fetch player data to switch layout.", ephemeral=True
            )

        new_embed = build_join_embed(
            player,
            tag,
            clan_name,
            member_count=member_count,
            member_cap=member_cap,
            layout="detailed",
        )
        self._update_button_styles("detailed")
        await interaction.response.edit_message(embed=new_embed, view=self)

    # ── helpers ──

    @staticmethod
    def _is_layout(embed: discord.Embed, layout: str) -> bool:
        """Check current layout by looking for fields unique to detailed."""
        field_names = [f.name for f in embed.fields]
        has_lifetime = any("LIFETIME" in (n or "").upper() for n in field_names)
        if layout == "detailed":
            return has_lifetime
        return not has_lifetime

    def _extract_tag(self, embed: discord.Embed) -> Optional[str]:
        """Pull player tag from the embed title or footer."""
        # Footer stores tag after bullet: "CC2 Clash Bot — Player Joined • #TAG"
        if embed.footer and embed.footer.text:
            m = re.search(r"#[A-Z0-9]+", embed.footer.text)
            if m:
                return m.group(0)
        # Fallback: title "PLAYER JOINED — Name (#TAG)"
        if embed.title:
            m = re.search(r"#[A-Z0-9]+", embed.title)
            if m:
                return m.group(0)
        return None

    def _extract_clan(self, embed: discord.Embed) -> Optional[str]:
        """Pull clan name from the CLAN field."""
        for field in embed.fields:
            if "CLAN" in (field.name or "").upper():
                # Field value is like "Clan: **CC2 Academy**\nRole: ..."
                m = re.search(r"Clan:\s*\*\*(.+?)\*\*", field.value or "")
                if m:
                    return m.group(1)
        return None

    def _extract_member_counts(self, embed: discord.Embed) -> tuple[Optional[int], int]:
        """Extract member count/cap from the CLAN field if present."""
        for field in embed.fields:
            if "CLAN" not in (field.name or "").upper():
                continue
            m = re.search(r"Members:\s*\*\*(\d+)\s*/\s*(\d+)\*\*", field.value or "")
            if m:
                try:
                    return int(m.group(1)), int(m.group(2))
                except Exception:
                    break
        return None, 50

    def _update_button_styles(self, active: str):
        """Set active button to blurple, other to gray."""
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if "compact" in (item.custom_id or ""):
                    item.style = discord.ButtonStyle.blurple if active == "compact" else discord.ButtonStyle.gray
                elif "detailed" in (item.custom_id or ""):
                    item.style = discord.ButtonStyle.blurple if active == "detailed" else discord.ButtonStyle.gray


class MembershipCog(commands.Cog, name="Membership"):
    """Monitors clans for member joins and leaves."""

    def __init__(self, bot):
        self.bot = bot
        self._tasks: Dict[str, asyncio.Task] = {}
        self._transfer_suppress_until: Dict[str, datetime] = {}
        self._onboarding_dm_sent_at: Dict[tuple[int, int], datetime] = {}

    @staticmethod
    def _fmt_mention_or_fallback(raw_value: Any, fallback: str) -> str:
        """Render a mention from an ID-like setting, otherwise return fallback text."""
        try:
            value = int(raw_value or 0)
        except Exception:
            value = 0
        if value > 0:
            return f"<#{value}>"
        return fallback

    def _build_onboarding_dm_embed(self, member: discord.Member) -> discord.Embed:
        """Build a guided onboarding DM for newly joined Discord members."""
        guild_id = member.guild.id if member.guild else None
        announce_raw = self.bot.resolve_effective_setting("announce_channel_id", 0, guild_id=guild_id)
        announce_hint = self._fmt_mention_or_fallback(announce_raw, "your clan channel")

        emb = discord.Embed(
            title="👋 Welcome to CC2 Academy",
            description=(
                f"Hi {member.mention}, welcome in. Here is your quick start guide so you can "
                "get verified and start using bot tools right away."
            ),
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(
            name="Step 1 • Link your Clash account",
            value="Use `cc2 link #PLAYER_TAG` (example: `cc2 link #2PQUE2J`) in the server.",
            inline=False,
        )
        emb.add_field(
            name="Step 2 • Set your main account",
            value="Run `cc2 setmain #PLAYER_TAG` so commands use your primary account by default.",
            inline=False,
        )
        emb.add_field(
            name="Step 3 • Learn key commands",
            value="Try `cc2 help`, `cc2 profile`, `cc2 raidsummary`, and `cc2 warpreview`.",
            inline=False,
        )
        emb.add_field(
            name="Where to post",
            value=(
                f"Use {announce_hint} for clan updates and command activity. "
                "If your DMs are open, you will also receive some reminders here."
            ),
            inline=False,
        )
        emb.set_footer(text=f"CC2 Clash Bot • Onboarding • {member.guild.name}")
        return emb

    async def send_onboarding_dm(self, member: discord.Member, *, force: bool = False) -> bool:
        """Send onboarding DM to a member.

        Returns True when DM was delivered, else False.
        """
        if not member or getattr(member, "bot", False):
            return False
        if not member.guild:
            return False

        guild_id = member.guild.id
        if not force:
            enabled = bool(self.bot.resolve_effective_setting("onboarding_dm_enabled", True, guild_id=guild_id))
            if not enabled:
                return False

        now = datetime.now(timezone.utc)
        key = (guild_id, member.id)
        sent_at = self._onboarding_dm_sent_at.get(key)
        if (not force) and sent_at and (now - sent_at) <= timedelta(minutes=10):
            return False

        try:
            dm_channel = member.dm_channel or await member.create_dm()
            await dm_channel.send(embed=self._build_onboarding_dm_embed(member))
            self._onboarding_dm_sent_at[key] = now
            logger.info("Sent onboarding DM to %s (%s) in guild %s", member.name, member.id, guild_id)
            return True
        except Exception as exc:
            logger.info("Onboarding DM skipped for %s (%s): %s", member.name, member.id, exc)
            return False

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """DM a guided onboarding message to newly joined Discord members."""
        await self.send_onboarding_dm(member, force=False)

    async def cog_load(self):
        """Start tracking for all currently monitored clans."""
        for clan in self.bot.get_all_monitored_clans():
            self.start_tracking(clan)

    async def cog_unload(self):
        """Cancel all tracking tasks on cog unload."""
        for tag in list(self._tasks):
            self.stop_tracking(tag)

    # ── public API for admin cog ──
    def start_tracking(self, clan: Dict[str, str]):
        tag = clan["tag"]
        if tag in self._tasks:
            return
        self._tasks[tag] = asyncio.create_task(self._track_clan(clan))
        logger.info(f"Started membership tracker for {clan['name']} ({tag})")

    def stop_tracking(self, clan_tag: str):
        task = self._tasks.pop(clan_tag, None)
        if task:
            task.cancel()
            logger.info(f"Stopped membership tracker for {clan_tag}")

    # ── the per-clan background loop ──
    async def _track_clan(self, clan: Dict[str, str]):
        await self.bot.wait_until_ready()
        clan_name = clan["name"]
        clan_tag = clan["tag"]

        # load persistent cache
        self.bot.strict_join_cache[clan_tag] = load_strict_cache(clan_tag)
        activity = load_member_activity()
        activity.setdefault(clan_tag, {})
        consecutive_empty = 0

        while not self.bot.is_closed():
            try:
                await asyncio.sleep(CHECK_INTERVAL)
                member_list = await self.bot.get_clan_member_list(clan_tag)

                # ── Guard 1: empty fetch ──
                if not member_list:
                    consecutive_empty += 1
                    if consecutive_empty == 5:
                        logger.warning(f"{clan_name}: 5+ consecutive empty fetches — skipping leave detection")
                    continue
                consecutive_empty = 0

                current_tags = {m["tag"]: m.get("name") for m in member_list if m.get("tag")}
                prev_tags = self.bot.strict_join_cache.get(clan_tag, set())
                member_count_live = len(current_tags)
                member_cap = 50
                try:
                    clan_data = await self.bot.coc_get(f"/clans/{urllib.parse.quote(clan_tag)}")
                    if isinstance(clan_data, dict):
                        member_cap = int(clan_data.get("maxMembers", 50) or 50)
                except Exception:
                    member_cap = 50

                # Persist last seen timestamp for all currently present members.
                now_iso = datetime.now(timezone.utc).isoformat()
                for m in member_list:
                    ptag = m.get("tag")
                    if not ptag:
                        continue
                    pname = m.get("name") or ptag
                    current_stats = {
                        "donations": int(m.get("donations", 0) or 0),
                        "received": int(m.get("donationsReceived", 0) or 0),
                        "trophies": int(m.get("trophies", 0) or 0),
                    }
                    previous = activity[clan_tag].get(ptag, {}) if isinstance(activity[clan_tag].get(ptag, {}), dict) else {}
                    previous_stats = previous.get("stats", {}) if isinstance(previous.get("stats", {}), dict) else {}

                    # "last_seen" = present in clan
                    # "last_progress_seen" = measurable game-stat change while tracked
                    progress_seen = previous.get("last_progress_seen")
                    if not progress_seen:
                        progress_seen = now_iso
                    if previous_stats and current_stats != previous_stats:
                        progress_seen = now_iso

                    activity[clan_tag][ptag] = {
                        "name": pname,
                        "last_seen": now_iso,
                        "last_progress_seen": progress_seen,
                        "stats": current_stats,
                    }
                save_member_activity(activity)

                # ── Baseline initialization (first run) ──
                if not prev_tags:
                    if current_tags:
                        self.bot.strict_join_cache[clan_tag] = set(current_tags.keys())
                    else:
                        self.bot.strict_join_cache[clan_tag] = set()
                    try:
                        save_strict_cache(clan_tag, self.bot.strict_join_cache[clan_tag])
                    except Exception:
                        pass
                    if clan_tag not in self.bot.initialized_baseline:
                        logger.info(f"Baseline initialized for {clan_name} ({clan_tag})")
                        self.bot.initialized_baseline.add(clan_tag)
                    continue

                # ── Joins ──
                joins = [t for t in current_tags if t not in prev_tags]
                for tag in joins:
                    suppress_until = self._transfer_suppress_until.get(tag)
                    if suppress_until and datetime.now(timezone.utc) <= suppress_until:
                        self.bot.strict_join_cache[clan_tag].add(tag)
                        continue

                    player = await self.bot.get_player(tag)
                    if player:
                        emb = build_join_embed(
                            player,
                            tag,
                            clan_name,
                            member_count=member_count_live,
                            member_cap=member_cap,
                        )
                        view = JoinEmbedView()
                    else:
                        name = current_tags.get(tag, tag)
                        emb = discord.Embed(
                            title=f"🟢 PLAYER JOINED — {name}",
                            description=f"`{tag}` joined **{clan_name}**",
                            color=0x2ecc71,
                            timestamp=datetime.now(timezone.utc),
                        )
                        emb.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
                        emb.add_field(name="👥 Members", value=f"**{member_count_live}/{member_cap}**", inline=True)
                        view = None
                    try:
                        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
                        for channel in channels:
                            await channel.send(embed=emb, view=view or discord.utils.MISSING)
                            await asyncio.sleep(0.12)
                    except Exception as e:
                        logger.error(f"Join send failed for {tag}: {e}")
                    # Record every join event so dashboard history is not limited to transfers only.
                    self._record_join_event(clan_name, clan_tag, tag)
                    self.bot.strict_join_cache[clan_tag].add(tag)

                if joins:
                    save_strict_cache(clan_tag, self.bot.strict_join_cache[clan_tag])

                # ── Leaves ──
                leaves = [t for t in list(prev_tags) if t not in current_tags]

                # ── Guard 2: mass-leave threshold ──
                if len(leaves) > max(5, len(prev_tags) * 0.5):
                    logger.warning(
                        f"{clan_name}: {len(leaves)} leaves at once (roster={len(prev_tags)}) — API glitch, skipping"
                    )
                    continue

                for tag in leaves:
                    transfer_dest = await self._find_transfer_destination(clan_tag, tag)
                    if transfer_dest:
                        await self._announce_transfer(clan_name, clan_tag, transfer_dest, tag)
                        self._record_transfer(clan_name, clan_tag, transfer_dest, tag)
                        self._transfer_suppress_until[tag] = datetime.now(timezone.utc) + timedelta(minutes=2)
                        if tag in self.bot.strict_join_cache[clan_tag]:
                            self.bot.strict_join_cache[clan_tag].remove(tag)
                        continue

                    leave_player = await self.bot.get_player(tag)
                    name = leave_player.get("name", tag) if leave_player else tag
                    emb = build_leave_embed(
                        tag,
                        name,
                        member_count=max(member_count_live, 0),
                        member_cap=member_cap,
                    )
                    try:
                        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
                        for channel in channels:
                            await channel.send(embed=emb)
                            await asyncio.sleep(0.12)
                    except Exception as e:
                        logger.error(f"Leave send failed for {tag}: {e}")
                    # Record a leave with unknown destination for history/trend analytics.
                    self._record_leave_event(clan_name, clan_tag, tag)
                    if tag in self.bot.strict_join_cache[clan_tag]:
                        self.bot.strict_join_cache[clan_tag].remove(tag)

                # Batched save (once per cycle, not per-leave)
                if leaves:
                    save_strict_cache(clan_tag, self.bot.strict_join_cache[clan_tag])

                # Clean up expired suppression entries.
                now_dt = datetime.now(timezone.utc)
                expired = [k for k, v in self._transfer_suppress_until.items() if v < now_dt]
                for k in expired:
                    self._transfer_suppress_until.pop(k, None)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Tracker error for {clan_name}: {e}")
                await asyncio.sleep(CHECK_INTERVAL)

    async def _find_transfer_destination(self, source_clan_tag: str, player_tag: str) -> Optional[Dict[str, str]]:
        """Return destination clan if player tag appears in another monitored clan."""
        target = str(player_tag or "").upper()
        for c in self.bot.get_all_monitored_clans():
            ctag = str(c.get("tag", "")).upper()
            if not ctag or ctag == str(source_clan_tag).upper():
                continue
            try:
                members = await self.bot.get_clan_member_list(c.get("tag"))
            except Exception:
                continue
            for m in members or []:
                if str(m.get("tag", "")).upper() == target:
                    return c
        return None

    async def _announce_transfer(
        self,
        source_name: str,
        source_tag: str,
        destination: Dict[str, str],
        player_tag: str,
    ) -> None:
        player = await self.bot.get_player(player_tag)
        pname = (player or {}).get("name", player_tag)
        emb = discord.Embed(
            title="🔁 Player Transfer Detected",
            description=f"**{pname}** `{player_tag}` moved between family clans.",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="From", value=f"{source_name} `{source_tag}`", inline=True)
        emb.add_field(name="To", value=f"{destination.get('name', 'Unknown')} `{destination.get('tag', '')}`", inline=True)
        emb.set_footer(text="CC2 Clash Bot • Transfer Log")

        channels = await self.bot.get_all_announce_channels()
        for channel in channels:
            await channel.send(embed=emb)

    def _record_transfer(
        self,
        source_name: str,
        source_tag: str,
        destination: Dict[str, str],
        player_tag: str,
    ) -> None:
        data = load_transfers_data()
        if not isinstance(data, dict):
            data = {}
        events = data.get("events", [])
        if not isinstance(events, list):
            events = []

        events.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "player_tag": player_tag,
                "from": {"name": source_name, "tag": source_tag},
                "to": {"name": destination.get("name", "Unknown"), "tag": destination.get("tag", "")},
            }
        )

        data["events"] = events[-500:]
        save_transfers_data(data)

    def _record_join_event(self, clan_name: str, clan_tag: str, player_tag: str) -> None:
        """Persist a join event (origin unknown) for clan history views."""
        data = load_transfers_data()
        if not isinstance(data, dict):
            data = {}
        events = data.get("events", [])
        if not isinstance(events, list):
            events = []

        events.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "player_tag": player_tag,
                "from": {"name": "Unknown", "tag": ""},
                "to": {"name": clan_name, "tag": clan_tag},
            }
        )

        data["events"] = events[-500:]
        save_transfers_data(data)

    def _record_leave_event(self, clan_name: str, clan_tag: str, player_tag: str) -> None:
        """Persist a leave event (destination unknown) for clan history views."""
        data = load_transfers_data()
        if not isinstance(data, dict):
            data = {}
        events = data.get("events", [])
        if not isinstance(events, list):
            events = []

        events.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "player_tag": player_tag,
                "from": {"name": clan_name, "tag": clan_tag},
                "to": {"name": "Unknown", "tag": ""},
            }
        )

        data["events"] = events[-500:]
        save_transfers_data(data)


async def setup(bot):
    await bot.add_cog(MembershipCog(bot))
