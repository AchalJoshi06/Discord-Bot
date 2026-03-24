"""Donation tracking commands and monthly snapshot loop."""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import calendar

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config import MONTHLY_SNAPSHOT_DAY, MIN_DONATION_RATIO, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID
from storage import (
    get_linked_tag_for_user,
    load_monthly_leaderboard,
    save_monthly_leaderboard,
    save_leaderboard_snapshot,
)
from donations import (
    extract_lifetime_donations, create_donation_snapshot,
    save_monthly_snapshot, get_donation_history,
    get_player_donation_stats, get_current_month_key,
)
from embeds import build_donation_embed
from cogs.profiles import clan_autocomplete, record_rush_history_for_player
from utils.helpers import normalize_tag, is_valid_tag, ClanSelectView, has_leadership_role
from calculations import calculate_activity_score

logger = logging.getLogger("cc2bot.cogs.donations")


class DonationsCog(commands.Cog, name="Donations"):
    """Donation statistics, history, and monthly snapshots."""

    def __init__(self, bot):
        self.bot = bot
        self._last_snapshot_month: Dict[str, str] = {}
        self._last_prereset_month: Dict[str, str] = {}
        self._last_zero_donation_count: Dict[str, int] = {}

    @staticmethod
    def _is_final_hour_before_month_reset(now: datetime) -> bool:
        last_day = calendar.monthrange(now.year, now.month)[1]
        return now.day == last_day and now.hour == 23

    def _should_trigger_reset_drop_snapshot(self, clan_tag: str, player_cache: Dict[str, Dict[str, Any]]) -> bool:
        zero_count = 0
        total = 0
        for p in player_cache.values():
            total += 1
            if int(p.get("donations", 0) or 0) == 0:
                zero_count += 1

        prev = self._last_zero_donation_count.get(clan_tag, 0)
        self._last_zero_donation_count[clan_tag] = zero_count
        if total == 0:
            return False

        # Heuristic: a sudden jump to many zero-donation members often signals a season reset wave.
        return zero_count >= 5 and zero_count >= (prev + 5)

    async def _take_and_announce_snapshot(
        self,
        clan: Dict[str, str],
        current_month: str,
        channels: List[Any],
        members: List[Dict[str, Any]],
        player_cache: Dict[str, Dict[str, Any]],
        reason: str,
    ) -> bool:
        clan_tag = clan["tag"]
        clan_name = clan["name"]
        snapshot = create_donation_snapshot(clan_tag, members, player_cache)
        success = save_monthly_snapshot(clan_tag, snapshot)
        if not success:
            return False

        self._last_snapshot_month[clan_tag] = current_month
        self._save_monthly_leaderboard(clan_tag, current_month, members, player_cache)
        member_count = len(snapshot.get("members", {}))
        logger.info(f"Donation snapshot saved for {clan_name}: {member_count} members ({reason})")

        await self._announce_low_ratio_alerts(clan, current_month, channels)
        for channel in channels:
            embed = discord.Embed(
                title=f"📸 Monthly Donation Snapshot — {clan_name}",
                color=0x3498db,
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(name="Month", value=current_month, inline=True)
            embed.add_field(name="Members", value=str(member_count), inline=True)
            embed.description = f"Donation snapshot taken ({reason}). Use `/donationhistory` to view."
            await channel.send(embed=embed)
            await self._announce_monthly_mvp(
                clan_name=clan_name,
                month_key=current_month,
                members=members,
                player_cache=player_cache,
                channel=channel,
            )
        return True

    def _save_monthly_leaderboard(
        self,
        clan_tag: str,
        month_key: str,
        members: List[Dict[str, Any]],
        player_cache: Dict[str, Dict[str, Any]],
    ) -> None:
        """Store per-month leaderboard metrics for later /top monthly support."""
        data = load_monthly_leaderboard()
        if not isinstance(data, dict):
            data = {}
        data.setdefault(clan_tag, {})

        month_payload: Dict[str, Any] = {"members": {}}
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            p = player_cache.get(tag)
            if not p:
                continue

            act = calculate_activity_score(p)
            month_payload["members"][tag] = {
                "name": p.get("name", m.get("name", "Unknown")),
                "donations": int(p.get("donations", 0) or 0),
                "received": int(p.get("donationsReceived", 0) or 0),
                "war_stars": int(p.get("warStars", 0) or 0),
                "trophies": int(p.get("trophies", 0) or 0),
                "activity_score": float(act.get("score", 0.0)),
            }

        data[clan_tag][month_key] = month_payload
        save_monthly_leaderboard(data)
        save_leaderboard_snapshot(clan_tag, month_key, month_payload)

    @staticmethod
    def _previous_month_key(month_key: str) -> Optional[str]:
        try:
            dt = datetime.strptime(month_key, "%Y-%m")
        except Exception:
            return None
        year = dt.year
        month = dt.month - 1
        if month == 0:
            year -= 1
            month = 12
        return f"{year:04d}-{month:02d}"

    def _low_ratio_streak_candidates(self, clan_tag: str, month_key: str) -> List[Dict[str, Any]]:
        data = load_monthly_leaderboard()
        if not isinstance(data, dict):
            return []

        clan_rows = data.get(clan_tag, {})
        if not isinstance(clan_rows, dict):
            return []

        prev_month = self._previous_month_key(month_key)
        if not prev_month:
            return []

        current = ((clan_rows.get(month_key, {}) if isinstance(clan_rows.get(month_key, {}), dict) else {})
                   .get("members", {}))
        previous = ((clan_rows.get(prev_month, {}) if isinstance(clan_rows.get(prev_month, {}), dict) else {})
                    .get("members", {}))
        if not isinstance(current, dict) or not isinstance(previous, dict):
            return []

        out: List[Dict[str, Any]] = []
        for tag, cur in current.items():
            if tag not in previous:
                continue
            prev = previous.get(tag, {}) if isinstance(previous.get(tag, {}), dict) else {}
            if not isinstance(cur, dict):
                continue

            cur_don = int(cur.get("donations", 0) or 0)
            cur_rec = int(cur.get("received", 0) or 0)
            prev_don = int(prev.get("donations", 0) or 0)
            prev_rec = int(prev.get("received", 0) or 0)

            cur_ratio = cur_don / max(1, cur_rec)
            prev_ratio = prev_don / max(1, prev_rec)
            if cur_ratio < MIN_DONATION_RATIO and prev_ratio < MIN_DONATION_RATIO:
                out.append({
                    "name": cur.get("name", "Unknown"),
                    "tag": tag,
                    "prev_ratio": prev_ratio,
                    "cur_ratio": cur_ratio,
                })

        out.sort(key=lambda x: x["cur_ratio"])
        return out

    async def _announce_low_ratio_alerts(self, clan: Dict[str, str], month_key: str, channels: List[Any]) -> None:
        flagged = self._low_ratio_streak_candidates(clan["tag"], month_key)
        if not flagged:
            return

        emb = discord.Embed(
            title=f"⚠️ Donation Ratio Alert — {clan['name']}",
            description=(
                f"Members below ratio threshold (**{MIN_DONATION_RATIO:.2f}**) "
                "for 2 consecutive months."
            ),
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        lines = []
        for row in flagged[:25]:
            lines.append(
                f"• **{row['name']}** `{row['tag']}` — "
                f"{row['prev_ratio']:.2f} → {row['cur_ratio']:.2f}"
            )
        emb.add_field(name="Flagged", value="\n".join(lines) if lines else "None", inline=False)
        emb.set_footer(text="Leadership review recommended")

        for channel in channels:
            await channel.send(embed=emb)

    async def _announce_monthly_mvp(
        self,
        clan_name: str,
        month_key: str,
        members: List[Dict[str, Any]],
        player_cache: Dict[str, Dict[str, Any]],
        channel,
    ):
        """Announce top performers for donations, war stars, and activity score."""
        scored: List[Dict[str, Any]] = []
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            p = player_cache.get(tag)
            if not p:
                continue
            act = calculate_activity_score(p)
            scored.append({
                "name": p.get("name", m.get("name", "Unknown")),
                "tag": tag,
                "donations": int(p.get("donations", 0) or 0),
                "war_stars": int(p.get("warStars", 0) or 0),
                "activity_score": float(act.get("score", 0.0)),
            })

        if not scored:
            return

        top_donation = max(scored, key=lambda x: x["donations"])
        top_war = max(scored, key=lambda x: x["war_stars"])
        top_activity = max(scored, key=lambda x: x["activity_score"])

        emb = discord.Embed(
            title=f"🏆 Season MVP — {clan_name}",
            description=f"Top performers for **{month_key}**",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(
            name="💝 Most Donations",
            value=f"**{top_donation['name']}** `{top_donation['tag']}`\n{top_donation['donations']:,}",
            inline=True,
        )
        emb.add_field(
            name="⭐ Most War Stars",
            value=f"**{top_war['name']}** `{top_war['tag']}`\n{top_war['war_stars']:,}",
            inline=True,
        )
        emb.add_field(
            name="📈 Best Activity Score",
            value=f"**{top_activity['name']}** `{top_activity['tag']}`\n{top_activity['activity_score']:.2f}/100",
            inline=True,
        )
        emb.set_footer(text="CC2 Clash Bot • Monthly MVP")
        await channel.send(embed=emb)

    async def cog_load(self):
        self.monthly_snapshot.start()

    async def cog_unload(self):
        self.monthly_snapshot.cancel()

    # ── background: monthly donation snapshot ──
    @tasks.loop(seconds=3600)
    async def monthly_snapshot(self):
        now = datetime.now(timezone.utc)
        is_regular_snapshot = now.day == MONTHLY_SNAPSHOT_DAY
        is_prereset_window = self._is_final_hour_before_month_reset(now)
        if not is_regular_snapshot and not is_prereset_window:
            return
        current_month = get_current_month_key()

        for clan in self.bot.get_all_monitored_clans():
            clan_tag = clan["tag"]
            clan_name = clan["name"]
            if self._last_snapshot_month.get(clan_tag) == current_month:
                continue
            try:
                members = await self.bot.get_clan_member_list(clan_tag)
                if not members:
                    continue
                tags = [m.get("tag") for m in members if m.get("tag")]
                player_cache = await self.bot.fetch_players(tags)
                if not player_cache:
                    continue

                channels = await self.bot.get_announce_channels_for_clan(clan_tag)

                if is_prereset_window and self._last_prereset_month.get(clan_tag) != current_month:
                    reset_drop_detected = self._should_trigger_reset_drop_snapshot(clan_tag, player_cache)
                    reason = "pre-reset final hour"
                    if reset_drop_detected:
                        reason = "pre-reset final hour (reset-drop detected)"
                    await self._take_and_announce_snapshot(
                        clan=clan,
                        current_month=current_month,
                        channels=channels,
                        members=members,
                        player_cache=player_cache,
                        reason=reason,
                    )
                    self._last_prereset_month[clan_tag] = current_month

                if is_regular_snapshot and self._last_snapshot_month.get(clan_tag) != current_month:
                    await self._take_and_announce_snapshot(
                        clan=clan,
                        current_month=current_month,
                        channels=channels,
                        members=members,
                        player_cache=player_cache,
                        reason="scheduled monthly run",
                    )
            except Exception as e:
                logger.error(f"Snapshot error for {clan_name}: {e}")

    @monthly_snapshot.before_loop
    async def before_monthly(self):
        await self.bot.wait_until_ready()

    # ═══════════════════════════════════
    # /donations  +  cc2 donations
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="donations", aliases=["don"],
        description="View donation statistics for a player",
    )
    @app_commands.describe(tag="Player tag — optional; uses linked tag if omitted")
    async def donations(self, ctx: commands.Context, *, tag: Optional[str] = None):
        await ctx.defer()
        if not tag:
            linked = get_linked_tag_for_user(ctx.author.id)
            if not linked:
                return await ctx.send("❌ No tag provided and no linked account.")
            tag_norm = normalize_tag(linked)
        else:
            tag_norm = normalize_tag(tag)

        if not is_valid_tag(tag_norm):
            return await ctx.send("❌ Invalid player tag format. Use format like #2PQUE2J.")

        player = await self.bot.get_player(tag_norm)
        if not player:
            return await ctx.send(f"❌ Could not fetch player `{tag_norm}`.")

        lifetime = extract_lifetime_donations(player)
        seasonal = player.get("donations", 0)
        received = player.get("donationsReceived", 0)

        embed = discord.Embed(
            title=f"💝 Donation Stats — {player.get('name', 'Unknown')}",
            color=0x2ecc71,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="🆔 Tag", value=f"`{tag_norm}`", inline=True)
        embed.add_field(
            name="📊 Lifetime Donations",
            value=(
                f"Troops: **{lifetime['troops_donated']:,}**\n"
                f"Spells: **{lifetime['spells_donated']:,}**\n"
                f"Siege: **{lifetime['siege_donated']:,}**\n"
                f"**Total: {lifetime['total_donated']:,}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="📅 Current Season",
            value=f"Sent: **{seasonal:,}**\nReceived: **{received:,}**",
            inline=True,
        )
        for clan in self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None):
            stats = get_player_donation_stats(tag_norm, clan["tag"])
            if stats:
                embed.add_field(
                    name="📈 Tracked Stats",
                    value=(
                        f"Tracking since: **{stats.get('tracked_from', 'N/A')}**\n"
                        f"Last snapshot: **{stats.get('snapshot_date', 'N/A')}**"
                    ),
                    inline=True,
                )
                break
        embed.set_footer(text="Lifetime stats from achievements • Seasonal from current season")
        await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /donationhistory
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="donationhistory", aliases=["dh"],
        description="View monthly donation history for a clan",
    )
    @app_commands.describe(
        clan="Clan to check (ALL CLANS for aggregated view)",
        months="Number of months to show (default 6, max 24)",
        scope="guild or family",
    )
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def donationhistory(self, ctx: commands.Context, clan: Optional[str] = None, months: int = 6, scope: str = "guild"):
        await ctx.defer()
        months = max(1, min(months, 24))

        from cogs.admin import resolve_clans, _resolve_scope_clans

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.")

        if not clan:
            scoped = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for donation history",
                    include_all=True,
                )
                await ctx.send("Select a clan for donation history:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        if not clan or clan == "ALL":
            scoped_clans = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            embed = discord.Embed(
                title=f"📊 Donation History — All Clans ({scope_val})",
                color=0x3498db,
                timestamp=datetime.now(timezone.utc),
            )
            for c in scoped_clans:
                history = get_donation_history(c["tag"], limit=months)
                if not history:
                    val = "⚠️ No donation history found."
                else:
                    lines = [f"{m.get('month', '?')}: {m.get('total_monthly', 0):,}" for m in history[:months]]
                    val = "\n".join(lines)
                embed.add_field(name=c["name"], value=val or "No data", inline=False)
            embed.set_footer(text=f"Snapshots on the {MONTHLY_SNAPSHOT_DAY}th — showing up to {months} months")
            return await ctx.send(embed=embed)

        if scope_val == "family":
            clans_to_check = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            tag_norm = clan.strip().upper()
            if not tag_norm.startswith("#"):
                tag_norm = "#" + tag_norm
            clans_to_check = [c for c in clans_to_check if c.get("tag", "").upper() == tag_norm]
        else:
            clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")
        if isinstance(clans_to_check, list) and not clans_to_check:
            return await ctx.send("❌ Clan not found.")
        clan_obj = clans_to_check[0]
        history = get_donation_history(clan_obj["tag"], limit=months)
        if not history:
            return await ctx.send(f"⚠️ No donation history for **{clan_obj['name']}**.")

        embed = discord.Embed(
            title=f"📊 Donation History — {clan_obj['name']}",
            color=0x3498db,
            timestamp=datetime.now(timezone.utc),
        )
        lines, total = [], 0
        for md in history[:months]:
            month = md.get("month", "Unknown")
            tot = md.get("total_monthly", 0)
            total += tot
            cnt = len(md.get("members", {}))
            lines.append(f"**{month}**: {tot:,} donations ({cnt} members)")
        embed.description = "\n".join(lines) if lines else "No data."
        embed.add_field(
            name="📈 Summary",
            value=f"Total tracked: **{total:,}** donations\nMonths shown: **{len(history)}**",
            inline=False,
        )
        embed.set_footer(text="Monthly snapshots taken on the 1st of each month")
        await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /takesnapshot
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="takesnapshot", aliases=["ts"],
        description="Manually take a donation snapshot for a clan (Leadership/Admin only)",
    )
    @commands.cooldown(1, 60, commands.BucketType.user)
    @app_commands.describe(clan="Clan to snapshot", scope="guild or family")
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def takesnapshot(self, ctx: commands.Context, clan: Optional[str] = None, scope: str = "guild"):
        await ctx.defer()

        if not has_leadership_role(ctx.author, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            return await ctx.send("❌ Leadership/Admin role required for this command.")

        from cogs.admin import resolve_clans, _resolve_scope_clans

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.")

        if not clan:
            scoped = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for donation snapshot",
                    include_all=True,
                )
                await ctx.send("Select a clan for donation snapshot:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        if not clan or clan == "ALL":
            scoped_clans = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            results = []
            for c in scoped_clans:
                members = await self.bot.get_clan_member_list(c["tag"])
                if not members:
                    results.append((c["name"], False, "No members"))
                    continue
                tags = [m.get("tag") for m in members if m.get("tag")]
                pc = await self.bot.fetch_players(tags)
                if not pc:
                    results.append((c["name"], False, "No player data"))
                    continue
                rush_saved = 0
                for p in pc.values():
                    if record_rush_history_for_player(p, clan_tag=c["tag"]):
                        rush_saved += 1
                snap = create_donation_snapshot(c["tag"], members, pc)
                ok = save_monthly_snapshot(c["tag"], snap)
                results.append(
                    (
                        c["name"],
                        ok,
                        f"Saved {len(snap.get('members', {}))} members | rush points: {rush_saved}" if ok else "Failed",
                    )
                )
            emb = discord.Embed(title=f"✅ Snapshot Results — All Clans ({scope_val})", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
            for rn in results:
                emb.add_field(name=rn[0], value=("✅ " + rn[2]) if rn[1] else ("❌ " + rn[2]), inline=False)
            return await ctx.send(embed=emb)

        if scope_val == "family":
            clans_to_check = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            tag_norm = clan.strip().upper()
            if not tag_norm.startswith("#"):
                tag_norm = "#" + tag_norm
            clans_to_check = [c for c in clans_to_check if c.get("tag", "").upper() == tag_norm]
        else:
            clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")
        if isinstance(clans_to_check, list) and not clans_to_check:
            return await ctx.send("❌ Clan not found.")
        clan_obj = clans_to_check[0]
        members = await self.bot.get_clan_member_list(clan_obj["tag"])
        if not members:
            return await ctx.send("❌ Could not fetch clan or empty.")
        tags = [m.get("tag") for m in members if m.get("tag")]
        pc = await self.bot.fetch_players(tags)
        if not pc:
            return await ctx.send("❌ Could not fetch player data.")
        rush_saved = 0
        for p in pc.values():
            if record_rush_history_for_player(p, clan_tag=clan_obj["tag"]):
                rush_saved += 1
        snap = create_donation_snapshot(clan_obj["tag"], members, pc)
        ok = save_monthly_snapshot(clan_obj["tag"], snap)
        if ok:
            embed = discord.Embed(title="✅ Snapshot Created", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
            embed.add_field(name="Clan", value=clan_obj["name"], inline=True)
            embed.add_field(name="Month", value=snap["date"], inline=True)
            embed.add_field(name="Members", value=str(len(snap.get("members", {}))), inline=True)
            embed.add_field(name="Rush Points", value=str(rush_saved), inline=True)
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ Failed to save snapshot.")


async def setup(bot):
    await bot.add_cog(DonationsCog(bot))
