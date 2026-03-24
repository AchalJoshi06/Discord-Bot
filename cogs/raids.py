"""Capital Raid weekend commands, snapshots, and reminders."""
import asyncio
import logging
import urllib.parse
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from storage import (
    load_json,
    save_json,
    load_settings,
    save_settings,
    load_raid_history,
    save_raid_history,
    save_guild_settings,
    load_capital_progress_data,
    save_capital_progress_data,
)
from storage import get_linked_user_for_tag
from cogs.profiles import clan_autocomplete
from utils.helpers import truncate, safe_send, build_paginated_embeds, send_paginated_embeds, ClanSelectView

logger = logging.getLogger("cc2bot.cogs.raids")

_RAID_STREAK_MILESTONES = {5, 10}

RAID_SNAPSHOT_INTERVAL = 6 * 60 * 60  # 6 hours
RAID_REMINDER_INTERVAL = 6 * 60 * 60  # 6 hours


# ────────────────────────────────────────────
# Raid helpers
# ────────────────────────────────────────────

async def get_latest_raid_weekend(bot, clan_tag: str) -> Optional[Dict]:
    weekends = await get_raid_weekends(bot, clan_tag, limit=1)
    return weekends[0] if weekends else None


def _parse_coc_timestamp(raw: Any) -> Optional[datetime]:
    """Parse Clash API timestamps like 20260321T070000.000Z or ISO strings."""
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None

    for fmt in ("%Y%m%dT%H%M%S.%fZ", "%Y%m%dT%H%M%SZ"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _extract_raid_items(payload: Any) -> List[Dict[str, Any]]:
    """Normalize raid endpoint payload into a list of weekend dicts."""
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return [it for it in items if isinstance(it, dict)]
        if "members" in payload or "startTime" in payload or "state" in payload:
            return [payload]
    if isinstance(payload, list):
        return [it for it in payload if isinstance(it, dict)]
    return []


async def get_raid_weekends(bot, clan_tag: str, limit: int = 4) -> List[Dict[str, Any]]:
    """Fetch raid weekends from the documented endpoint with backward-compatible fallback."""
    lim = max(1, int(limit or 1))
    tag_q = urllib.parse.quote(clan_tag)
    paths = [
        f"/clans/{tag_q}/capitalraidseasons?limit={lim}",
        f"/clans/{tag_q}/capitalraidseason?limit={lim}",
    ]

    for path in paths:
        payload = await bot.coc_get(path)
        items = _extract_raid_items(payload)
        if items:
            items.sort(
                key=lambda r: _parse_coc_timestamp(r.get("startTime")) or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            return items[:lim]
    return []


def _raid_member_used(member: Dict[str, Any]) -> int:
    return int(
        member.get("attacksUsed", member.get("attacks", 0))
        or 0
    )


def _raid_member_limit(member: Dict[str, Any]) -> int:
    base = int(member.get("attackLimit", 0) or 0)
    bonus = int(member.get("bonusAttackLimit", 0) or 0)
    if base > 0 or bonus > 0:
        return max(1, base + bonus)
    return int(member.get("attacksLimit", 6) or 6)


def _normalize_raid_members(raid: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for m in raid.get("members", []) or []:
        if not isinstance(m, dict):
            continue
        used = _raid_member_used(m)
        limit = _raid_member_limit(m)
        row = dict(m)
        row["attacksUsed"] = used
        row["attacksLimit"] = limit
        row["attacks"] = used
        row["attackLimit"] = int(m.get("attackLimit", max(0, limit - int(m.get("bonusAttackLimit", 0) or 0))) or 0)
        row["bonusAttackLimit"] = int(m.get("bonusAttackLimit", max(0, limit - int(m.get("attackLimit", 0) or 0))) or 0)
        row["capitalResourcesLooted"] = int(m.get("capitalResourcesLooted", 0) or 0)
        out.append(row)
    return out


def _raid_total_attacks(raid: Dict[str, Any], members: List[Dict[str, Any]]) -> int:
    val = raid.get("totalAttacks")
    if val is not None:
        try:
            return int(val)
        except Exception:
            pass
    return sum(int(m.get("attacksUsed", 0) or 0) for m in members)


def is_raid_weekend_active(raid: Dict[str, Any], now: Optional[datetime] = None) -> bool:
    """Return True when a raid weekend is currently active."""
    if not isinstance(raid, dict):
        return False

    current = now or datetime.now(timezone.utc)
    state = str(raid.get("state") or "").strip().lower()
    if state in {"ended", "closed", "finished"}:
        return False
    if state in {"ongoing", "inprogress", "active", "started", "inwar"}:
        return True

    start = _parse_coc_timestamp(raid.get("startTime"))
    end = _parse_coc_timestamp(raid.get("endTime"))
    if start and end:
        return start <= current < end
    return False


async def get_active_raid_weekend(bot, clan_tag: str, lookback: int = 4) -> Optional[Dict[str, Any]]:
    weekends = await get_raid_weekends(bot, clan_tag, limit=max(1, int(lookback or 1)))
    for raid in weekends:
        if is_raid_weekend_active(raid):
            return raid
    return None


def analyze_raid_weekend(raid: Dict):
    completed, partial, not_attacked = [], [], []
    for member in _normalize_raid_members(raid):
        used = int(member.get("attacksUsed", 0) or 0)
        limit = int(member.get("attacksLimit", 6) or 6)
        if used == 0:
            not_attacked.append(member)
        elif used < limit:
            partial.append(member)
        else:
            completed.append(member)
    return completed, partial, not_attacked


def get_pending_raid_members(raid: Dict) -> List[Dict]:
    return [
        m for m in _normalize_raid_members(raid)
        if int(m.get("attacksUsed", 0) or 0) < int(m.get("attacksLimit", 6) or 6)
    ]


def _raid_urgency_band(util_rate: float, pending_count: int) -> tuple[str, str]:
    util = max(0.0, float(util_rate or 0.0))
    pending = max(0, int(pending_count or 0))
    if pending == 0 and util >= 95.0:
        return "On Track", "🟢"
    if util >= 80.0 and pending <= 5:
        return "Watch", "🟡"
    if util >= 60.0:
        return "Needs Push", "🟠"
    return "Critical", "🔴"


def _raid_action_hint(util_rate: float, pending_count: int, no_attack_count: int) -> str:
    util = max(0.0, float(util_rate or 0.0))
    pending = max(0, int(pending_count or 0))
    no_attack = max(0, int(no_attack_count or 0))

    if pending == 0:
        return "All attacks are complete. Focus on cleanup efficiency and district finish quality."
    if util >= 85.0 and no_attack == 0:
        return "Close to finish. Ping remaining partial members for final cleanup attacks now."
    if no_attack > 0:
        return "Prioritize no-attack members first, then rotate partial members to close leftover attacks."
    if util < 60.0:
        return "Low utilization risk. Run immediate reminders and assign target districts to active hitters."
    return "Maintain pressure on pending members and re-check within the next activity window."


def get_district_destruction_summary(raid: Dict) -> Dict[str, Any]:
    """Aggregate district destruction from raid attackLog entries."""
    attack_log = raid.get("attackLog") or []
    if not isinstance(attack_log, list) or not attack_log:
        return {"has_data": False, "full": 0, "partial": 0, "top_partial": []}

    # Keep max destruction seen per district across attack logs.
    district_map: Dict[str, Dict[str, Any]] = {}
    for enemy in attack_log:
        if not isinstance(enemy, dict):
            continue
        for d in enemy.get("districts", []) or []:
            if not isinstance(d, dict):
                continue
            dname = str(d.get("name") or "Unknown District")
            dkey = str(d.get("id") or dname)
            try:
                percent = float(d.get("destructionPercent", d.get("destructionPercentage", 0.0)) or 0.0)
            except Exception:
                percent = 0.0

            prev = district_map.get(dkey)
            if prev is None or percent > float(prev.get("percent", 0.0)):
                district_map[dkey] = {"name": dname, "percent": percent}

    if not district_map:
        return {"has_data": False, "full": 0, "partial": 0, "top_partial": []}

    full = 0
    partial_rows: List[Dict[str, Any]] = []
    for row in district_map.values():
        pct = float(row.get("percent", 0.0))
        if pct >= 100.0:
            full += 1
        elif pct > 0.0:
            partial_rows.append(row)

    partial_rows.sort(key=lambda x: float(x.get("percent", 0.0)), reverse=True)
    top_partial = [f"{r.get('name', 'Unknown')}: {float(r.get('percent', 0.0)):.1f}%" for r in partial_rows[:5]]
    return {
        "has_data": True,
        "full": full,
        "partial": len(partial_rows),
        "top_partial": top_partial,
    }


def save_raid_snapshot(clan_tag: str, raid: Dict):
    data = load_raid_history()
    start_dt = _parse_coc_timestamp(raid.get("startTime"))
    raid_id = start_dt.strftime("%Y-%m-%d") if start_dt else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data.setdefault(clan_tag, {})
    data[clan_tag][raid_id] = {
        "start": raid.get("startTime"),
        "end": raid.get("endTime"),
        "members": {
            m.get("tag"): {
                "name": m.get("name"),
                "attacks": int(m.get("attacksUsed", 0) or 0),
                "limit": int(m.get("attacksLimit", 6) or 6),
                "loot": int(m.get("capitalResourcesLooted", 0) or 0),
            }
            for m in _normalize_raid_members(raid)
        },
        "summary": {
            "capitalTotalLoot": int(raid.get("capitalTotalLoot", 0) or 0),
            "raidsCompleted": int(raid.get("raidsCompleted", 0) or 0),
            "totalAttacks": int(raid.get("totalAttacks", 0) or 0),
            "enemyDistrictsDestroyed": int(raid.get("enemyDistrictsDestroyed", 0) or 0),
            "offensiveReward": int(raid.get("offensiveReward", 0) or 0),
            "defensiveReward": int(raid.get("defensiveReward", 0) or 0),
            "state": str(raid.get("state") or ""),
        },
    }
    save_raid_history(data)


def _to_positive_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def extract_capital_progress(clan_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extract capital hall and district levels from a clan payload."""
    hall_level = 0
    district_levels: Dict[str, int] = {}

    capital_obj = clan_payload.get("clanCapital") if isinstance(clan_payload, dict) else None
    if isinstance(capital_obj, dict):
        hall_level = _to_positive_int(
            capital_obj.get("capitalHallLevel")
            or capital_obj.get("capitalHall")
            or capital_obj.get("hallLevel")
            or capital_obj.get("level")
        )
        districts = capital_obj.get("districts")
    else:
        districts = None

    if not isinstance(districts, list):
        districts = clan_payload.get("capitalDistricts") if isinstance(clan_payload, dict) else []
    if not isinstance(districts, list):
        districts = []

    for d in districts:
        if not isinstance(d, dict):
            continue
        name = str(d.get("name") or d.get("id") or "").strip()
        if not name:
            continue
        level = _to_positive_int(
            d.get("districtHallLevel")
            or d.get("hallLevel")
            or d.get("level")
        )
        if level > 0:
            district_levels[name] = level

    return {
        "capital_hall_level": hall_level,
        "district_levels": district_levels,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_capital_upgrade_lines(previous: Dict[str, Any], current: Dict[str, Any]) -> List[str]:
    """Return human-readable lines for capital level increases."""
    lines: List[str] = []

    prev_hall = _to_positive_int((previous or {}).get("capital_hall_level"))
    curr_hall = _to_positive_int((current or {}).get("capital_hall_level"))
    if curr_hall > prev_hall:
        lines.append(f"🏛️ Capital Hall: **{prev_hall} → {curr_hall}**")

    prev_districts = (previous or {}).get("district_levels", {})
    curr_districts = (current or {}).get("district_levels", {})
    if not isinstance(prev_districts, dict):
        prev_districts = {}
    if not isinstance(curr_districts, dict):
        curr_districts = {}

    for district_name in sorted(curr_districts.keys()):
        prev_level = _to_positive_int(prev_districts.get(district_name))
        curr_level = _to_positive_int(curr_districts.get(district_name))
        if curr_level > prev_level:
            lines.append(f"🏘️ {district_name}: **{prev_level} → {curr_level}**")

    return lines


# ────────────────────────────────────────────
# Cog
# ────────────────────────────────────────────

class RaidsCog(commands.Cog, name="Raids"):
    """Capital Raid weekend tracking and reminders."""

    def __init__(self, bot):
        self.bot = bot
        self._last_announced_raid_id: Dict[str, str] = {}

    async def _send_embed_with_optional_pin(self, channel: discord.abc.Messageable, embed: discord.Embed, *, pin: bool = False):
        msg = await channel.send(embed=embed)
        if pin:
            try:
                await msg.pin(reason="CC2 important announcement")
            except Exception:
                pass
        return msg

    @staticmethod
    def _full_raid_streak(clan_tag: str, player_tag: str) -> int:
        data = load_raid_history()
        if not isinstance(data, dict):
            return 0

        clan_data = data.get(clan_tag, {})
        if not isinstance(clan_data, dict) or not clan_data:
            return 0

        weekends = sorted(clan_data.items(), key=lambda x: x[0])
        streak = 0
        for _, weekend in reversed(weekends):
            members = weekend.get("members", {})
            row = members.get(player_tag)
            if not isinstance(row, dict):
                break
            used = int(row.get("attacks", 0) or 0)
            limit = int(row.get("limit", 6) or 6)
            if limit > 0 and used >= limit:
                streak += 1
            else:
                break
        return streak

    async def _announce_raid_streak_milestones(self, clan: Dict[str, str], members: List[Dict[str, Any]]):
        milestones: List[str] = []
        for m in members:
            if not isinstance(m, dict):
                continue
            tag = m.get("tag")
            if not tag:
                continue
            streak = self._full_raid_streak(clan["tag"], tag)
            if streak in _RAID_STREAK_MILESTONES:
                milestones.append(f"• **{m.get('name', 'Unknown')}** `{tag}` — {streak} weekends")

        if not milestones:
            return

        emb = discord.Embed(
            title=f"🔥 Raid Completion Streaks — {clan['name']}",
            description="Members hitting full-completion milestones:",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Milestones", value="\n".join(milestones[:25]), inline=False)
        emb.set_footer(text="CC2 Clash Bot • Raid Streaks")

        channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
        for channel in channels:
            await channel.send(embed=emb)

    async def _track_capital_progress(self, clan: Dict[str, str]):
        """Persist latest capital levels and announce upgrades when levels increase."""
        clan_payload = await self.bot.coc_get(f"/clans/{urllib.parse.quote(clan['tag'])}")
        if not isinstance(clan_payload, dict):
            return

        current = extract_capital_progress(clan_payload)
        has_data = bool(current.get("capital_hall_level") or current.get("district_levels"))
        if not has_data:
            return

        data = load_capital_progress_data()
        previous = data.get(clan["tag"], {}) if isinstance(data, dict) else {}
        if not isinstance(data, dict):
            data = {}

        upgrade_lines = build_capital_upgrade_lines(previous, current) if previous else []
        data[clan["tag"]] = current
        if not save_capital_progress_data(data):
            logger.warning("Failed to persist capital progress for %s", clan.get("name", clan.get("tag", "unknown")))

        if not upgrade_lines:
            return

        emb = discord.Embed(
            title=f"🏗️ Clan Capital Upgrade Complete — {clan['name']}",
            description="New capital levels detected:",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Upgrades", value="\n".join(upgrade_lines[:20]), inline=False)
        emb.set_footer(text="CC2 Clash Bot • Capital Progress")

        channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
        for channel in channels:
            await channel.send(embed=emb)

    async def cog_load(self):
        self.raid_snapshot_loop.start()
        self.raid_reminder_loop.start()

    async def cog_unload(self):
        self.raid_snapshot_loop.cancel()
        self.raid_reminder_loop.cancel()

    # ── background: snapshot every 6 h ──
    @tasks.loop(seconds=RAID_SNAPSHOT_INTERVAL)
    async def raid_snapshot_loop(self):
        for clan in self.bot.get_all_monitored_clans():
            try:
                await self._track_capital_progress(clan)
                raid = await get_latest_raid_weekend(self.bot, clan["tag"])
                if raid:
                    save_raid_snapshot(clan["tag"], raid)
                    active_raid = await get_active_raid_weekend(self.bot, clan["tag"], lookback=4)
                    active_dt = _parse_coc_timestamp((active_raid or {}).get("startTime")) if active_raid else None
                    raid_id = active_dt.strftime("%Y-%m-%d") if active_dt else ""
                    if active_raid and raid_id and self._last_announced_raid_id.get(clan["tag"]) != raid_id:
                        channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
                        members = active_raid.get("members", []) or []
                        ranked = sorted(
                            [m for m in members if isinstance(m, dict)],
                            key=lambda m: int(m.get("capitalResourcesLooted", 0) or 0),
                            reverse=True,
                        )[:10]
                        if ranked:
                            emb = discord.Embed(
                                title=f"🏅 Raid Loot Leaderboard — {clan['name']}",
                                color=discord.Color.gold(),
                                timestamp=datetime.now(timezone.utc),
                            )
                            lines = []
                            for i, m in enumerate(ranked, 1):
                                loot = int(m.get("capitalResourcesLooted", 0) or 0)
                                attacks_used = int(m.get("attacksUsed", 0) or 0)
                                attacks_limit = int(m.get("attacksLimit", 6) or 6)
                                lines.append(
                                    f"{i}. **{m.get('name', 'Unknown')}** `{m.get('tag', '')}` — "
                                    f"💰 {loot:,} • ⚔️ {attacks_used}/{attacks_limit}"
                                )
                            emb.description = "\n".join(lines)
                            emb.set_footer(text="CC2 Clash Bot • Raid Leaderboard")
                            for channel in channels:
                                await self._send_embed_with_optional_pin(channel, emb, pin=True)
                            await self._announce_raid_streak_milestones(clan, members)
                            self._last_announced_raid_id[clan["tag"]] = raid_id
                    logger.debug(f"Raid snapshot saved for {clan['name']}")
            except Exception as e:
                logger.error(f"Raid snapshot error for {clan['name']}: {e}")

    @raid_snapshot_loop.before_loop
    async def before_snapshot(self):
        await self.bot.wait_until_ready()

    # ── background: reminder every 6 h ──
    @tasks.loop(seconds=RAID_REMINDER_INTERVAL)
    async def raid_reminder_loop(self):
        enabled = bool(self.bot.resolve_effective_setting("raid_reminder_enabled", getattr(self.bot, "raid_reminder_enabled", True)))
        if not enabled:
            return

        dm_enabled = bool(self.bot.resolve_effective_setting("raid_dm_reminder_enabled", False))

        for clan in self.bot.get_all_monitored_clans():
            try:
                raid = await get_active_raid_weekend(self.bot, clan["tag"], lookback=4)
                if not raid:
                    continue
                pending = get_pending_raid_members(raid)
                if not pending:
                    continue
                channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
                if not channels:
                    continue
                lines = [
                    f"🚨 **RAID WEEKEND REMINDER — {clan['name']}**",
                    "⏰ The following members still have raid attacks left:\n",
                ]
                dm_sent = 0
                dm_failed = 0
                for m in pending:
                    left = m.get("attacksLimit", 6) - m.get("attacksUsed", 0)
                    lines.append(f"• **{m.get('name')}** `{m.get('tag')}` — {left} attacks")

                    if dm_enabled:
                        player_tag = str(m.get("tag") or "").upper()
                        discord_id = get_linked_user_for_tag(player_tag)
                        if discord_id:
                            try:
                                user = await self.bot.fetch_user(int(discord_id))
                                await user.send(
                                    f"⚠️ **RAID REMINDER**\n"
                                    f"You still have **{left}** raid attacks left for **{clan['name']}**.\n"
                                    f"Please finish before Raid Weekend ends."
                                )
                                dm_sent += 1
                                await asyncio.sleep(0.25)
                            except Exception:
                                dm_failed += 1
                lines.append("\n💪 **Complete your attacks before the weekend ends!**")
                if dm_enabled:
                    lines.append(f"\n📨 DM sent: **{dm_sent}** | ❌ Failed: **{dm_failed}**")
                for channel in channels:
                    await safe_send(channel, "\n".join(lines))
                logger.info(f"Raid reminder sent for {clan['name']} ({len(pending)} pending)")
            except Exception as e:
                logger.error(f"Raid reminder error for {clan['name']}: {e}")

    @raid_reminder_loop.before_loop
    async def before_reminder(self):
        await self.bot.wait_until_ready()

    # ═══════════════════════════════════
    # /raidstatus  +  cc2 raidstatus
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="raidstatus", aliases=["rs"],
        description="Show raid weekend completion status (completed / partial / none)",
    )
    @app_commands.describe(clan="(Optional) clan to check; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def raidstatus(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for raid status",
                    include_all=True,
                )
                await ctx.send("Select a clan for raid status:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        from cogs.admin import resolve_clans
        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found in monitored list.")

        embeds = []
        for c in clans_to_check:
            raid = await get_active_raid_weekend(self.bot, c["tag"], lookback=4)
            raid_source = "active"
            if not raid:
                raid = await get_latest_raid_weekend(self.bot, c["tag"])
                raid_source = "latest"
            if not raid:
                continue
            completed, partial, not_attacked = analyze_raid_weekend(raid)
            members = _normalize_raid_members(raid)
            total_members = len(members)
            total_used = _raid_total_attacks(raid, members)
            total_limit = sum(int(m.get("attacksLimit", 6) or 6) for m in members)
            total_loot = int(raid.get("capitalTotalLoot", 0) or 0)
            if total_loot <= 0:
                total_loot = sum(int(m.get("capitalResourcesLooted", 0) or 0) for m in members)
            completion_rate = (len(completed) / total_members * 100.0) if total_members > 0 else 0.0
            util_rate = (total_used / total_limit * 100.0) if total_limit > 0 else 0.0
            pending_count = len(partial) + len(not_attacked)
            urgency_label, urgency_icon = _raid_urgency_band(util_rate, pending_count)
            action_hint = _raid_action_hint(util_rate, pending_count, len(not_attacked))

            embed = discord.Embed(
                title=f"🏰 Raid Weekend Status — {c['name']}",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            if raid_source == "latest":
                embed.description = "No active weekend right now. Showing latest saved weekend data."
            embed.add_field(
                name="📊 Summary",
                value=(
                    f"👥 Members: **{total_members}**\n"
                    f"✅ Full Completion: **{completion_rate:.1f}%**\n"
                    f"⚔️ Attack Utilization: **{util_rate:.1f}%**\n"
                    f"💰 Total Loot: **{total_loot:,}**\n"
                    f"🏚️ Districts Destroyed: **{int(raid.get('enemyDistrictsDestroyed', 0) or 0)}**\n"
                    f"🎖️ Raid Medals: **{int(raid.get('offensiveReward', 0) or 0)} atk + {int(raid.get('defensiveReward', 0) or 0)} def**\n"
                    f"{urgency_icon} Urgency: **{urgency_label}**"
                ),
                inline=False,
            )
            embed.add_field(name="🎯 Suggested Next Step", value=action_hint, inline=False)

            completed_sorted = sorted(completed, key=lambda m: int(m.get("capitalResourcesLooted", 0) or 0), reverse=True)
            partial_sorted = sorted(partial, key=lambda m: int((m.get("attacksLimit", 6) or 6) - (m.get("attacksUsed", 0) or 0)), reverse=True)
            none_sorted = sorted(not_attacked, key=lambda m: str(m.get("name", "")).lower())

            embed.add_field(
                name=f"✅ Completed Attacks ({len(completed)})",
                value=truncate("\n".join(
                    f"{m.get('name')} ({m.get('attacksUsed', 0)}/{m.get('attacksLimit', 6)}) • 💰 {int(m.get('capitalResourcesLooted', 0) or 0):,}"
                    for m in completed_sorted
                ) or "None"),
                inline=False,
            )
            embed.add_field(
                name=f"⚠️  Partial Attacks ({len(partial)})",
                value=truncate("\n".join(
                    f"{m.get('name')} ({m.get('attacksUsed', 0)}/{m.get('attacksLimit', 6)}) • left {int((m.get('attacksLimit', 6) or 6) - (m.get('attacksUsed', 0) or 0))}"
                    for m in partial_sorted
                ) or "None"),
                inline=False,
            )
            embed.add_field(
                name=f"❌ No Attacks ({len(not_attacked)})",
                value=truncate("\n".join(m.get("name") for m in none_sorted) or "None"),
                inline=False,
            )
            district = get_district_destruction_summary(raid)
            if district.get("has_data"):
                top_partial = district.get("top_partial") or []
                summary_value = (
                    f"✅ Full Districts: **{district.get('full', 0)}**\n"
                    f"⚠️ Partial Districts: **{district.get('partial', 0)}**"
                )
                if top_partial:
                    summary_value += "\n\nTop partial:\n" + "\n".join(f"• {x}" for x in top_partial)
                embed.add_field(name="🏘️ District Destruction", value=truncate(summary_value), inline=False)
            else:
                embed.add_field(name="🏘️ District Destruction", value="No district log data available.", inline=False)
            embed.set_footer(text="CC2 Clash Bot • Raid Status")
            embeds.append(embed)

        if embeds:
            for emb in embeds:
                await ctx.send(embed=emb)
        else:
            await ctx.send("No raid weekend data available.")

    # ═══════════════════════════════════
    # /raidhistory  +  cc2 raidhistory
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="raidreport", aliases=["rrpt"],
        description="Show full post-raid weekend summary",
    )
    @app_commands.describe(clan="(Optional) clan to check; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def raidreport(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for raid report",
                    include_all=True,
                )
                await ctx.send("Select a clan for raid report:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        from cogs.admin import resolve_clans
        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        sent_any = False
        for c in clans_to_check:
            raid = await get_active_raid_weekend(self.bot, c["tag"], lookback=4)
            raid_source = "active"
            if not raid:
                raid = await get_latest_raid_weekend(self.bot, c["tag"])
                raid_source = "latest"
            if not raid:
                continue

            members = _normalize_raid_members(raid)
            if not members:
                continue

            completed, partial, not_attacked = analyze_raid_weekend(raid)
            total_members = len(members)
            total_loot = int(raid.get("capitalTotalLoot", 0) or 0)
            if total_loot <= 0:
                total_loot = sum(int(m.get("capitalResourcesLooted", 0) or 0) for m in members)
            total_used = _raid_total_attacks(raid, members)
            total_limit = sum(int(m.get("attacksLimit", 6) or 6) for m in members)
            avg_attacks = (total_used / total_members) if total_members > 0 else 0.0
            util_rate = (total_used / total_limit * 100.0) if total_limit > 0 else 0.0

            mvp = max(members, key=lambda m: int(m.get("capitalResourcesLooted", 0) or 0)) if members else None
            mvp_name = str((mvp or {}).get("name") or "N/A")
            mvp_tag = str((mvp or {}).get("tag") or "")
            mvp_loot = int((mvp or {}).get("capitalResourcesLooted", 0) or 0)
            mvp_attacks = int((mvp or {}).get("attacksUsed", 0) or 0)
            mvp_limit = int((mvp or {}).get("attacksLimit", 6) or 6)

            district = get_district_destruction_summary(raid)
            top_partial = district.get("top_partial") or []

            embed = discord.Embed(
                title=f"📊 Raid Report — {c['name']}",
                color=discord.Color.gold(),
                timestamp=datetime.now(timezone.utc),
            )
            if raid_source == "latest":
                embed.description = "No active weekend right now. Showing latest saved weekend data."
            embed.add_field(
                name="Weekend Summary",
                value=(
                    f"👥 Members: **{total_members}**\n"
                    f"💰 Total Loot: **{total_loot:,}**\n"
                    f"⚔️ Avg Attacks / Member: **{avg_attacks:.2f}**\n"
                    f"📈 Attack Utilization: **{util_rate:.1f}%**\n"
                    f"🏚️ Districts Destroyed: **{int(raid.get('enemyDistrictsDestroyed', 0) or 0)}**\n"
                    f"🎖️ Raid Medals: **{int(raid.get('offensiveReward', 0) or 0)} atk + {int(raid.get('defensiveReward', 0) or 0)} def**"
                ),
                inline=False,
            )
            embed.add_field(
                name="Participation",
                value=(
                    f"✅ Full Completion: **{len(completed)}**\n"
                    f"⚠️ Partial: **{len(partial)}**\n"
                    f"❌ No-Shows: **{len(not_attacked)}**"
                ),
                inline=True,
            )
            embed.add_field(
                name="MVP",
                value=(
                    f"**{mvp_name}** `{mvp_tag}`\n"
                    f"💰 {mvp_loot:,} loot\n"
                    f"⚔️ {mvp_attacks}/{mvp_limit} attacks"
                ),
                inline=True,
            )

            if not_attacked:
                no_show_lines = [f"• {m.get('name', 'Unknown')} `{m.get('tag', '')}`" for m in not_attacked[:20]]
                embed.add_field(name="No-Shows", value=truncate("\n".join(no_show_lines)), inline=False)

            if district.get("has_data"):
                dist_value = (
                    f"✅ Full Districts: **{district.get('full', 0)}**\n"
                    f"⚠️ Partial Districts: **{district.get('partial', 0)}**"
                )
                if top_partial:
                    dist_value += "\n\nTop partial:\n" + "\n".join(f"• {row}" for row in top_partial[:5])
                embed.add_field(name="District Summary", value=truncate(dist_value), inline=False)

            embed.set_footer(text="CC2 Clash Bot • Raid Report")
            await ctx.send(embed=embed)
            sent_any = True

        if not sent_any:
            await ctx.send("No raid weekend data available for the selected clan(s).")

    @commands.hybrid_command(
        name="raidhistory", aliases=["rh"],
        description="Show stored raid weekend history",
    )
    @app_commands.describe(
        clan="Clan to check",
        limit="Number of past raids to show (default 3, max 12)",
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def raidhistory(self, ctx: commands.Context, clan: Optional[str] = None, limit: int = 3):
        await ctx.defer()
        limit = max(1, min(limit, 12))

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for raid history",
                    include_all=True,
                )
                await ctx.send("Select a clan for raid history:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        data = load_raid_history()
        if not data:
            return await ctx.send("No raid history stored yet.")

        from cogs.admin import resolve_clans
        if clan and clan != "ALL":
            clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
            if clans_to_check is None:
                return await ctx.send("❌ Clan not found.")
            all_data = {c["tag"]: data.get(c["tag"], {}) for c in clans_to_check}
        else:
            all_data = {ct: rd for ct, rd in data.items()}

        for clan_tag, raid_dict in all_data.items():
            if not raid_dict:
                continue
            clan_name = next((c["name"] for c in self.bot.get_all_monitored_clans() if c["tag"] == clan_tag), clan_tag)
            recent = list(raid_dict.items())[-limit:]
            embed = discord.Embed(
                title=f"📜 Raid History — {clan_name}",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc),
            )
            for raid_id, rd in recent:
                total_loot = sum(m.get("loot", 0) for m in rd.get("members", {}).values())
                member_count = len(rd.get("members", {}))
                embed.add_field(
                    name=raid_id,
                    value=f"👥 Members: {member_count} | 💰 Total Loot: {total_loot:,}",
                    inline=False,
                )
            embed.set_footer(text="CC2 Clash Bot • Raid History")
            await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /raidtrends
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="raidtrends", aliases=["rt"],
        description="Show raid completion and loot trends over recent weekends",
    )
    @app_commands.describe(
        clan="Clan to check (optional, default = all monitored clans)",
        weekends="Number of weekends to analyze (default 4, max 12)",
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def raidtrends(self, ctx: commands.Context, clan: Optional[str] = None, weekends: int = 4):
        await ctx.defer()
        weekends = max(1, min(weekends, 12))

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for raid trends",
                    include_all=True,
                )
                await ctx.send("Select a clan for raid trends:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        data = load_raid_history()
        if not data:
            return await ctx.send("No raid history stored yet.")
        from cogs.admin import resolve_clans
        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        embed = discord.Embed(
            title="📈 Raid Trends",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )

        has_any = False
        for c in clans_to_check:
            raid_dict = data.get(c["tag"], {})
            if not raid_dict:
                continue

            recent = list(raid_dict.items())[-weekends:]
            if not recent:
                continue

            total_loot = 0
            total_used = 0
            total_limit = 0
            full_completion_members = 0
            total_member_entries = 0

            for _, rd in recent:
                members = rd.get("members", {})
                for _, m in members.items():
                    used = int(m.get("attacks", 0) or 0)
                    limit = int(m.get("limit", 6) or 6)
                    loot = int(m.get("loot", 0) or 0)
                    total_loot += loot
                    total_used += used
                    total_limit += limit
                    total_member_entries += 1
                    if used >= limit and limit > 0:
                        full_completion_members += 1

            if total_member_entries == 0 or total_limit == 0:
                continue

            has_any = True
            avg_loot = total_loot / len(recent)
            avg_attack_util = (total_used / total_limit) * 100.0
            completion_rate = (full_completion_members / total_member_entries) * 100.0

            embed.add_field(
                name=f"{c['name']} ({len(recent)} weekends)",
                value=(
                    f"💰 Avg Loot/Weekend: **{avg_loot:,.0f}**\n"
                    f"⚔️ Avg Attack Utilization: **{avg_attack_util:.1f}%**\n"
                    f"✅ Full Completion Rate: **{completion_rate:.1f}%**"
                ),
                inline=False,
            )

        if not has_any:
            return await ctx.send("No raid trend data available.")

        embed.set_footer(text="CC2 Clash Bot • Raid Trends")
        await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /raidsleft  +  cc2 raidsleft
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="raidsleft", aliases=["rl"],
        description="Show players who did NOT finish capital raid attacks",
    )
    @app_commands.describe(clan="(Optional) clan to check; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def raidsleft(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for raids left",
                    include_all=True,
                )
                await ctx.send("Select a clan for raids left:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        from cogs.admin import resolve_clans
        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        out: List[str] = []
        for c in clans_to_check:
            raid = await get_active_raid_weekend(self.bot, c["tag"], lookback=4)
            raid_source = "active"
            if not raid:
                raid = await get_latest_raid_weekend(self.bot, c["tag"])
                raid_source = "latest"
            if not raid:
                continue
            pending = get_pending_raid_members(raid)
            if pending:
                members = _normalize_raid_members(raid)
                total_used = _raid_total_attacks(raid, members)
                total_limit = sum(int(m.get("attacksLimit", 6) or 6) for m in members)
                util_rate = (total_used / total_limit * 100.0) if total_limit > 0 else 0.0

                pending_sorted = sorted(
                    pending,
                    key=lambda p: int((p.get("attacksLimit", 6) or 6) - (p.get("attacksUsed", 0) or 0)),
                    reverse=True,
                )
                urgency_label, urgency_icon = _raid_urgency_band(util_rate, len(pending_sorted))
                action_hint = _raid_action_hint(util_rate, len(pending_sorted), len([p for p in pending_sorted if int(p.get("attacksUsed", 0) or 0) == 0]))

                prefix = "[ACTIVE]" if raid_source == "active" else "[LATEST]"
                out.append(f"**{prefix} {c['name']} — Missing Attacks ({len(pending_sorted)}) • Utilization {util_rate:.1f}%**")
                out.append(f"• {urgency_icon} Urgency: **{urgency_label}**")
                out.append(f"• Suggested action: {action_hint}")
                for p in pending_sorted:
                    left = int((p.get("attacksLimit", 6) or 6) - (p.get("attacksUsed", 0) or 0))
                    out.append(
                        f"• {p.get('name')} `{p.get('tag')}` — "
                        f"{p.get('attacksUsed', 0)}/{p.get('attacksLimit', 6)} (left {left})"
                    )

        if not out:
            await ctx.send("Everyone completed raid attacks!")
        else:
            pages = build_paginated_embeds(
                title="🏰 Raid Attacks Left",
                lines=out,
                color=discord.Color.orange(),
                per_page=16,
                footer_prefix="CC2 Clash Bot • Raids Left",
            )
            await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /raidreminder  +  cc2 raidreminder
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="raidreminder", aliases=["rr"],
        description="Enable or disable automatic raid reminders",
    )
    @app_commands.describe(mode="on or off")
    @app_commands.choices(
        mode=[
            app_commands.Choice(name="Enable", value="on"),
            app_commands.Choice(name="Disable", value="off"),
        ]
    )
    async def raidreminder(self, ctx: commands.Context, mode: str = "on"):
        # Normalise for text usage (cc2 raidreminder on)
        val = mode.lower() if isinstance(mode, str) else mode.value.lower()
        is_guild_scope = ctx.guild is not None
        if val == "on":
            self.bot.raid_reminder_enabled = True
            if is_guild_scope:
                save_guild_settings(ctx.guild.id, {"raid_reminder_enabled": True}, merge=True)
            else:
                settings = load_settings()
                settings["raid_reminder_enabled"] = True
                save_settings(settings)
            embed = discord.Embed(
                title="✅ Raid Reminders Enabled",
                description="Will send reminders every 6 hours during Raid Weekend",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
        else:
            self.bot.raid_reminder_enabled = False
            if is_guild_scope:
                save_guild_settings(ctx.guild.id, {"raid_reminder_enabled": False}, merge=True)
            else:
                settings = load_settings()
                settings["raid_reminder_enabled"] = False
                save_settings(settings)
            embed = discord.Embed(
                title="❌ Raid Reminders Disabled",
                description="Will not send raid reminder messages",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
        embed.set_footer(text="CC2 Clash Bot • Raid Reminder")
        await ctx.send(embed=embed, ephemeral=True)
        logger.info(f"Raid reminders {'enabled' if self.bot.raid_reminder_enabled else 'disabled'}")

    # ═══════════════════════════════════
    # /capitalstatus  +  cc2 capitalstatus
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="capitalstatus", aliases=["cps", "capital"],
        description="Show clan capital points/league and latest raid season summary",
    )
    @app_commands.describe(clan="(Optional) clan to check; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def capitalstatus(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for capital status",
                    include_all=True,
                )
                await ctx.send("Select a clan for capital status:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        from cogs.admin import resolve_clans
        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        sent_any = False
        for c in clans_to_check:
            clan_payload = await self.bot.coc_get(f"/clans/{urllib.parse.quote(c['tag'])}")
            if not isinstance(clan_payload, dict):
                continue

            weekends = await get_raid_weekends(self.bot, c["tag"], limit=1)
            latest = weekends[0] if weekends else {}
            members = _normalize_raid_members(latest) if isinstance(latest, dict) else []
            total_used = _raid_total_attacks(latest if isinstance(latest, dict) else {}, members)
            total_limit = sum(int(m.get("attacksLimit", 6) or 6) for m in members)
            util_rate = (total_used / total_limit * 100.0) if total_limit > 0 else 0.0

            capital_points = int(clan_payload.get("clanCapitalPoints", 0) or 0)
            capital_league = ((clan_payload.get("capitalLeague") or {}).get("name") or "Unknown")
            weekend_loot = int((latest or {}).get("capitalTotalLoot", 0) or 0)
            weekend_attacks = int((latest or {}).get("totalAttacks", 0) or 0)
            weekend_destroyed = int((latest or {}).get("enemyDistrictsDestroyed", 0) or 0)

            emb = discord.Embed(
                title=f"🏛️ Capital Status — {c['name']}",
                color=discord.Color.teal(),
                timestamp=datetime.now(timezone.utc),
            )
            emb.add_field(
                name="Capital Profile",
                value=(
                    f"🪙 Capital Points: **{capital_points:,}**\n"
                    f"🏆 Capital League: **{capital_league}**"
                ),
                inline=False,
            )
            emb.add_field(
                name="Latest Raid Season",
                value=(
                    f"💰 Loot: **{weekend_loot:,}**\n"
                    f"⚔️ Total Attacks: **{weekend_attacks}**\n"
                    f"🏚️ Districts Destroyed: **{weekend_destroyed}**\n"
                    f"📈 Utilization: **{util_rate:.1f}%**"
                ),
                inline=False,
            )
            emb.set_footer(text="CC2 Clash Bot • Capital Status")
            await ctx.send(embed=emb)
            sent_any = True

        if not sent_any:
            await ctx.send("No capital status data available.")

    # ═══════════════════════════════════
    # /capitalrank  +  cc2 capitalrank
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="capitalrank", aliases=["cprank"],
        description="Show capital leaderboard rank for selected clan(s) by location",
    )
    @app_commands.describe(
        clan="(Optional) clan to check; default = all",
        location_id="Location id (32000000 = Global)",
        limit="How many leaderboard rows to fetch (max 200)",
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def capitalrank(self, ctx: commands.Context, clan: Optional[str] = None, location_id: int = 32000000, limit: int = 200):
        await ctx.defer()
        lim = max(10, min(int(limit or 200), 200))
        location = int(location_id or 32000000)

        payload = await self.bot.coc_get(f"/locations/{location}/rankings/capitals?limit={lim}")
        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list) or not items:
            return await ctx.send("No capital ranking data available from API.")

        rank_map: Dict[str, Dict[str, Any]] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            tag = str(row.get("tag") or "").upper()
            if tag:
                rank_map[tag] = row

        from cogs.admin import resolve_clans
        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        lines: List[str] = []
        for c in clans_to_check:
            row = rank_map.get(str(c.get("tag") or "").upper())
            if not row:
                lines.append(f"• **{c['name']}** `{c['tag']}` — not in top {lim} for location {location}.")
                continue

            rank = int(row.get("rank", 0) or 0)
            prev = int(row.get("previousRank", 0) or 0)
            delta = (prev - rank) if (prev > 0 and rank > 0) else 0
            trend = "▲" if delta > 0 else ("▼" if delta < 0 else "•")
            lines.append(
                f"• **{c['name']}** `{c['tag']}` — Rank **#{rank}** {trend} ({delta:+d}) • "
                f"🪙 {int(row.get('clanCapitalPoints', 0) or 0):,} • {((row.get('capitalLeague') or {}).get('name') or 'Unknown')}"
            )

        pages = build_paginated_embeds(
            title=f"🏆 Capital Rankings (Location {location})",
            lines=lines,
            color=discord.Color.gold(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • Capital Rank",
        )
        await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /capitalleagues  +  cc2 capitalleagues
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="capitalleagues", aliases=["cpleagues"],
        description="List all capital leagues or show a specific league by id",
    )
    @app_commands.describe(league_id="Optional league id")
    async def capitalleagues(self, ctx: commands.Context, league_id: Optional[int] = None):
        await ctx.defer()

        if league_id is None:
            payload = await self.bot.coc_get("/capitalleagues")
            items = payload.get("items", []) if isinstance(payload, dict) else []
            if not isinstance(items, list) or not items:
                return await ctx.send("No capital leagues data available from API.")

            lines = [f"• **{int(it.get('id', 0) or 0)}** — {it.get('name', 'Unknown')}" for it in items if isinstance(it, dict)]
            pages = build_paginated_embeds(
                title="🏅 Capital Leagues",
                lines=lines,
                color=discord.Color.blurple(),
                per_page=20,
                footer_prefix="CC2 Clash Bot • Capital Leagues",
            )
            return await send_paginated_embeds(ctx, pages)

        payload = await self.bot.coc_get(f"/capitalleagues/{int(league_id)}")
        if not isinstance(payload, dict) or not payload:
            return await ctx.send("League not found.")

        emb = discord.Embed(
            title="🏅 Capital League Detail",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="League ID", value=str(int(payload.get("id", 0) or 0)), inline=True)
        emb.add_field(name="League Name", value=str(payload.get("name") or "Unknown"), inline=True)
        emb.set_footer(text="CC2 Clash Bot • Capital Leagues")
        await ctx.send(embed=emb)


async def setup(bot):
    await bot.add_cog(RaidsCog(bot))
