"""Achievement badges and milestone tracking commands."""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks

from donations import extract_lifetime_donations, calculate_monthly_donations, get_current_month_key
from calculations import calculate_weighted_rush_score, calculate_activity_score
from clash_rush import HERO_CAPS, LAB_CAPS
from config import LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID
from storage import (
    get_linked_tag_for_user,
    get_linked_user_for_tag,
    load_achievements_data,
    save_achievements_data,
    load_raid_history,
    load_war_player_stats,
)
from utils.helpers import normalize_tag, is_valid_tag, has_leadership_role

logger = logging.getLogger("cc2bot.cogs.achievements")
_CUSTOM_KEY = "__custom__"


def _load_achievements() -> Dict[str, Any]:
    data = load_achievements_data()
    return data if isinstance(data, dict) else {}


def _save_achievements(data: Dict[str, Any]) -> None:
    save_achievements_data(data)


def _get_raid_streak(clan_tag: str, player_tag: str) -> int:
    """Count trailing consecutive raid weekends with full attack usage."""
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
        m = members.get(player_tag)
        if not isinstance(m, dict):
            break
        used = int(m.get("attacks", 0) or 0)
        limit = int(m.get("limit", 6) or 6)
        if limit > 0 and used >= limit:
            streak += 1
        else:
            break
    return streak


def _get_war_participation(clan_tag: str, player_tag: str) -> int:
    data = load_war_player_stats()
    if not isinstance(data, dict):
        return 0
    row = ((data.get(clan_tag, {}) if isinstance(data.get(clan_tag, {}), dict) else {}).get(player_tag, {}))
    return int(row.get("wars_participated", 0) or 0) if isinstance(row, dict) else 0


def _get_war_stats_row(clan_tag: str, player_tag: str) -> Dict[str, Any]:
    data = load_war_player_stats()
    if not isinstance(data, dict):
        return {}
    clan_rows = data.get(clan_tag, {}) if isinstance(data.get(clan_tag, {}), dict) else {}
    row = clan_rows.get(player_tag, {}) if isinstance(clan_rows, dict) else {}
    return row if isinstance(row, dict) else {}


def _tier_label(index: int, total: int) -> str:
    # Supports variable-length tier lists while preserving requested naming style.
    labels = ["Bronze", "Silver", "Gold", "Heroic", "Legendary", "Mythic"]
    if total <= len(labels):
        return labels[index]
    return f"Tier {index + 1}"


def _tiered_badges(base_name: str, value: int, milestones: List[int]) -> List[str]:
    out: List[str] = []
    for i, threshold in enumerate(milestones):
        if value >= threshold:
            out.append(f"{base_name} ({_tier_label(i, len(milestones))})")
    return out


def _count_true_streak_by_month(month_flags: Dict[str, Any]) -> int:
    if not isinstance(month_flags, dict):
        return 0
    streak = 0
    for month in sorted(month_flags.keys()):
        if bool(month_flags.get(month)):
            streak += 1
        else:
            streak = 0
    return streak


def _count_trailing_true_day_streak(day_flags: Dict[str, Any]) -> int:
    """Count trailing consecutive true daily flags using YYYY-MM-DD keys."""
    if not isinstance(day_flags, dict) or not day_flags:
        return 0

    items: List[tuple] = []
    for day, flag in day_flags.items():
        try:
            d = datetime.strptime(str(day), "%Y-%m-%d").date()
        except Exception:
            continue
        items.append((d, bool(flag)))

    if not items:
        return 0

    items.sort(key=lambda x: x[0])
    streak = 0
    prev_day = None
    for day, flag in items:
        if prev_day is not None and (day - prev_day).days > 1:
            streak = 0
        if flag:
            streak += 1
        else:
            streak = 0
        prev_day = day
    return streak


def _hero_total_and_cap_for_current_th(player: Dict[str, Any]) -> tuple:
    try:
        th = int(player.get("townHallLevel", 0) or 0)
    except Exception:
        return 0, 0

    caps = HERO_CAPS.get(th, {})
    if not isinstance(caps, dict) or not caps:
        return 0, 0

    hero_map = {"BK": 0, "AQ": 0, "GW": 0, "RC": 0, "MP": 0}
    for h in player.get("heroes", []) or []:
        name = str(h.get("name", "")).lower()
        try:
            lvl = int(h.get("level", 0) or 0)
        except Exception:
            lvl = 0
        if "barbarian king" in name:
            hero_map["BK"] = lvl
        elif "archer queen" in name:
            hero_map["AQ"] = lvl
        elif "grand warden" in name:
            hero_map["GW"] = lvl
        elif "royal champion" in name:
            hero_map["RC"] = lvl
        elif "minion prince" in name:
            hero_map["MP"] = lvl

    required = int(sum(int(v or 0) for v in caps.values()) or 0)
    current = 0
    for key in ("BK", "AQ", "GW", "RC", "MP"):
        cap = int(caps.get(key, 0) or 0)
        current += min(cap, int(hero_map.get(key, 0) or 0))
    return current, required


def _lab_total_and_cap_for_current_th(player: Dict[str, Any]) -> tuple:
    try:
        th = int(player.get("townHallLevel", 0) or 0)
    except Exception:
        return 0, 0

    required = int(LAB_CAPS.get(th, 0) or 0)
    if required <= 0:
        return 0, 0

    total = 0
    for key in ("troops", "spells"):
        for row in player.get(key, []) or []:
            try:
                total += int(row.get("level", 0) or 0)
            except Exception:
                continue
    return min(total, required), required


def _get_monthly_top_donors(clan_tag: str) -> set:
    """Return top donor tag(s) for the most recent complete comparable month."""
    monthly = calculate_monthly_donations(clan_tag)
    if not isinstance(monthly, dict):
        return set()
    members = monthly.get("members", {})
    if not isinstance(members, dict) or not members:
        return set()

    top_value = None
    top_tags = set()
    for tag, row in members.items():
        if not isinstance(row, dict):
            continue
        val = int(row.get("monthly", 0) or 0)
        if top_value is None or val > top_value:
            top_value = val
            top_tags = {tag}
        elif top_value is not None and val == top_value:
            top_tags.add(tag)
    return top_tags


def _next_milestone(current: int, milestones: List[int]) -> int:
    for m in milestones:
        if current < m:
            return m
    return milestones[-1]


def _filled_bar_from_ratio(ratio: float, width: int = 12) -> str:
    ratio = max(0.0, min(1.0, float(ratio)))
    filled = int(round(ratio * width))
    return ("█" * filled) + ("░" * (width - filled))


def _milestone_state(current: int, milestones: List[int], width: int = 12) -> Dict[str, Any]:
    valid = sorted({int(m) for m in milestones if int(m) > 0})
    if not valid:
        return {
            "next": None,
            "remaining": 0,
            "reached": 0,
            "total": 0,
            "pct": 100.0,
            "bar": _filled_bar_from_ratio(1.0, width=width),
            "top": 0,
        }

    current = max(0, int(current))
    reached = sum(1 for m in valid if current >= m)
    total = len(valid)
    top = valid[-1]

    if current >= top:
        return {
            "next": None,
            "remaining": 0,
            "reached": total,
            "total": total,
            "pct": 100.0,
            "bar": _filled_bar_from_ratio(1.0, width=width),
            "top": top,
        }

    next_target = _next_milestone(current, valid)
    previous_target = valid[reached - 1] if reached > 0 else 0
    segment_span = max(1, next_target - previous_target)
    segment_progress = max(0, current - previous_target)
    ratio = max(0.0, min(float(segment_progress) / float(segment_span), 1.0))

    return {
        "next": next_target,
        "remaining": max(0, next_target - current),
        "reached": reached,
        "total": total,
        "pct": round(ratio * 100.0, 1),
        "bar": _filled_bar_from_ratio(ratio, width=width),
        "top": top,
    }


def _format_milestone_progress(current: int, milestones: List[int], width: int = 12) -> str:
    current = max(0, int(current))
    state = _milestone_state(current, milestones, width=width)

    lines = [
        f"Current: **{current:,}**",
        f"Progress: {state['bar']}  {state['pct']:.1f}%",
    ]
    if state["next"] is None:
        lines.append(f"Next: **Top milestone reached ({int(state['top']):,})**")
    else:
        lines.append(f"Next: **{int(state['next']):,}** ({int(state['remaining']):,} to go)")
    lines.append(f"Milestones reached: **{int(state['reached'])}/{int(state['total'])}**")
    return "\n".join(lines)


def _get_custom_definitions(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = data.get(_CUSTOM_KEY, []) if isinstance(data, dict) else []
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not row.get("name") or not row.get("metric"):
            continue
        out.append(row)
    return out


def _evaluate_custom_badges(custom_defs: List[Dict[str, Any]], metrics: Dict[str, float]) -> List[str]:
    badges: List[str] = []
    for row in custom_defs:
        name = str(row.get("name", "")).strip()
        metric = str(row.get("metric", "")).strip()
        threshold_raw = row.get("threshold", 0)
        try:
            threshold = float(threshold_raw)
        except Exception:
            continue
        try:
            current = float(metrics.get(metric, 0.0) or 0.0)
        except Exception:
            current = 0.0

        if metric == "rush_score_max":
            passed = current <= threshold
        else:
            passed = current >= threshold

        if passed and name:
            badges.append(name)
    return badges


class AchievementsCog(commands.Cog, name="Achievements"):
    """Award badges for long-term engagement and expose progress commands."""

    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        self.achievement_scan_loop.start()

    async def cog_unload(self):
        self.achievement_scan_loop.cancel()

    async def _run_achievement_scan(self, announce_unlocks: bool = True) -> Dict[str, Any]:
        ach = _load_achievements()
        summary: Dict[str, Any] = {
            "players_scanned": 0,
            "players_with_new_badges": 0,
            "badges_awarded": 0,
            "awards": [],
        }

        for clan in self.bot.get_all_monitored_clans():
            channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
            if not channels:
                continue
            members = await self.bot.get_clan_member_list(clan["tag"])
            if not members:
                continue
            tags = [m.get("tag") for m in members if m.get("tag")]
            players = await self.bot.fetch_players(tags)
            monthly_top_donors = _get_monthly_top_donors(clan["tag"])
            month_key = get_current_month_key()

            for m in members:
                tag = m.get("tag")
                if not tag:
                    continue
                player = players.get(tag)
                if not player:
                    continue

                summary["players_scanned"] += 1

                name = player.get("name", m.get("name", "Unknown"))
                lifetime = extract_lifetime_donations(player)
                troops_donated = int(lifetime.get("troops_donated", 0) or 0)
                spells_donated = int(lifetime.get("spells_donated", 0) or 0)
                siege_donated = int(lifetime.get("siege_donated", 0) or 0)
                lifetime_total = int(lifetime.get("total_donated", 0) or 0)
                war_participated = _get_war_participation(clan["tag"], tag)
                raid_streak = _get_raid_streak(clan["tag"], tag)
                war_row = _get_war_stats_row(clan["tag"], tag)
                war_stars_total = int(war_row.get("stars_earned", 0) or 0)
                participation_streak = int(war_row.get("participation_streak", 0) or 0)
                giant_slayer_3stars = int(war_row.get("giant_slayer_3stars", 0) or 0)
                rush = calculate_weighted_rush_score(player)
                rush_score = float(rush.get("score", 999.0) if rush else 999.0)
                activity_score = float((calculate_activity_score(player) or {}).get("score", 0.0) or 0.0)
                th_level = int(player.get("townHallLevel", 0) or 0)

                hero_current, hero_required = _hero_total_and_cap_for_current_th(player)
                lab_current, lab_required = _lab_total_and_cap_for_current_th(player)

                badge_candidates: List[str] = []
                if war_participated >= 1000:
                    badge_candidates.append("War Veteran")
                if lifetime_total >= 100_000:
                    badge_candidates.append("Donation King")
                if raid_streak >= 1000:
                    badge_candidates.append("Raid Master")
                if rush_score < 5:
                    badge_candidates.append("Un-Rushed")
                if rush_score <= 1 and int(player.get("townHallLevel", 0) or 0) >= 10:
                    badge_candidates.append("TH Max")

                # Requested badge set
                if participation_streak >= 20:
                    badge_candidates.append("Never Miss")

                badge_candidates.extend(
                    _tiered_badges("Star Collector", war_stars_total, [100, 500, 1000, 2000])
                )
                badge_candidates.extend(
                    _tiered_badges("Generous", troops_donated, [1000, 5000, 10000, 50000, 100000])
                )
                badge_candidates.extend(
                    _tiered_badges("Spell Sender", spells_donated, [500, 2000, 10000])
                )
                badge_candidates.extend(
                    _tiered_badges("Siege Supplier", siege_donated, [10, 50, 200])
                )
                if giant_slayer_3stars >= 1:
                    badge_candidates.append("Giant Slayer")
                if tag in monthly_top_donors:
                    badge_candidates.append("Clan Backbone")

                # Best-effort monthly donation-ratio streak tracking from live season stats.
                row_existing = ach.get(tag, {"name": name, "badges": [], "history": []})
                meta = row_existing.get("_meta", {}) if isinstance(row_existing.get("_meta", {}), dict) else {}

                # Welcome — linked account to bot
                if get_linked_user_for_tag(tag) is not None:
                    badge_candidates.append("Welcome")

                # Family tenure badges (Regular + Veteran)
                now_dt = datetime.now(timezone.utc)
                family_first_seen = str(meta.get("family_first_seen_at", "") or "")
                if not family_first_seen:
                    family_first_seen = now_dt.isoformat()
                    meta["family_first_seen_at"] = family_first_seen
                try:
                    family_days = max(0, (now_dt - datetime.fromisoformat(family_first_seen)).days)
                except Exception:
                    family_days = 0

                if family_days >= 30:
                    badge_candidates.append("Regular (30 Days)")
                if family_days >= 90:
                    badge_candidates.append("Regular (90 Days)")
                if family_days >= 180:
                    badge_candidates.append("Regular (180 Days)")
                if family_days >= 365:
                    badge_candidates.append("Regular (365 Days)")
                    badge_candidates.append("Veteran")

                # Active Member — activity score > 80 for 30 consecutive days
                day_key = now_dt.date().isoformat()
                activity_days = meta.get("activity_over_80_days", {}) if isinstance(meta.get("activity_over_80_days", {}), dict) else {}
                activity_days[day_key] = (activity_score > 80.0)
                for old_day in sorted(activity_days.keys())[:-120]:
                    activity_days.pop(old_day, None)
                meta["activity_over_80_days"] = activity_days
                if _count_trailing_true_day_streak(activity_days) >= 30:
                    badge_candidates.append("Active Member")

                # Un-Rushed milestone: below 15 after previously being above/equal 15.
                was_above_15 = bool(meta.get("rush_was_above_15", False))
                if rush_score >= 15.0:
                    meta["rush_was_above_15"] = True
                    was_above_15 = True
                if was_above_15 and rush_score < 15.0:
                    badge_candidates.append("Un-Rushed (Below 15)")

                # Fully maxed / hero max / lab max for current TH.
                if rush_score <= 0.0:
                    badge_candidates.append("Fully Maxed")
                if hero_required > 0 and hero_current >= hero_required:
                    badge_candidates.append("Hero Grind")
                if lab_required > 0 and lab_current >= lab_required:
                    badge_candidates.append("Lab Rat")

                # TH progression and end-game target.
                highest_th_seen = int(meta.get("highest_th_seen", th_level) or th_level)
                if th_level > highest_th_seen:
                    badge_candidates.append("Town Hall Up")
                    meta["highest_th_seen"] = th_level
                else:
                    meta["highest_th_seen"] = max(highest_th_seen, th_level)
                if th_level >= 18:
                    badge_candidates.append("End Game")

                ratio_months = meta.get("donation_ratio_months", {}) if isinstance(meta.get("donation_ratio_months", {}), dict) else {}
                season_don = int(player.get("donations", 0) or 0)
                season_rcv = int(player.get("donationsReceived", 0) or 0)
                ratio = (float(season_don) / float(max(1, season_rcv))) if season_don > 0 else 0.0
                ratio_months[month_key] = (ratio >= 1.5)
                # Keep at most 12 months of markers.
                for old_month in sorted(ratio_months.keys())[:-12]:
                    ratio_months.pop(old_month, None)
                meta["donation_ratio_months"] = ratio_months
                if _count_true_streak_by_month(ratio_months) >= 3:
                    badge_candidates.append("Giving Back")

                metrics = {
                    "donations_total": float(lifetime_total),
                    "war_participated": float(war_participated),
                    "raid_full_streak": float(raid_streak),
                    "rush_score_max": float(rush_score),
                    "best_trophies": float(int(player.get("bestTrophies", player.get("trophies", 0)) or 0)),
                    "town_hall": float(int(player.get("townHallLevel", 0) or 0)),
                }
                custom_defs = _get_custom_definitions(ach)
                badge_candidates.extend(_evaluate_custom_badges(custom_defs, metrics))

                row = row_existing
                row["_meta"] = meta
                existing = set(row.get("badges", []))
                new_badges = [b for b in badge_candidates if b not in existing]

                if not new_badges:
                    row["name"] = name
                    ach[tag] = row
                    continue

                summary["players_with_new_badges"] += 1

                for badge in new_badges:
                    row.setdefault("badges", []).append(badge)
                    row.setdefault("history", []).append(
                        {
                            "badge": badge,
                            "clan": clan["name"],
                            "date": datetime.now(timezone.utc).isoformat(),
                        }
                    )

                    summary["badges_awarded"] += 1
                    summary["awards"].append(
                        {
                            "badge": badge,
                            "player": name,
                            "tag": tag,
                            "clan": clan["name"],
                        }
                    )

                    if announce_unlocks:
                        emb = discord.Embed(
                            title="🏅 Achievement Unlocked!",
                            description=(
                                f"**{name}** `{tag}` earned **{badge}** in **{clan['name']}**!"
                            ),
                            color=discord.Color.gold(),
                            timestamp=datetime.now(timezone.utc),
                        )
                        emb.set_footer(text="CC2 Clash Bot • Achievements")
                        for channel in channels:
                            await channel.send(embed=emb)

                ach[tag] = row

        _save_achievements(ach)
        return summary

    @tasks.loop(hours=12)
    async def achievement_scan_loop(self):
        await self._run_achievement_scan(announce_unlocks=True)

    @achievement_scan_loop.before_loop
    async def before_scan(self):
        await self.bot.wait_until_ready()

    @commands.hybrid_command(name="achievements", aliases=["ach"], description="Show a player's earned achievements")
    @app_commands.describe(tag="Player tag (optional, defaults to linked account)")
    async def achievements(self, ctx: commands.Context, tag: Optional[str] = None):
        await ctx.defer()
        if tag:
            ptag = normalize_tag(tag)
            if not is_valid_tag(ptag):
                return await ctx.send("❌ Invalid tag format. Use like #2PQUE2J.")
        else:
            linked = get_linked_tag_for_user(ctx.author.id)
            if not linked:
                return await ctx.send("❌ No linked account. Use /link or pass a tag.")
            ptag = normalize_tag(linked)

        ach = _load_achievements()
        row = ach.get(ptag)
        if not row:
            return await ctx.send("No achievements recorded for this player yet.")

        badges = row.get("badges", [])
        history = row.get("history", [])
        emb = discord.Embed(
            title=f"🏅 Achievements — {row.get('name', ptag)}",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Player", value=f"`{ptag}`", inline=True)
        emb.add_field(name="Badge Count", value=str(len(badges)), inline=True)
        emb.add_field(name="Badges", value=("\n".join(f"• {b}" for b in badges) if badges else "None"), inline=False)
        if history:
            last = history[-5:]
            emb.add_field(
                name="Recent Unlocks",
                value="\n".join(f"• {h.get('badge')} ({h.get('clan')})" for h in last),
                inline=False,
            )
        emb.set_footer(text="CC2 Clash Bot • Achievements")
        await ctx.send(embed=emb)

    @commands.hybrid_command(name="milestone", aliases=["ms"], description="Show progress toward major player milestones")
    @app_commands.describe(tag="Player tag (optional, defaults to linked account)")
    async def milestone(self, ctx: commands.Context, tag: Optional[str] = None):
        await ctx.defer(ephemeral=True)
        if tag:
            ptag = normalize_tag(tag)
            if not is_valid_tag(ptag):
                return await ctx.send("❌ Invalid tag format. Use like #2PQUE2J.", ephemeral=True)
        else:
            linked = get_linked_tag_for_user(ctx.author.id)
            if not linked:
                return await ctx.send("❌ No linked account. Use /link or pass a tag.", ephemeral=True)
            ptag = normalize_tag(linked)

        player = await self.bot.get_player(ptag)
        if not player:
            return await ctx.send("❌ Could not fetch player.", ephemeral=True)

        lifetime = extract_lifetime_donations(player)
        donations_total = int(lifetime.get("total_donated", 0) or 0)
        war_stars = int(player.get("warStars", 0) or 0)
        trophies = int(player.get("bestTrophies", player.get("trophies", 0)) or 0)

        donation_milestones = [1000, 5000, 10000, 25000, 50000, 100000]
        war_star_milestones = [100, 500, 1000, 2000]
        trophy_milestones = [5000, 6000]

        emb = discord.Embed(
            title=f"🎯 Milestones — {player.get('name', ptag)}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(
            name="Donations",
            value=_format_milestone_progress(donations_total, donation_milestones),
            inline=True,
        )
        emb.add_field(
            name="War Stars",
            value=_format_milestone_progress(war_stars, war_star_milestones),
            inline=True,
        )
        emb.add_field(
            name="Trophies",
            value=_format_milestone_progress(trophies, trophy_milestones),
            inline=True,
        )
        emb.set_footer(text="CC2 Clash Bot • Milestones")
        await ctx.send(embed=emb, ephemeral=True)

    @commands.hybrid_command(name="scanachievements", aliases=["scanach"], description="Run achievement scan now (leadership)")
    async def scanachievements(self, ctx: commands.Context):
        if not has_leadership_role(ctx.author, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            return await ctx.send("❌ Leadership role required for this command.", ephemeral=True)

        await ctx.defer(ephemeral=True)
        summary = await self._run_achievement_scan(announce_unlocks=True)

        players_scanned = int(summary.get("players_scanned", 0) or 0)
        players_with_new = int(summary.get("players_with_new_badges", 0) or 0)
        badges_awarded = int(summary.get("badges_awarded", 0) or 0)
        awards = summary.get("awards", []) if isinstance(summary.get("awards", []), list) else []

        emb = discord.Embed(
            title="✅ Achievement Scan Complete",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Players Scanned", value=str(players_scanned), inline=True)
        emb.add_field(name="Players With New Badges", value=str(players_with_new), inline=True)
        emb.add_field(name="Badges Awarded", value=str(badges_awarded), inline=True)

        if awards:
            preview_lines = []
            for row in awards[:20]:
                badge = str(row.get("badge", ""))
                player_name = str(row.get("player", "Unknown"))
                player_tag = str(row.get("tag", ""))
                preview_lines.append(f"• {badge} — {player_name} `{player_tag}`")
            if len(awards) > 20:
                preview_lines.append(f"… and {len(awards) - 20} more")
            emb.add_field(name="New Awards", value="\n".join(preview_lines), inline=False)
        else:
            emb.add_field(name="New Awards", value="No new badges awarded this run.", inline=False)

        emb.set_footer(text="CC2 Clash Bot • Achievements")
        await ctx.send(embed=emb, ephemeral=True)

    @commands.hybrid_command(name="addachievement", aliases=["addach"], description="Create a custom achievement definition (leadership)")
    @app_commands.describe(
        name="Achievement badge name",
        description="Short description shown in admin confirmation",
        metric="Metric to evaluate",
        threshold="Threshold value",
    )
    @app_commands.choices(
        metric=[
            app_commands.Choice(name="Total Donations (>=)", value="donations_total"),
            app_commands.Choice(name="War Participation (>=)", value="war_participated"),
            app_commands.Choice(name="Raid Full Streak (>=)", value="raid_full_streak"),
            app_commands.Choice(name="Rush Score Max (<=)", value="rush_score_max"),
            app_commands.Choice(name="Best Trophies (>=)", value="best_trophies"),
            app_commands.Choice(name="Town Hall Level (>=)", value="town_hall"),
        ]
    )
    async def addachievement(
        self,
        ctx: commands.Context,
        name: str,
        description: str,
        metric: str,
        threshold: float,
    ):
        if not has_leadership_role(ctx.author, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            return await ctx.send("❌ Leadership role required for this command.", ephemeral=True)

        await ctx.defer(ephemeral=True)
        ach = _load_achievements()
        defs = _get_custom_definitions(ach)

        target_name = name.strip()
        if not target_name:
            return await ctx.send("❌ Achievement name cannot be empty.", ephemeral=True)
        if any(str(d.get("name", "")).lower() == target_name.lower() for d in defs):
            return await ctx.send("❌ A custom achievement with that name already exists.", ephemeral=True)

        row = {
            "name": target_name,
            "description": description.strip(),
            "metric": str(metric),
            "threshold": float(threshold),
            "created_by": int(ctx.author.id),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        defs.append(row)
        ach[_CUSTOM_KEY] = defs
        _save_achievements(ach)

        op = "<=" if metric == "rush_score_max" else ">="
        emb = discord.Embed(
            title="✅ Custom Achievement Added",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Name", value=target_name, inline=False)
        emb.add_field(name="Description", value=(description.strip() or "N/A"), inline=False)
        emb.add_field(name="Rule", value=f"{metric} {op} {threshold}", inline=False)
        emb.set_footer(text="CC2 Clash Bot • Achievements")
        await ctx.send(embed=emb, ephemeral=True)


async def setup(bot):
    await bot.add_cog(AchievementsCog(bot))
