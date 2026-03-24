"""War tracking, attack alerts, and scheduled reminders."""
import logging
import asyncio
import urllib.parse
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config import WAR_POLL_INTERVAL, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID
from storage import (
    load_links,
    load_bases,
    load_war_baseline,
    save_war_baseline,
    load_settings,
    save_settings,
    save_guild_settings,
    load_war_results,
    save_war_results,
    load_war_attack_log,
    save_war_attack_log,
    load_war_player_stats,
    save_war_player_stats,
    get_linked_user_for_tag,
)
from cogs.profiles import clan_autocomplete
from utils.helpers import safe_send, build_paginated_embeds, send_paginated_embeds, ClanSelectView, has_leadership_role, build_error_embed

logger = logging.getLogger("cc2bot.cogs.war")

_STREAK_MILESTONES = {5, 10, 15, 20}
_WAR_PIN_SETTINGS_KEY = "war_pinned_messages"
_WAR_ACTIVE_REMINDER_KEY = "war_active_reminder_state"


def _determine_war_result(clan_stars: int, opp_stars: int, clan_destruction: float, opp_destruction: float) -> str:
    if clan_stars > opp_stars:
        return "win"
    if clan_stars < opp_stars:
        return "loss"
    if clan_destruction > opp_destruction:
        return "win"
    if clan_destruction < opp_destruction:
        return "loss"
    return "tie"


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _result_to_cell(result: str) -> str:
    r = str(result or "").lower()
    if r == "win":
        return "█"
    if r == "tie":
        return "▒"
    return "░"


def _build_result_sparkline(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "N/A"
    cells = [_result_to_cell(str(r.get("result", ""))) for r in rows]
    return "".join(cells)


async def _fetch_cwl_group(bot, clan_tag: str) -> Optional[Dict[str, Any]]:
    tag_q = urllib.parse.quote(str(clan_tag or "").upper().strip(), safe="")
    payload = await bot.coc_get(f"/clans/{tag_q}/currentwar/leaguegroup")
    return payload if isinstance(payload, dict) else None


def _round_tags(round_row: Dict[str, Any]) -> List[str]:
    tags = round_row.get("warTags", []) if isinstance(round_row, dict) else []
    if not isinstance(tags, list):
        return []
    out: List[str] = []
    for t in tags:
        raw = str(t or "").strip()
        if raw and raw != "#0":
            out.append(raw)
    return out


async def _fetch_cwl_wars(bot, war_tags: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for war_tag in war_tags:
        try:
            tag_q = urllib.parse.quote(str(war_tag), safe="")
            payload = await bot.coc_get(f"/clanwarleagues/wars/{tag_q}")
            if isinstance(payload, dict):
                rows.append(payload)
        except Exception:
            continue
    return rows


def _star_efficiency_label(attacker_th: Any, defender_th: Any, stars: int) -> str:
    atk = _as_int(attacker_th)
    dfn = _as_int(defender_th)
    diff = atk - dfn

    if diff >= 2:
        if stars == 3:
            return "Expected triple"
        if stars == 2:
            return "Below expectation"
        return "Far below expectation"

    if diff == 1:
        if stars == 3:
            return "Strong hit"
        if stars == 2:
            return "Slightly below expectation"
        return "Below expectation"

    if diff == 0:
        if stars == 3:
            return "Excellent hit"
        if stars == 2:
            return "Solid hit"
        return "Low value hit"

    # Attacker hitting up.
    if stars == 3:
        return "Outstanding hit-up"
    if stars == 2:
        return "Great hit-up"
    if stars == 1:
        return "Reasonable hit-up"
    return "Rough hit-up"


def _war_pending_urgency(pending_count: int, total_members: int) -> tuple[str, str]:
    pending = max(0, int(pending_count or 0))
    total = max(0, int(total_members or 0))
    if pending == 0:
        return "On Track", "🟢"

    ratio = float(pending) / float(max(1, total))
    if ratio <= 0.20:
        return "Watch", "🟡"
    if ratio <= 0.45:
        return "Needs Push", "🟠"
    return "Critical", "🔴"


def _war_pending_action_hint(pending_count: int, total_members: int) -> str:
    pending = max(0, int(pending_count or 0))
    total = max(0, int(total_members or 0))
    if pending == 0:
        return "All attacks used. Focus on cleanup value and late-war coordination."

    ratio = float(pending) / float(max(1, total))
    if ratio <= 0.20:
        return "Small pending list. Ping remaining attackers and assign final targets now."
    if ratio <= 0.45:
        return "Moderate pending load. Prioritize higher-hit-value attackers first."
    return "High pending risk. Trigger leadership escalation and direct-call missing attackers immediately."


def _warmap_pressure_band(zero_used: int, one_used: int, total_members: int) -> tuple[str, str]:
    zero = max(0, int(zero_used or 0))
    one = max(0, int(one_used or 0))
    total = max(0, int(total_members or 0))
    if total == 0:
        return "Unknown", "⚪"

    untouched_ratio = float(zero) / float(total)
    pending_ratio = float(zero + one) / float(total)

    if pending_ratio <= 0.20:
        return "Stable", "🟢"
    if untouched_ratio <= 0.20 and pending_ratio <= 0.45:
        return "Watch", "🟡"
    if pending_ratio <= 0.70:
        return "Needs Push", "🟠"
    return "Critical", "🔴"


def _warmap_action_hint(zero_used: int, one_used: int, total_members: int) -> str:
    zero = max(0, int(zero_used or 0))
    one = max(0, int(one_used or 0))
    total = max(0, int(total_members or 0))
    if total == 0:
        return "No member map data available yet."
    if zero == 0 and one == 0:
        return "All attacks used. Shift to cleanup quality and final hit-value checks."
    if zero > 0 and one > 0:
        return "Prioritize zero-hit members first, then allocate second attacks for cleanup value."
    if zero > 0:
        return "Many players have not opened yet. Trigger direct pings and assign opening targets now."
    return "Mostly second-hit cleanup remains. Assign safer cleanup hits to secure final stars."


def _warpreview_pressure_band(est_weight_band: str, avg_th: float) -> tuple[str, str]:
    band = str(est_weight_band or "").strip().lower()
    th = max(0.0, float(avg_th or 0.0))
    if band == "high" and th >= 15.0:
        return "High Pressure", "🔴"
    if band == "medium" or th >= 14.0:
        return "Balanced Pressure", "🟡"
    return "Favorable", "🟢"


def _warpreview_action_hint(pressure_label: str, top_heavy_ratio: float) -> str:
    pressure = str(pressure_label or "").strip().lower()
    ratio = max(0.0, min(1.0, float(top_heavy_ratio or 0.0)))

    if "high pressure" in pressure and ratio >= 0.40:
        return "Open with safe 2-star plans on top bases, then convert cleanup to triples on lower lanes."
    if "high pressure" in pressure:
        return "Use disciplined mirror/opening hits first and save strongest attackers for decisive targets."
    if "balanced pressure" in pressure:
        return "Run mirror-first openers, then schedule controlled dip attacks for cleanup efficiency."
    return "Favorable matchup. Push aggressive triple plans early while preserving backup cleanup options."


def _opponent_lineup_action_hint(top_heavy_ratio: float, avg_conceded_stars: float) -> tuple[str, str]:
    ratio = max(0.0, min(1.0, float(top_heavy_ratio or 0.0)))
    conceded = max(0.0, min(3.0, float(avg_conceded_stars or 0.0)))

    if ratio >= 0.45 and conceded <= 1.5:
        return (
            "Top-heavy Hard",
            "Open with safe 2-star routes on upper bases and reserve strongest triples for late cleanup pivots.",
        )
    if ratio >= 0.45:
        return (
            "Top-heavy Manageable",
            "Top is dense but breakable; split strongest attackers across top lanes to secure early momentum.",
        )
    if conceded >= 2.0:
        return (
            "Breakable Defense",
            "Defenses are conceding stars; push aggressive triple plans early and chain cleanup quickly.",
        )
    return (
        "Balanced Lineup",
        "Use mirror-first planning, then convert remaining attacks into controlled cleanup opportunities.",
    )


def _war_performance_band(participation: float, avg_stars: float, missed_streak: int) -> tuple[str, str]:
    part = max(0.0, min(100.0, float(participation or 0.0)))
    stars = max(0.0, min(3.0, float(avg_stars or 0.0)))
    streak = max(0, int(missed_streak or 0))

    if streak == 0 and part >= 90.0 and stars >= 2.2:
        return "Elite", "🟢"
    if streak <= 1 and part >= 75.0 and stars >= 1.8:
        return "Reliable", "🟡"
    if part >= 55.0 and stars >= 1.3:
        return "Developing", "🟠"
    return "At Risk", "🔴"


def _war_performance_action_hint(participation: float, avg_stars: float, missed_streak: int) -> str:
    part = max(0.0, min(100.0, float(participation or 0.0)))
    stars = max(0.0, min(3.0, float(avg_stars or 0.0)))
    streak = max(0, int(missed_streak or 0))

    if streak >= 2:
        return "Stop streak risk first: commit attacks early and coordinate with leadership before war close."
    if part < 70.0:
        return "Participation is below target. Prioritize full attack usage every war cycle."
    if stars < 1.8:
        return "Improve hit value: favor safer 2-star plans before high-risk triples."
    if part >= 90.0 and stars >= 2.2:
        return "Strong performance. Maintain consistency and mentor lower map positions."
    return "Stable track. Keep full usage and target efficient cleanup for incremental gains."


def _warhistory_momentum_band(wins: int, losses: int, ties: int, latest_streak: int) -> tuple[str, str]:
    w = max(0, int(wins or 0))
    l = max(0, int(losses or 0))
    t = max(0, int(ties or 0))
    streak = max(0, int(latest_streak or 0))
    total = max(1, w + l + t)
    points_rate = (float(w) + (0.5 * float(t))) / float(total)

    if streak >= 3 or points_rate >= 0.70:
        return "Strong Momentum", "🟢"
    if points_rate >= 0.50:
        return "Balanced", "🟡"
    if points_rate >= 0.35:
        return "Unstable", "🟠"
    return "Downtrend", "🔴"


def _warhistory_action_hint(momentum_label: str, latest_streak: int, wins: int, losses: int) -> str:
    label = str(momentum_label or "").strip().lower()
    streak = max(0, int(latest_streak or 0))
    w = max(0, int(wins or 0))
    l = max(0, int(losses or 0))

    if "strong momentum" in label:
        return "Keep current war plans consistent and focus on converting safe 2-stars into late cleanup triples."
    if "balanced" in label:
        return "Results are mixed. Tighten target assignments and prioritize high-certainty attacks first."
    if "unstable" in label:
        return "Momentum is shaky. Review recent losses and simplify plans to stabilize attack value."
    if streak == 0 and l > w:
        return "Current downtrend. Run leadership reset: safer openings, stricter cleanup sequencing, and attendance checks."
    return "Apply conservative war plans until consistency improves across consecutive wars."


def _record_war_ended(clan_tag: str, clan_name: str, war: Dict[str, Any]) -> Dict[str, Any]:
    clan_data = war.get("clan") or {}
    opp_data = war.get("opponent") or {}

    clan_stars = int(clan_data.get("stars", 0) or 0)
    opp_stars = int(opp_data.get("stars", 0) or 0)
    clan_destruction = float(clan_data.get("destructionPercentage", 0.0) or 0.0)
    opp_destruction = float(opp_data.get("destructionPercentage", 0.0) or 0.0)
    result = _determine_war_result(clan_stars, opp_stars, clan_destruction, opp_destruction)

    results = load_war_results()
    clan_results = results.get(clan_tag, [])
    prev_streak = int(clan_results[-1].get("win_streak", 0)) if clan_results else 0
    win_streak = (prev_streak + 1) if result == "win" else 0

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "clan_name": clan_name,
        "state": war.get("state", "warEnded"),
        "result": result,
        "clan_stars": clan_stars,
        "opponent_stars": opp_stars,
        "clan_destruction": round(clan_destruction, 2),
        "opponent_destruction": round(opp_destruction, 2),
        "opponent_name": opp_data.get("name", "Unknown"),
        "opponent_tag": opp_data.get("tag", ""),
        "win_streak": win_streak,
    }

    clan_results.append(record)
    results[clan_tag] = clan_results[-100:]
    save_war_results(results)

    player_stats = load_war_player_stats()
    clan_stats = player_stats.get(clan_tag, {})

    members = clan_data.get("members") or []
    opponent_members = opp_data.get("members") or []
    opponent_th_by_tag = {}
    for om in opponent_members:
        if isinstance(om, dict) and om.get("tag"):
            opponent_th_by_tag[str(om.get("tag"))] = int(om.get("townhallLevel", 0) or 0)

    for m in members:
        if not isinstance(m, dict) or not m.get("tag"):
            continue
        tag = m.get("tag")
        name = m.get("name", "Unknown")
        attacker_th = int(m.get("townhallLevel", 0) or 0)
        attacks = m.get("attacks") or []
        used = len(attacks)
        stars = sum(int(a.get("stars", 0) or 0) for a in attacks)
        destruction_sum = 0.0
        giant_slayer_hits = 0
        for a in attacks:
            destruction_sum += float(a.get("destructionPercentage", a.get("destructionPercent", 0.0)) or 0.0)
            defender_tag = str(a.get("defenderTag", "") or "")
            defender_th = int(opponent_th_by_tag.get(defender_tag, 0) or 0)
            attack_stars = int(a.get("stars", 0) or 0)
            if attack_stars >= 3 and defender_th >= attacker_th + 2:
                giant_slayer_hits += 1

        row = clan_stats.get(tag, {
            "name": name,
            "wars_participated": 0,
            "attacks_used": 0,
            "total_possible_attacks": 0,
            "stars_earned": 0,
            "destruction_sum": 0.0,
            "missed_attacks": 0,
            "missed_streak": 0,
            "participation_streak": 0,
            "giant_slayer_3stars": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        })

        row["name"] = name
        row["wars_participated"] += 1
        row["attacks_used"] += used
        row["total_possible_attacks"] += 2
        row["stars_earned"] += stars
        row["destruction_sum"] += destruction_sum
        row["missed_attacks"] += max(0, 2 - used)
        row["missed_streak"] = int(row.get("missed_streak", 0) or 0) + 1 if used == 0 else 0
        row["participation_streak"] = int(row.get("participation_streak", 0) or 0) + 1 if used > 0 else 0
        row["giant_slayer_3stars"] = int(row.get("giant_slayer_3stars", 0) or 0) + giant_slayer_hits
        row["last_updated"] = datetime.now(timezone.utc).isoformat()
        clan_stats[tag] = row

    player_stats[clan_tag] = clan_stats
    save_war_player_stats(player_stats)

    attack_log = load_war_attack_log()
    clan_attack_rows = attack_log.get(clan_tag, [])
    if not isinstance(clan_attack_rows, list):
        clan_attack_rows = []

    war_timestamp = record["timestamp"]
    opponent_name = str(opp_data.get("name") or "Unknown")
    opponent_tag = str(opp_data.get("tag") or "")

    for m in members:
        if not isinstance(m, dict) or not m.get("tag"):
            continue
        attacker_tag = str(m.get("tag") or "")
        attacker_name = str(m.get("name") or "Unknown")
        attacker_th = _as_int(m.get("townhallLevel"), 0)
        attacks = m.get("attacks") or []
        if not isinstance(attacks, list):
            continue

        for atk in attacks:
            if not isinstance(atk, dict):
                continue
            defender_tag = str(atk.get("defenderTag") or "")
            defender_th = _as_int(opponent_th_by_tag.get(defender_tag, 0), 0)
            stars = _as_int(atk.get("stars"), 0)
            destruction = float(atk.get("destructionPercentage", atk.get("destructionPercent", 0.0)) or 0.0)

            clan_attack_rows.append(
                {
                    "timestamp": war_timestamp,
                    "clan_tag": clan_tag,
                    "clan_name": clan_name,
                    "opponent_tag": opponent_tag,
                    "opponent_name": opponent_name,
                    "result": result,
                    "attacker_tag": attacker_tag,
                    "attacker_name": attacker_name,
                    "attacker_th": attacker_th,
                    "defender_tag": defender_tag,
                    "defender_th": defender_th,
                    "stars": stars,
                    "destruction": round(destruction, 2),
                    "efficiency": _star_efficiency_label(attacker_th, defender_th, stars),
                }
            )

    attack_log[clan_tag] = clan_attack_rows[-3000:]
    save_war_attack_log(attack_log)

    performers: List[Tuple[str, int, float]] = []
    for m in members:
        if not isinstance(m, dict):
            continue
        attacks = m.get("attacks") or []
        if not attacks:
            continue
        total_stars = sum(int(a.get("stars", 0) or 0) for a in attacks)
        total_destruction = sum(float(a.get("destructionPercentage", a.get("destructionPercent", 0.0)) or 0.0) for a in attacks)
        avg_destruction = total_destruction / max(1, len(attacks))
        performers.append((m.get("name", "Unknown"), total_stars, avg_destruction))

    performers.sort(key=lambda x: (x[1], x[2]), reverse=True)

    return {
        "record": record,
        "win_streak": win_streak,
        "top_performers": performers[:3],
    }


async def _fetch_clan_rankings(bot, location_id: int) -> Optional[Dict[str, Any]]:
    """Fetch clan rankings for a specific location."""
    try:
        payload = await bot.coc_get(f"/locations/{location_id}/rankings/clans")
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        logger.warning(f"Failed to fetch clan rankings for location {location_id}: {e}")
        return None


async def _fetch_player_rankings(bot, location_id: int) -> Optional[Dict[str, Any]]:
    """Fetch player rankings for a specific location."""
    try:
        payload = await bot.coc_get(f"/locations/{location_id}/rankings/players")
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        logger.warning(f"Failed to fetch player rankings for location {location_id}: {e}")
        return None


def _format_rankings(title: str, items: List[Dict[str, Any]], limit: int = 25) -> List[discord.Embed]:
    """Format rankings data into paginated embeds."""
    embeds = []
    
    if not items:
        embed = discord.Embed(
            title=title,
            description="No rankings data available.",
            color=discord.Color.greyple()
        )
        embeds.append(embed)
        return embeds
    
    page_size = 10
    total_items = min(len(items), limit)
    total_pages = (total_items + page_size - 1) // page_size
    
    for page_num in range(total_pages):
        start = page_num * page_size
        end = min(start + page_size, total_items)
        page_items = items[start:end]
        
        embed = discord.Embed(
            title=f"{title} (Page {page_num + 1}/{total_pages})" if total_pages > 1 else title,
            color=discord.Color.gold()
        )
        
        rank_lines = []
        for idx, item in enumerate(page_items, start=start + 1):
            name = item.get("name", "Unknown")
            tag = item.get("tag", "Unknown")
            trophies = item.get("trophies", 0)
            exp_level = item.get("expLevel", 0)
            
            line = f"**{idx}.** {name} ({tag})\n    🏆 {trophies:,} | 🔆 Level {exp_level}"
            rank_lines.append(line)
        
        embed.description = "\n".join(rank_lines)
        embeds.append(embed)
    
    return embeds


async def _fetch_labels(bot, label_type: str) -> Optional[Dict[str, Any]]:
    """Fetch available labels for clans or players."""
    if label_type.lower() not in ["clans", "players"]:
        return None
    
    try:
        payload = await bot.coc_get(f"/labels/{label_type.lower()}")
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        logger.warning(f"Failed to fetch {label_type} labels: {e}")
        return None


def _format_labels(items: List[Dict[str, Any]], label_type: str) -> discord.Embed:
    """Format labels into a single embed."""
    if not items:
        embed = discord.Embed(
            title=f"📌 {label_type.title()} Labels",
            description="No labels available.",
            color=discord.Color.greyple()
        )
        return embed
    
    embed = discord.Embed(
        title=f"📌 {label_type.title()} Labels",
        color=discord.Color.blurple()
    )
    
    label_lines = []
    for idx, item in enumerate(items, 1):
        name = item.get("name", "Unknown")
        label_id = item.get("id", "N/A")
        icon_url = item.get("iconURL", "")
        
        line = f"**{idx}.** `{name}` (ID: {label_id})"
        label_lines.append(line)
    
    embed.description = "\n".join(label_lines[:30])  # Limit to 30 labels per embed
    embed.set_footer(text=f"Total: {len(items)} labels available")
    
    return embed


# In-memory cache for locations (API endpoint is stable, unlikely to change frequently)
_LOCATIONS_CACHE: List[Dict[str, Any]] = []
_LOCATIONS_CACHE_TIME: Optional[float] = None
_LOCATIONS_CACHE_TTL = 3600  # 1 hour cache TTL


async def _fetch_locations(bot, force_refresh: bool = False) -> List[Dict[str, Any]]:
    """Fetch all locations with local caching to reduce API calls."""
    global _LOCATIONS_CACHE, _LOCATIONS_CACHE_TIME
    
    from time import time
    
    now = time()
    if (
        not force_refresh
        and _LOCATIONS_CACHE
        and _LOCATIONS_CACHE_TIME
        and (now - _LOCATIONS_CACHE_TIME) < _LOCATIONS_CACHE_TTL
    ):
        return _LOCATIONS_CACHE
    
    try:
        payload = await bot.coc_get("/locations")
        if isinstance(payload, dict) and "items" in payload:
            items = payload.get("items", [])
            if isinstance(items, list):
                _LOCATIONS_CACHE = items
                _LOCATIONS_CACHE_TIME = now
                return items
    except Exception as e:
        logger.warning(f"Failed to fetch locations: {e}")
    
    return _LOCATIONS_CACHE


async def _fetch_location_detail(bot, location_id: int) -> Optional[Dict[str, Any]]:
    """Fetch details for a specific location."""
    try:
        payload = await bot.coc_get(f"/locations/{location_id}")
        return payload if isinstance(payload, dict) else None
    except Exception as e:
        logger.warning(f"Failed to fetch location {location_id}: {e}")
        return None


def _format_locations_list(items: List[Dict[str, Any]], search_term: Optional[str] = None) -> List[discord.Embed]:
    """Format locations into paginated embeds, optionally filtered by search term."""
    filtered_items = items
    
    if search_term:
        term_lower = search_term.lower()
        filtered_items = [
            item for item in items
            if term_lower in str(item.get("name", "")).lower()
            or term_lower in str(item.get("id", "")).lower()
        ]
    
    if not filtered_items:
        embed = discord.Embed(
            title="📍 Locations",
            description="No locations found matching your search.",
            color=discord.Color.greyple()
        )
        return [embed]
    
    embeds = []
    page_size = 15
    total_pages = (len(filtered_items) + page_size - 1) // page_size
    
    for page_num in range(total_pages):
        start = page_num * page_size
        end = min(start + page_size, len(filtered_items))
        page_items = filtered_items[start:end]
        
        embed = discord.Embed(
            title=f"📍 Locations (Page {page_num + 1}/{total_pages})" if total_pages > 1 else "📍 Locations",
            color=discord.Color.blue()
        )
        
        loc_lines = []
        for item in page_items:
            name = item.get("name", "Unknown")
            loc_id = item.get("id", 0)
            is_country = item.get("isCountry", False)
            
            country_flag = "🇦 " if is_country else ""
            loc_lines.append(f"{country_flag}**{name}** (`{loc_id}`)")
        
        embed.description = "\n".join(loc_lines)
        embeds.append(embed)
    
    return embeds





class WarCog(commands.Cog, name="War"):
    """War tracking, attack announcements, and war reminders."""

    def __init__(self, bot):
        self.bot = bot
        self._war_tasks: Dict[str, asyncio.Task] = {}
        self._war_baselines: Dict[str, Dict[str, Any]] = {}
        self._last_war_state: Dict[str, str] = {}
        self._prep_announcement_key: Dict[str, str] = {}
        self._prep_analysis_key: Dict[str, str] = {}

    async def cog_load(self):
        for clan in self.bot.get_all_monitored_clans():
            self.start_tracking(clan)
        self.fixed_time_reminder.start()
        self.active_war_reminder_loop.start()
        self.pinned_cleanup_loop.start()

    async def cog_unload(self):
        for tag in list(self._war_tasks):
            self.stop_tracking(tag)
        self.fixed_time_reminder.cancel()
        self.active_war_reminder_loop.cancel()
        self.pinned_cleanup_loop.cancel()

    async def _ensure_leadership_ctx(self, ctx: commands.Context) -> bool:
        if not has_leadership_role(ctx.author, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            await ctx.send("❌ Leadership role required for this command.")
            return False
        return True

    def _load_active_war_reminder_state(self) -> Dict[str, Dict[str, str]]:
        settings = load_settings()
        data = settings.get(_WAR_ACTIVE_REMINDER_KEY, {})
        if not isinstance(data, dict):
            return {}
        cleaned: Dict[str, Dict[str, str]] = {}
        for clan_tag, row in data.items():
            if not isinstance(row, dict):
                continue
            war_key = str(row.get("war_key") or "")
            last_sent_at = str(row.get("last_sent_at") or "")
            if not war_key or not last_sent_at:
                continue
            cleaned[str(clan_tag)] = {
                "war_key": war_key,
                "last_sent_at": last_sent_at,
            }
        return cleaned

    def _save_active_war_reminder_state(self, state: Dict[str, Dict[str, str]]) -> None:
        settings = load_settings()
        settings[_WAR_ACTIVE_REMINDER_KEY] = state
        save_settings(settings)

    def _set_active_war_reminder_checkpoint(self, clan_tag: str, war_key: str, when: Optional[datetime] = None) -> None:
        state = self._load_active_war_reminder_state()
        state[str(clan_tag)] = {
            "war_key": str(war_key),
            "last_sent_at": (when or datetime.now(timezone.utc)).isoformat(),
        }
        self._save_active_war_reminder_state(state)

    def _load_war_pin_entries(self) -> List[Dict[str, Any]]:
        settings = load_settings()
        entries = settings.get(_WAR_PIN_SETTINGS_KEY, [])
        if not isinstance(entries, list):
            return []
        cleaned: List[Dict[str, Any]] = []
        for row in entries:
            if not isinstance(row, dict):
                continue
            channel_id = row.get("channel_id")
            message_id = row.get("message_id")
            pinned_at = row.get("pinned_at")
            if channel_id is None or message_id is None or not pinned_at:
                continue
            cleaned.append({
                "channel_id": int(channel_id),
                "message_id": int(message_id),
                "pinned_at": str(pinned_at),
            })
        return cleaned

    def _save_war_pin_entries(self, entries: List[Dict[str, Any]]) -> None:
        settings = load_settings()
        settings[_WAR_PIN_SETTINGS_KEY] = entries
        save_settings(settings)

    def _register_war_pinned_message(self, channel_id: int, message_id: int, pinned_at: Optional[datetime] = None) -> None:
        entries = self._load_war_pin_entries()
        entries.append({
            "channel_id": int(channel_id),
            "message_id": int(message_id),
            "pinned_at": (pinned_at or datetime.now(timezone.utc)).isoformat(),
        })
        entries = entries[-500:]
        self._save_war_pin_entries(entries)

    def start_tracking(self, clan: Dict[str, str]):
        tag = clan["tag"]
        if tag not in self._war_tasks:
            self._war_tasks[tag] = asyncio.create_task(self._war_tracker(clan))
            logger.info("Started war tracker for %s (%s)", clan["name"], tag)

    def stop_tracking(self, clan_tag: str):
        task = self._war_tasks.pop(clan_tag, None)
        if task:
            task.cancel()
            logger.info("Stopped war tracker for %s", clan_tag)

    async def _send_embed_with_optional_pin(self, channel: discord.abc.Messageable, embed: discord.Embed, *, pin: bool = False):
        msg = await channel.send(embed=embed)
        if pin:
            try:
                await msg.pin(reason="CC2 important announcement")
                self._register_war_pinned_message(int(msg.channel.id), int(msg.id), msg.created_at)
            except Exception:
                pass
        return msg

    async def _cleanup_expired_war_pins(self) -> None:
        entries = self._load_war_pin_entries()
        if not entries:
            return

        now = datetime.now(timezone.utc)
        keep: List[Dict[str, Any]] = []

        for row in entries:
            pinned_at_text = str(row.get("pinned_at") or "")
            try:
                pinned_at = datetime.fromisoformat(pinned_at_text)
                if pinned_at.tzinfo is None:
                    pinned_at = pinned_at.replace(tzinfo=timezone.utc)
            except Exception:
                # Drop malformed entries so they do not accumulate forever.
                continue

            if now - pinned_at < timedelta(hours=24):
                keep.append(row)
                continue

            channel_id = int(row.get("channel_id", 0) or 0)
            message_id = int(row.get("message_id", 0) or 0)
            if channel_id <= 0 or message_id <= 0:
                continue

            try:
                channel = self.bot.get_channel(channel_id)
                if channel is None:
                    channel = await self.bot.fetch_channel(channel_id)
                msg = await channel.fetch_message(message_id)

                if getattr(msg, "pinned", False):
                    try:
                        await msg.unpin(reason="CC2 auto cleanup after 24 hours")
                    except Exception:
                        pass

                try:
                    await msg.delete()
                except Exception:
                    pass
            except Exception as cleanup_err:
                logger.debug("War pin cleanup fetch failed for channel %s message %s: %s", channel_id, message_id, cleanup_err)
                # Message/channel likely gone. Remove stale entry.
                continue

        if len(keep) != len(entries):
            self._save_war_pin_entries(keep)

    async def _send_war_start_embed(self, clan_tag: str, clan_name: str, war: Dict[str, Any]) -> None:
        opp = war.get("opponent") or {}
        emb = discord.Embed(
            title=f"🚩 War Started — {clan_name}",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Opponent", value=f"{opp.get('name', 'Unknown')} `{opp.get('tag', '')}`", inline=False)
        emb.add_field(name="Team Size", value=str(war.get("teamSize", "N/A")), inline=True)
        emb.add_field(name="Attacks Per Player", value="2", inline=True)
        emb.set_footer(text="CC2 Clash Bot • War Start")

        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
        for channel in channels:
            await self._send_embed_with_optional_pin(channel, emb, pin=True)

    async def _send_war_end_embed(self, clan_tag: str, clan_name: str, war: Dict[str, Any], summary: Dict[str, Any]) -> None:
        record = summary.get("record", {})
        result = str(record.get("result", "unknown")).upper()
        color = discord.Color.green() if result == "WIN" else (discord.Color.red() if result == "LOSS" else discord.Color.gold())

        emb = discord.Embed(
            title=f"🏁 War Ended — {clan_name}",
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Result", value=result, inline=True)
        emb.add_field(
            name="Score",
            value=f"{record.get('clan_stars', 0)} - {record.get('opponent_stars', 0)}★",
            inline=True,
        )
        emb.add_field(
            name="Destruction",
            value=f"{record.get('clan_destruction', 0)}% - {record.get('opponent_destruction', 0)}%",
            inline=True,
        )
        emb.add_field(
            name="Opponent",
            value=f"{record.get('opponent_name', 'Unknown')} `{record.get('opponent_tag', '')}`",
            inline=False,
        )

        top_performers = summary.get("top_performers", [])
        if top_performers:
            lines = [f"• {nm} — {st}★, {dst:.1f}% avg" for nm, st, dst in top_performers]
            emb.add_field(name="Top Performers", value="\n".join(lines), inline=False)

        streak = int(summary.get("win_streak", 0) or 0)
        if streak > 0:
            emb.add_field(name="Current Win Streak", value=f"{streak}", inline=True)

        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
        for channel in channels:
            await self._send_embed_with_optional_pin(channel, emb, pin=True)

        if streak in _STREAK_MILESTONES:
            milestone = discord.Embed(
                title="🔥 Win Streak Milestone!",
                description=f"**{clan_name}** reached a **{streak}-war win streak**!",
                color=discord.Color.gold(),
                timestamp=datetime.now(timezone.utc),
            )
            milestone.set_footer(text="CC2 Clash Bot • War Milestone")
            for channel in channels:
                await channel.send(embed=milestone)

    async def _send_missed_streak_dm(self, clan_tag: str, clan_name: str, war: Dict[str, Any]) -> None:
        """DM linked members who now have 2+ consecutive wars with no attacks."""
        members = ((war.get("clan") or {}).get("members") or [])
        if not members:
            return

        stats_data = load_war_player_stats()
        clan_stats = stats_data.get(clan_tag, {}) if isinstance(stats_data.get(clan_tag, {}), dict) else {}

        for m in members:
            if not isinstance(m, dict):
                continue
            tag = str(m.get("tag") or "").upper()
            if not tag:
                continue
            attacks_used = len(m.get("attacks") or [])
            if attacks_used > 0:
                continue

            row = clan_stats.get(tag, {}) if isinstance(clan_stats.get(tag, {}), dict) else {}
            missed_streak = int(row.get("missed_streak", 0) or 0)
            if missed_streak < 2:
                continue

            discord_id = get_linked_user_for_tag(tag)
            if not discord_id:
                continue
            try:
                user = await self.bot.fetch_user(int(discord_id))
                await user.send(
                    "⚠️ **War Participation Warning**\n"
                    f"You have missed attacks in **{missed_streak} consecutive wars** for **{clan_name}**.\n"
                    "Please coordinate with leadership and use your attacks in upcoming wars."
                )
                await asyncio.sleep(0.25)
            except Exception:
                pass

    async def _send_preparation_reminder(self, clan_tag: str, clan_name: str, war: Dict[str, Any]) -> None:
        prep_key = str(war.get("startTime") or war.get("preparationStartTime") or "unknown")
        if self._prep_announcement_key.get(clan_tag) == prep_key:
            return

        members = ((war.get("clan") or {}).get("members") or [])
        if not members:
            return

        bases = load_bases()
        missing = []
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            has_any_base = bool((bases.get(tag) or {}))
            if not has_any_base:
                missing.append(f"• {m.get('name', 'Unknown')} `{tag}`")

        if not missing:
            self._prep_announcement_key[clan_tag] = prep_key
            return

        emb = discord.Embed(
            title=f"🛡️ War Preparation Reminder — {clan_name}",
            description="Members with no saved war base in basebook:",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name=f"Missing Basebook Entries ({len(missing)})", value="\n".join(missing[:40]), inline=False)
        emb.set_footer(text="Use /setbase war <link> <name> to save war bases")

        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
        for channel in channels:
            await channel.send(embed=emb)

        self._prep_announcement_key[clan_tag] = prep_key

    async def _send_preparation_opponent_analysis(self, clan_tag: str, clan_name: str, war: Dict[str, Any]) -> None:
        prep_key = str(war.get("startTime") or war.get("preparationStartTime") or "unknown")
        if self._prep_analysis_key.get(clan_tag) == prep_key:
            return

        opp = war.get("opponent") or {}
        opp_tag = str(opp.get("tag") or "")
        opp_name = opp.get("name", "Unknown")
        opp_level = _as_int(opp.get("clanLevel"), 0)

        opp_wins = 0
        avg_opp_th = 0.0
        if opp_tag:
            try:
                data = await self.bot.coc_get(f"/clans/{urllib.parse.quote(opp_tag)}")
                if isinstance(data, dict):
                    opp_wins = _as_int(data.get("warWins"), 0)
                    members = data.get("memberList") or []
                    th_values = [_as_int(m.get("townHallLevel"), 0) for m in members if isinstance(m, dict)]
                    th_values = [x for x in th_values if x > 0]
                    if th_values:
                        avg_opp_th = sum(th_values) / len(th_values)
            except Exception:
                pass

        emb = discord.Embed(
            title=f"🧭 War Opponent Analysis — {clan_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Opponent", value=f"**{opp_name}** `{opp_tag}`", inline=False)
        emb.add_field(name="Opponent Clan Level", value=str(opp_level if opp_level > 0 else "N/A"), inline=True)
        emb.add_field(name="Opponent War Wins", value=str(opp_wins if opp_wins > 0 else "N/A"), inline=True)
        emb.add_field(name="Opponent Avg TH", value=(f"{avg_opp_th:.2f}" if avg_opp_th > 0 else "N/A"), inline=True)
        emb.set_footer(text="CC2 Clash Bot • Preparation Analysis")

        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
        for channel in channels:
            await channel.send(embed=emb)

        self._prep_analysis_key[clan_tag] = prep_key

    async def _send_war_attack_updates(
        self,
        clan_tag: str,
        clan_name: str,
        war: Dict[str, Any],
        events: List[Dict[str, Any]],
    ) -> None:
        if not events:
            return

        channels = await self.bot.get_announce_channels_for_clan(clan_tag)
        if not channels:
            return

        clan_data = war.get("clan") or {}
        opp_data = war.get("opponent") or {}
        members = clan_data.get("members") or []

        clan_stars = _as_int(clan_data.get("stars"), 0)
        opp_stars = _as_int(opp_data.get("stars"), 0)
        clan_dest = float(clan_data.get("destructionPercentage", 0.0) or 0.0)
        opp_dest = float(opp_data.get("destructionPercentage", 0.0) or 0.0)

        total_members = len([m for m in members if isinstance(m, dict)])
        attacks_used = sum(len((m.get("attacks") or [])) for m in members if isinstance(m, dict))
        total_possible = total_members * 2
        pending_zero = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]
        pending_zero = sorted(pending_zero, key=lambda m: _as_int(m.get("mapPosition"), 999))

        sorted_events = sorted(
            events,
            key=lambda e: (int(e.get("stars", 0)), float(e.get("destruction", 0.0))),
            reverse=True,
        )

        chunks: List[List[Dict[str, Any]]] = []
        chunk_size = 8
        for i in range(0, len(sorted_events), chunk_size):
            chunks.append(sorted_events[i:i + chunk_size])

        for idx, chunk in enumerate(chunks, start=1):
            emb = discord.Embed(
                title=(
                    f"⚔️ War Attack Feed — {clan_name}"
                    if len(chunks) == 1
                    else f"⚔️ War Attack Feed — {clan_name} ({idx}/{len(chunks)})"
                ),
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            emb.add_field(
                name="Live Scoreboard",
                value=(
                    f"★ {clan_stars} - {opp_stars}\n"
                    f"💥 {clan_dest:.2f}% - {opp_dest:.2f}%\n"
                    f"🗡️ Attacks Used: {attacks_used}/{total_possible}"
                ),
                inline=False,
            )

            hit_lines: List[str] = []
            for e in chunk:
                stars = int(e.get("stars", 0) or 0)
                stars_glyph = "★" * max(0, min(3, stars)) + "☆" * max(0, 3 - max(0, min(3, stars)))
                hit_lines.append(
                    f"{stars_glyph} {e.get('attacker_name')} (TH{e.get('attacker_th')} #{e.get('attacker_pos')}) "
                    f"→ {e.get('defender_name')} (TH{e.get('defender_th')} #{e.get('defender_pos')}) "
                    f"• {float(e.get('destruction', 0.0)):.1f}% • {e.get('efficiency')}"
                )
            emb.add_field(name=f"New Attacks ({len(chunk)})", value="\n".join(hit_lines), inline=False)

            if idx == 1:
                if pending_zero:
                    action_lines: List[str] = []
                    for m in pending_zero[:10]:
                        player_tag = str(m.get("tag") or "")
                        linked = get_linked_user_for_tag(player_tag) if player_tag else None
                        mention = f"<@{linked}> " if linked else ""
                        action_lines.append(
                            f"• {mention}{m.get('name', 'Unknown')} (TH{m.get('townhallLevel', '?')} #{_as_int(m.get('mapPosition'), '?')})"
                        )
                    if len(pending_zero) > 10:
                        action_lines.append(f"• ...and {len(pending_zero) - 10} more")
                    emb.add_field(name="Immediate Action (0/2 attacks)", value="\n".join(action_lines), inline=False)
                else:
                    emb.add_field(
                        name="Immediate Action",
                        value="No zero-attack members remaining. Focus on high-value cleanup hits.",
                        inline=False,
                    )

            emb.set_footer(text="CC2 Clash Bot • War Tracker")

            for channel in channels:
                try:
                    await channel.send(embed=emb)
                    await asyncio.sleep(0.08)
                except Exception as send_err:
                    logger.debug("War attack update send failed for %s in channel %s: %s", clan_tag, getattr(channel, "id", "?"), send_err)

    async def _war_tracker(self, clan: Dict[str, str]):
        await self.bot.wait_until_ready()
        clan_name = clan["name"]
        clan_tag = clan["tag"]
        self._war_baselines[clan_tag] = load_war_baseline(clan_tag)
        no_war_backoff = WAR_POLL_INTERVAL

        while not self.bot.is_closed():
            try:
                war = await self.bot.get_current_war(clan_tag)
                state = (war or {}).get("state")
                last_state = self._last_war_state.get(clan_tag)

                if war and state == "warEnded" and last_state != "warEnded":
                    try:
                        summary = _record_war_ended(clan_tag, clan_name, war)
                        await self._send_war_end_embed(clan_tag, clan_name, war, summary)
                        await self._send_missed_streak_dm(clan_tag, clan_name, war)
                        logger.info("Recorded war result for %s (%s)", clan_name, clan_tag)
                    except Exception as rec_err:
                        logger.error("Failed to process war-end for %s: %s", clan_name, rec_err)

                if war and state == "inWar" and last_state != "inWar":
                    try:
                        await self._send_war_start_embed(clan_tag, clan_name, war)
                        war_key = str(war.get("startTime") or war.get("preparationStartTime") or war.get("endTime") or "unknown")
                        self._set_active_war_reminder_checkpoint(clan_tag, war_key)
                    except Exception as start_err:
                        logger.debug("War-start announcement failed for %s: %s", clan_name, start_err)

                if war and state == "preparation":
                    try:
                        await self._send_preparation_opponent_analysis(clan_tag, clan_name, war)
                        await self._send_preparation_reminder(clan_tag, clan_name, war)
                    except Exception as prep_err:
                        logger.debug("Prep reminder failed for %s: %s", clan_name, prep_err)

                self._last_war_state[clan_tag] = state or "unknown"

                if not war or state != "inWar":
                    no_war_backoff = min(no_war_backoff * 1.5, 120)
                    await asyncio.sleep(no_war_backoff)
                    continue

                no_war_backoff = WAR_POLL_INTERVAL

                clan_data = war.get("clan") or {}
                opp_data = war.get("opponent") or {}
                members = clan_data.get("members") or []
                opp_members = opp_data.get("members") or []
                opp_by_tag = {m.get("tag"): m for m in opp_members if isinstance(m, dict) and m.get("tag")}
                current_map: Dict[str, list] = {}

                for member in members:
                    if not isinstance(member, dict):
                        continue
                    tag = member.get("tag")
                    if tag:
                        current_map[tag] = member.get("attacks", []) or []

                prev_map = self._war_baselines.get(clan_tag, {})
                new_events: List[Dict[str, Any]] = []

                for tag, attacks in current_map.items():
                    prev_attacks = prev_map.get(tag, [])
                    if len(attacks) <= len(prev_attacks):
                        continue

                    attacker = next((m for m in members if m.get("tag") == tag), {})
                    attacker_name = attacker.get("name") or tag
                    attacker_th = attacker.get("townhallLevel", "?")
                    attacker_pos = _as_int(attacker.get("mapPosition"), 0)

                    for atk in attacks[len(prev_attacks):]:
                        stars = int(atk.get("stars", 0) or 0)
                        desc_raw = atk.get("destructionPercentage", atk.get("destructionPercent", 0.0))
                        try:
                            destruction_pct = float(desc_raw or 0.0)
                        except Exception:
                            destruction_pct = 0.0
                        defender_tag = atk.get("defenderTag")
                        defender = opp_by_tag.get(defender_tag, {}) if defender_tag else {}
                        defender_name = defender.get("name") or (defender_tag or "Unknown")
                        defender_th = defender.get("townhallLevel", "?")
                        defender_pos = _as_int(defender.get("mapPosition"), 0)
                        result_note = _star_efficiency_label(attacker_th, defender_th, stars)
                        new_events.append({
                            "attacker_name": attacker_name,
                            "attacker_th": attacker_th,
                            "attacker_pos": (attacker_pos if attacker_pos > 0 else "?"),
                            "defender_name": defender_name,
                            "defender_th": defender_th,
                            "defender_pos": (defender_pos if defender_pos > 0 else "?"),
                            "stars": stars,
                            "destruction": destruction_pct,
                            "efficiency": result_note,
                        })

                if new_events:
                    await self._send_war_attack_updates(clan_tag, clan_name, war, new_events)

                self._war_baselines[clan_tag] = current_map
                save_war_baseline(clan_tag, current_map)
                await asyncio.sleep(WAR_POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("War tracker error for %s: %s", clan_name, e)
                await asyncio.sleep(WAR_POLL_INTERVAL)

    @tasks.loop(seconds=30)
    async def fixed_time_reminder(self):
        enabled = bool(self.bot.resolve_effective_setting("war_reminder_enabled", getattr(self.bot, "war_reminder_enabled", True)))
        if not enabled:
            return

        now = datetime.now()
        hour, minute = now.hour, now.minute
        if minute != 0 or hour % 2 != 0:
            return

        for clan in self.bot.get_all_monitored_clans():
            war = await self.bot.get_current_war(clan["tag"])
            if not war or war.get("state") != "inWar":
                continue

            members = (war.get("clan") or {}).get("members") or []
            pending = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]
            if not pending:
                continue

            out_lines: List[str] = []
            dm_sent = 0
            dm_failed = 0
            out_lines.append(f"**{clan['name']}** — {len(pending)} pending")
            out_lines += [f"• {p.get('name')} `{p.get('tag')}`" for p in pending[:40]]

            links = load_links()
            for p in pending:
                tag_norm = (p.get("tag") or "").upper()
                discord_id = links.get(tag_norm)
                if not discord_id:
                    continue
                try:
                    user = await self.bot.fetch_user(int(discord_id))
                    await user.send(
                        f"⚠️ **WAR REMINDER**\nYou have **0 attacks used** in war for **{clan['name']}**.\nPlease attack ASAP!"
                    )
                    dm_sent += 1
                    await asyncio.sleep(0.25)
                except Exception as e:
                    dm_failed += 1
                    logger.debug("DM failed for %s: %s", tag_norm, e)

            try:
                channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
                msg = "⏰ **WAR REMINDER — Every 2 Hours (Even Hours)**\n" + "\n".join(out_lines)
                full = msg + f"\n\n📨 **DM sent:** {dm_sent} | ❌ **Failed:** {dm_failed}"
                for channel in channels:
                    await safe_send(channel, full)
            except Exception as e:
                logger.error("War reminder send failed: %s", e)

    @fixed_time_reminder.before_loop
    async def before_reminder(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=30)
    async def active_war_reminder_loop(self):
        now = datetime.now(timezone.utc)
        reminder_state = self._load_active_war_reminder_state()
        changed = False

        for clan in self.bot.get_all_monitored_clans():
            clan_tag = str(clan.get("tag") or "")
            clan_name = str(clan.get("name") or clan_tag)
            if not clan_tag:
                continue

            war = await self.bot.get_current_war(clan_tag)
            if not war or str(war.get("state") or "") != "inWar":
                continue

            war_key = str(war.get("startTime") or war.get("preparationStartTime") or war.get("endTime") or "unknown")
            row = reminder_state.get(clan_tag, {}) if isinstance(reminder_state.get(clan_tag, {}), dict) else {}

            # Initialize/reset gate for a newly detected war and avoid immediate duplicate send.
            if not row or str(row.get("war_key") or "") != war_key:
                reminder_state[clan_tag] = {
                    "war_key": war_key,
                    "last_sent_at": now.isoformat(),
                }
                changed = True
                continue

            last_sent_raw = str(row.get("last_sent_at") or "")
            try:
                last_sent = datetime.fromisoformat(last_sent_raw)
                if last_sent.tzinfo is None:
                    last_sent = last_sent.replace(tzinfo=timezone.utc)
            except Exception:
                last_sent = now

            if now - last_sent < timedelta(hours=12):
                continue

            opp_data = war.get("opponent") or {}
            emb = discord.Embed(
                title=f"🚩 War Started — {clan_name}",
                color=discord.Color.red(),
                timestamp=now,
            )
            emb.add_field(
                name="Opponent",
                value=f"{opp_data.get('name', 'Unknown')} `{opp_data.get('tag', '')}`",
                inline=False,
            )
            emb.add_field(name="Team Size", value=str(war.get("teamSize", "N/A")), inline=True)
            emb.add_field(name="Attacks Per Player", value="2", inline=True)
            emb.set_footer(text="CC2 Clash Bot • War Start • 12h reminder")

            try:
                channels = await self.bot.get_announce_channels_for_clan(clan_tag)
                for channel in channels:
                    await channel.send(embed=emb)
            except Exception as send_err:
                logger.debug("Active-war reminder send failed for %s: %s", clan_tag, send_err)
                continue

            reminder_state[clan_tag] = {
                "war_key": war_key,
                "last_sent_at": now.isoformat(),
            }
            changed = True

        if changed:
            self._save_active_war_reminder_state(reminder_state)

    @active_war_reminder_loop.before_loop
    async def before_active_war_reminder(self):
        await self.bot.wait_until_ready()

    @tasks.loop(hours=1)
    async def pinned_cleanup_loop(self):
        await self._cleanup_expired_war_pins()

    @pinned_cleanup_loop.before_loop
    async def before_pinned_cleanup(self):
        await self.bot.wait_until_ready()

    @commands.hybrid_command(
        name="whohavenotattacked", aliases=["wna"],
        description="Show players who haven't attacked in current war.",
    )
    @app_commands.describe(clan="(Optional) select a clan; if empty, checks all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def whohavenotattacked(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        from cogs.admin import resolve_clans

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for whohavenotattacked",
                    include_all=True,
                )
                await ctx.send("Select a clan for war attack check:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found in monitored list.")

        out_lines: List[str] = []
        status_lines: List[str] = []
        for c in clans_to_check:
            war = await self.bot.get_current_war(c["tag"])
            if not war:
                status_lines.append(f"• {c['name']}: war data unavailable.")
                continue
            state = war.get("state")
            if state != "inWar":
                if state == "preparation":
                    end_time = war.get("startTime", "unknown")
                    status_lines.append(f"• {c['name']}: currently in preparation (war starts at {end_time}).")
                elif state == "notInWar":
                    status_lines.append(f"• {c['name']}: not currently in war.")
                else:
                    status_lines.append(f"• {c['name']}: state is {state}.")
                continue

            members = (war.get("clan") or {}).get("members") or []
            pending = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]
            urgency_label, urgency_icon = _war_pending_urgency(len(pending), len(members))
            action_hint = _war_pending_action_hint(len(pending), len(members))
            if pending:
                out_lines.append(f"**{c['name']}** — {len(pending)} pending")
                out_lines.append(f"• {urgency_icon} Urgency: **{urgency_label}**")
                out_lines.append(f"• Suggested action: {action_hint}")
                out_lines += [f"• {p.get('name')} `{p.get('tag')}`" for p in pending[:50]]
            else:
                status_lines.append(
                    f"• {c['name']}: {urgency_icon} {urgency_label}. {action_hint}"
                )

        if not out_lines:
            if status_lines:
                await ctx.send("No pending attacks right now.\n" + "\n".join(status_lines))
            else:
                await ctx.send("No ongoing war or everyone attacked.")
            return

        if status_lines:
            out_lines += ["", "**War State Notes:**"] + status_lines

        pages = build_paginated_embeds(
            title="⚔️ Who Has Not Attacked",
            lines=out_lines,
            color=discord.Color.red(),
            per_page=16,
            footer_prefix="CC2 Clash Bot • War",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="warpreview", aliases=["wpv"], description="Pre-war scouting summary for opponent lineup")
    @app_commands.describe(clan="(Optional) select a clan; if empty, checks all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def warpreview(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        from cogs.admin import resolve_clans

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for war preview",
                    include_all=True,
                )
                await ctx.send("Select a clan for war preview:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found in monitored list.")

        lines: List[str] = []
        for c in clans_to_check:
            war = await self.bot.get_current_war(c["tag"])
            if not war:
                lines.append(f"**{c['name']}** — war data unavailable.")
                continue

            state = str(war.get("state") or "unknown")
            if state != "preparation":
                lines.append(f"**{c['name']}** — not in preparation state (current: **{state}**).")
                continue

            opp_data = war.get("opponent") or {}
            opp_name = str(opp_data.get("name") or "Unknown")
            opp_tag = str(opp_data.get("tag") or "")
            opp_level = _as_int(opp_data.get("clanLevel"), 0)
            members = [m for m in (opp_data.get("members") or []) if isinstance(m, dict)]
            if not members:
                lines.append(f"**{c['name']}** vs **{opp_name}** `{opp_tag}` — opponent member list unavailable.")
                continue

            th_counts: Dict[int, int] = {}
            hero_total = 0
            hero_count = 0
            weighted_power = 0

            for m in members:
                th = _as_int(m.get("townhallLevel"), 0)
                if th > 0:
                    th_counts[th] = th_counts.get(th, 0) + 1
                    weighted_power += th * th

                heroes = m.get("heroes") or []
                if isinstance(heroes, list):
                    hsum = sum(_as_int(h.get("level"), 0) for h in heroes if isinstance(h, dict))
                    if hsum > 0:
                        hero_total += hsum
                        hero_count += 1

            avg_th = (sum(th * cnt for th, cnt in th_counts.items()) / len(members)) if members else 0.0
            avg_hero = (hero_total / hero_count) if hero_count > 0 else 0.0
            th_spread = " • ".join([f"TH{th}x{cnt}" for th, cnt in sorted(th_counts.items(), reverse=True)]) or "N/A"
            max_th = max(th_counts.keys()) if th_counts else 0
            top_heavy_count = sum(cnt for th, cnt in th_counts.items() if th >= max(1, max_th - 1)) if max_th > 0 else 0
            top_heavy_ratio = (float(top_heavy_count) / float(len(members))) if members else 0.0

            est_weight_band = "Low"
            if weighted_power >= 9000:
                est_weight_band = "High"
            elif weighted_power >= 7000:
                est_weight_band = "Medium"

            pressure_label, pressure_icon = _warpreview_pressure_band(est_weight_band, avg_th)
            action_hint = _warpreview_action_hint(pressure_label, top_heavy_ratio)

            start_time = str(war.get("startTime") or "unknown")

            lines.append(f"**{c['name']}** vs **{opp_name}** `{opp_tag}` (Lvl {opp_level if opp_level > 0 else '?'})")
            lines.append(f"Prep ends / war starts: `{start_time}`")
            lines.append(f"Opponent roster: **{len(members)}** • Avg TH: **{avg_th:.2f}** • Avg Hero Sum: **{avg_hero:.1f}**")
            lines.append(f"TH spread: {th_spread}")
            lines.append(f"Estimated weight band: **{est_weight_band}** (index {weighted_power})")
            lines.append(f"Pressure: {pressure_icon} **{pressure_label}**")
            lines.append(f"Top-heavy ratio: **{top_heavy_ratio * 100.0:.1f}%**")
            lines.append(f"Tactical plan: {action_hint}")
            lines.append("")

        pages = build_paginated_embeds(
            title="🧭 War Preview",
            lines=lines,
            color=discord.Color.dark_gold(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • War Preview",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="warmap", aliases=["wm"], description="Show current war map positions and attack status")
    @app_commands.describe(clan="(Optional) select a clan; if empty, checks all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def warmap(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        from cogs.admin import resolve_clans

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for warmap",
                    include_all=True,
                )
                await ctx.send("Select a clan for war map:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found in monitored list.")

        lines: List[str] = []
        for c in clans_to_check:
            war = await self.bot.get_current_war(c["tag"])
            if not war or war.get("state") not in {"inWar", "preparation"}:
                lines.append(f"**{c['name']}** — no active/preparation war.")
                continue

            state = str(war.get("state") or "unknown")
            members = (war.get("clan") or {}).get("members") or []
            sorted_members = sorted(members, key=lambda m: _as_int(m.get("mapPosition"), 999))
            zero_used = 0
            one_used = 0
            two_used = 0
            for m in sorted_members:
                used = len((m.get("attacks") or [])) if isinstance(m, dict) else 0
                if used <= 0:
                    zero_used += 1
                elif used == 1:
                    one_used += 1
                else:
                    two_used += 1

            pressure_label, pressure_icon = _warmap_pressure_band(zero_used, one_used, len(sorted_members))
            action_hint = _warmap_action_hint(zero_used, one_used, len(sorted_members))

            lines.append(f"**{c['name']}** — state: **{state}**")
            lines.append(f"Summary: ❌ {zero_used} | ⏳ {one_used} | ✅ {two_used}")
            lines.append(f"Pressure: {pressure_icon} **{pressure_label}**")
            lines.append(f"Suggested action: {action_hint}")
            for m in sorted_members[:50]:
                if not isinstance(m, dict):
                    continue
                pos = _as_int(m.get("mapPosition"), 0)
                attacks_used = len(m.get("attacks") or [])
                status = "✅" if attacks_used >= 2 else ("⏳" if attacks_used == 1 else "❌")
                th = m.get("townhallLevel", "?")
                lines.append(
                    f"{status} #{pos} TH{th} **{m.get('name', 'Unknown')}** `{m.get('tag', '')}` — {attacks_used}/2"
                )

        if not lines:
            return await ctx.send("No war map data available.")

        pages = build_paginated_embeds(
            title="🗺️ War Map Status",
            lines=lines,
            color=discord.Color.blue(),
            per_page=14,
            footer_prefix="CC2 Clash Bot • War Map",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="opponentlineup", aliases=["ol"], description="Show opponent lineup details for war planning")
    @app_commands.describe(clan="(Optional) select a clan; if empty, checks all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def opponentlineup(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        from cogs.admin import resolve_clans

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for opponent lineup",
                    include_all=True,
                )
                await ctx.send("Select a clan for opponent lineup:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found in monitored list.")

        lines: List[str] = []
        for c in clans_to_check:
            war = await self.bot.get_current_war(c["tag"])
            if not war or war.get("state") not in {"inWar", "preparation"}:
                lines.append(f"**{c['name']}** — no active/preparation war.")
                continue

            state = str(war.get("state") or "unknown")
            opp_data = war.get("opponent") or {}
            opp_name = str(opp_data.get("name") or "Unknown")
            opp_tag = str(opp_data.get("tag") or "")
            opp_members = opp_data.get("members") or []
            sorted_opp = sorted(
                [m for m in opp_members if isinstance(m, dict)],
                key=lambda m: _as_int(m.get("mapPosition"), 999),
            )

            if not sorted_opp:
                lines.append(f"**{c['name']}** — opponent lineup unavailable.")
                continue

            th_counts: Dict[int, int] = {}
            th_total = 0
            for m in sorted_opp:
                th = _as_int(m.get("townhallLevel"), 0)
                if th > 0:
                    th_counts[th] = th_counts.get(th, 0) + 1
                    th_total += th

            avg_th = (th_total / len(sorted_opp)) if sorted_opp else 0.0
            th_spread = " • ".join([f"TH{th}x{cnt}" for th, cnt in sorted(th_counts.items(), reverse=True)])
            if not th_spread:
                th_spread = "N/A"

            max_th = max(th_counts.keys()) if th_counts else 0
            top_heavy_count = sum(cnt for th, cnt in th_counts.items() if th >= max(1, max_th - 1)) if max_th > 0 else 0
            top_heavy_ratio = (float(top_heavy_count) / float(len(sorted_opp))) if sorted_opp else 0.0

            defended_rows = 0
            conceded_star_sum = 0.0
            for m in sorted_opp:
                best_hit = m.get("bestOpponentAttack") or {}
                if isinstance(best_hit, dict) and best_hit:
                    defended_rows += 1
                    conceded_star_sum += float(_as_int(best_hit.get("stars"), 0))
            avg_conceded_stars = (conceded_star_sum / float(defended_rows)) if defended_rows > 0 else 0.0
            lineup_label, lineup_hint = _opponent_lineup_action_hint(top_heavy_ratio, avg_conceded_stars)

            lines.append(f"**{c['name']}** vs **{opp_name}** `{opp_tag}` — state: **{state}**")
            lines.append(f"Opponent summary: {len(sorted_opp)} bases • Avg TH {avg_th:.2f}")
            lines.append(f"TH spread: {th_spread}")
            lines.append(f"Top-heavy ratio: **{top_heavy_ratio * 100.0:.1f}%**")
            lines.append(f"Avg conceded stars (best-hit): **{avg_conceded_stars:.2f}**")
            lines.append(f"Lineup read: **{lineup_label}**")
            lines.append(f"Tactical plan: {lineup_hint}")

            for m in sorted_opp[:50]:
                pos = _as_int(m.get("mapPosition"), 0)
                th = _as_int(m.get("townhallLevel"), 0)
                tag_text = str(m.get("tag") or "")
                name = str(m.get("name") or "Unknown")

                attacks_used = len(m.get("attacks") or [])
                best_hit = m.get("bestOpponentAttack") or {}
                if isinstance(best_hit, dict) and best_hit:
                    conceded_stars = _as_int(best_hit.get("stars"), 0)
                    conceded_destr = float(best_hit.get("destructionPercentage", 0.0) or 0.0)
                    defense_note = f"best conceded {conceded_stars}★/{conceded_destr:.0f}%"
                else:
                    defense_note = "unhit"

                lines.append(
                    f"#{pos if pos > 0 else '?'} TH{th if th > 0 else '?'} **{name}** `{tag_text}` — "
                    f"attacks used {attacks_used}/2 • {defense_note}"
                )

            lines.append("")

        if not lines:
            return await ctx.send("No opponent lineup data available.")

        pages = build_paginated_embeds(
            title="🧭 Opponent Lineup",
            lines=lines,
            color=discord.Color.purple(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • Opponent Scout",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="warreminder", aliases=["wr"], description="Enable or disable automatic war reminders")
    @app_commands.describe(mode="on or off")
    @app_commands.choices(
        mode=[
            app_commands.Choice(name="Enable", value="on"),
            app_commands.Choice(name="Disable", value="off"),
        ]
    )
    async def warreminder(self, ctx: commands.Context, mode: str = "on"):
        val = mode.lower() if isinstance(mode, str) else mode.value.lower()
        is_guild_scope = ctx.guild is not None

        if val == "on":
            self.bot.war_reminder_enabled = True
            if is_guild_scope:
                save_guild_settings(ctx.guild.id, {"war_reminder_enabled": True}, merge=True)
            else:
                settings = load_settings()
                settings["war_reminder_enabled"] = True
                save_settings(settings)
            embed = discord.Embed(
                title="✅ War Reminders Enabled",
                description="Will send war reminders every 2 hours during active war.",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
        else:
            self.bot.war_reminder_enabled = False
            if is_guild_scope:
                save_guild_settings(ctx.guild.id, {"war_reminder_enabled": False}, merge=True)
            else:
                settings = load_settings()
                settings["war_reminder_enabled"] = False
                save_settings(settings)
            embed = discord.Embed(
                title="❌ War Reminders Disabled",
                description="Will not send automatic war reminder messages.",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )

        embed.set_footer(text="CC2 Clash Bot • War Reminder")
        await ctx.send(embed=embed, ephemeral=True)
        logger.info("War reminders %s", "enabled" if self.bot.war_reminder_enabled else "disabled")

    @commands.hybrid_command(name="warhistory", aliases=["wh"], description="Show recent war results history")
    @app_commands.describe(clan="(Optional) clan to check; default = all", limit="Number of wars (max 20)")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def warhistory(self, ctx: commands.Context, clan: Optional[str] = None, limit: int = 10):
        await ctx.defer()
        limit = max(1, min(limit, 20))
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for war history",
                    include_all=True,
                )
                await ctx.send("Select a clan for war history:", view=view)
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

        data = load_war_results()
        lines: List[str] = []

        def _current_win_streak(newest_first_rows: List[Dict[str, Any]]) -> int:
            streak = 0
            for row in newest_first_rows:
                if str(row.get("result", "")).lower() == "win":
                    streak += 1
                else:
                    break
            return streak

        for c in clans_to_check:
            clan_tag = str(c.get("tag") or "").upper().strip()
            rows_newest_first: List[Dict[str, Any]] = []
            source_label = "Live API (warlog)"
            api_error: Optional[str] = None

            try:
                encoded_tag = urllib.parse.quote(clan_tag, safe="")
                payload = await self.bot.coc_get(f"/clans/{encoded_tag}/warlog?limit={limit}")
                items = payload.get("items", []) if isinstance(payload, dict) else []
                if isinstance(items, list):
                    for item in items[:limit]:
                        if not isinstance(item, dict):
                            continue
                        clan_row = item.get("clan", {}) if isinstance(item.get("clan"), dict) else {}
                        opp_row = item.get("opponent", {}) if isinstance(item.get("opponent"), dict) else {}

                        clan_stars = int(clan_row.get("stars", 0) or 0)
                        opp_stars = int(opp_row.get("stars", 0) or 0)
                        clan_destruction = float(clan_row.get("destructionPercentage", 0.0) or 0.0)
                        opp_destruction = float(opp_row.get("destructionPercentage", 0.0) or 0.0)

                        result = str(item.get("result") or "").lower().strip()
                        if result not in {"win", "loss", "tie"}:
                            result = _determine_war_result(clan_stars, opp_stars, clan_destruction, opp_destruction)

                        rows_newest_first.append(
                            {
                                "result": result,
                                "opponent_name": str(opp_row.get("name") or "Unknown"),
                                "clan_stars": clan_stars,
                                "opponent_stars": opp_stars,
                                "clan_destruction": round(clan_destruction, 2),
                                "opponent_destruction": round(opp_destruction, 2),
                                "end_time": item.get("endTime"),
                            }
                        )
            except Exception as exc:
                api_error = str(exc)

            if not rows_newest_first:
                # Fallback to stored local snapshots when warlog is private/unavailable.
                stored_rows = list(data.get(clan_tag, []))[-limit:]
                rows_newest_first = list(reversed(stored_rows))
                source_label = "Stored snapshots"

            if not rows_newest_first:
                if api_error and ("accessDenied" in api_error or "403" in api_error or "private" in api_error.lower()):
                    lines.append(f"**{c['name']}** — war log is private and no local history is stored yet.")
                else:
                    lines.append(f"**{c['name']}** — no recorded war history yet.")
                continue

            wins = sum(1 for r in rows_newest_first if str(r.get("result", "")).lower() == "win")
            losses = sum(1 for r in rows_newest_first if str(r.get("result", "")).lower() == "loss")
            ties = sum(1 for r in rows_newest_first if str(r.get("result", "")).lower() == "tie")
            latest_streak = _current_win_streak(rows_newest_first)
            momentum_label, momentum_icon = _warhistory_momentum_band(wins, losses, ties, latest_streak)
            action_hint = _warhistory_action_hint(momentum_label, latest_streak, wins, losses)

            lines.append(f"**{c['name']}**")
            lines.append(f"Source: {source_label}")
            lines.append(f"Summary ({len(rows_newest_first)} wars): W {wins} • L {losses} • T {ties} • Current Win Streak {latest_streak}")
            lines.append(f"Momentum: {momentum_icon} **{momentum_label}**")
            lines.append(f"Suggested action: {action_hint}")
            for r in rows_newest_first:
                base_line = (
                    f"• {r.get('result', 'unknown').upper()} vs {r.get('opponent_name', 'Unknown')} "
                    f"({r.get('clan_stars', 0)}-{r.get('opponent_stars', 0)}★, "
                    f"{r.get('clan_destruction', 0)}%-{r.get('opponent_destruction', 0)}%)"
                )
                rating = str(r.get("leadership_rating", "") or "").strip().upper()
                note = str(r.get("leadership_note", "") or "").strip()
                if rating:
                    base_line += f" • Rated: **{rating}**"
                if note:
                    trimmed = note if len(note) <= 80 else (note[:77].rstrip() + "...")
                    base_line += f" • Note: {trimmed}"
                lines.append(base_line)

        pages = build_paginated_embeds(
            title="📜 War History",
            lines=lines,
            color=discord.Color.gold(),
            per_page=14,
            footer_prefix="CC2 Clash Bot • War History",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="cwlgroup", aliases=["cwl"], description="Show current CWL league group state")
    @app_commands.describe(clan="(Optional) clan to check; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def cwlgroup(self, ctx: commands.Context, clan: Optional[str] = None):
        await ctx.defer()
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for CWL group",
                    include_all=True,
                )
                await ctx.send("Select a clan for CWL group:", view=view)
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

        lines: List[str] = []
        for c in clans_to_check:
            group = await _fetch_cwl_group(self.bot, c["tag"])
            if not isinstance(group, dict):
                lines.append(f"**{c['name']}** — CWL group unavailable from API.")
                continue

            state = str(group.get("state") or "unknown").lower()
            season = str(group.get("season") or "unknown")
            rounds = group.get("rounds", []) if isinstance(group.get("rounds"), list) else []
            clans = group.get("clans", []) if isinstance(group.get("clans"), list) else []

            league_name = "Unknown"
            for rc in clans:
                if not isinstance(rc, dict):
                    continue
                if str(rc.get("tag") or "").upper() == str(c["tag"] or "").upper():
                    league_name = str((rc.get("warLeague") or {}).get("name") or league_name)
                    break

            ready_rounds = 0
            total_wars = 0
            for rr in rounds:
                tags = _round_tags(rr)
                total_wars += len(tags)
                if tags:
                    ready_rounds += 1

            lines.append(f"**{c['name']}** `{c['tag']}`")
            lines.append(f"Season: **{season}** • State: **{state}** • League: **{league_name}**")
            lines.append(f"Rounds ready: **{ready_rounds}/{len(rounds)}** • Published wars: **{total_wars}**")

            clan_names = [str(x.get("name") or x.get("tag") or "Unknown") for x in clans if isinstance(x, dict)]
            if clan_names:
                lines.append("Clans: " + ", ".join(clan_names[:8]))
            lines.append("")

        pages = build_paginated_embeds(
            title="🏆 CWL Group",
            lines=lines,
            color=discord.Color.blurple(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • CWL Group",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="cwlround", aliases=["cwlr"], description="Show CWL round wars for monitored clan(s)")
    @app_commands.describe(clan="(Optional) clan to check; default = all", round_no="Round number (1-based); default latest available")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def cwlround(self, ctx: commands.Context, clan: Optional[str] = None, round_no: Optional[int] = None):
        await ctx.defer()
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for CWL round",
                    include_all=True,
                )
                await ctx.send("Select a clan for CWL round:", view=view)
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

        lines: List[str] = []
        for c in clans_to_check:
            group = await _fetch_cwl_group(self.bot, c["tag"])
            if not isinstance(group, dict):
                lines.append(f"**{c['name']}** — CWL group unavailable from API.")
                continue

            rounds = group.get("rounds", []) if isinstance(group.get("rounds"), list) else []
            if not rounds:
                lines.append(f"**{c['name']}** — no CWL rounds published yet.")
                continue

            if round_no and int(round_no) > 0:
                idx = min(len(rounds) - 1, int(round_no) - 1)
            else:
                idx = -1
                for i in range(len(rounds) - 1, -1, -1):
                    if _round_tags(rounds[i]):
                        idx = i
                        break
                if idx < 0:
                    lines.append(f"**{c['name']}** — no CWL war tags available yet.")
                    continue

            target_round = rounds[idx]
            war_tags = _round_tags(target_round)
            if not war_tags:
                lines.append(f"**{c['name']}** — selected round has no war tags yet.")
                continue

            wars = await _fetch_cwl_wars(self.bot, war_tags)
            clan_tag_up = str(c["tag"] or "").upper()
            round_lines: List[str] = []
            for w in wars:
                c_row = w.get("clan") if isinstance(w.get("clan"), dict) else {}
                o_row = w.get("opponent") if isinstance(w.get("opponent"), dict) else {}
                c_tag = str((c_row or {}).get("tag") or "").upper()
                o_tag = str((o_row or {}).get("tag") or "").upper()

                if c_tag == clan_tag_up:
                    team = c_row
                    opp = o_row
                elif o_tag == clan_tag_up:
                    team = o_row
                    opp = c_row
                else:
                    continue

                team_stars = _as_int((team or {}).get("stars"), 0)
                opp_stars = _as_int((opp or {}).get("stars"), 0)
                team_destr = float((team or {}).get("destructionPercentage", 0.0) or 0.0)
                opp_destr = float((opp or {}).get("destructionPercentage", 0.0) or 0.0)
                result = _determine_war_result(team_stars, opp_stars, team_destr, opp_destr).upper()
                opp_name = str((opp or {}).get("name") or "Unknown")
                war_state = str(w.get("state") or "unknown")
                round_lines.append(
                    f"• {result} vs **{opp_name}** — {team_stars}-{opp_stars}★, {team_destr:.1f}-{opp_destr:.1f}% (state: {war_state})"
                )

            lines.append(f"**{c['name']}** — Round **{idx + 1}/{len(rounds)}**")
            if round_lines:
                lines.extend(round_lines)
            else:
                lines.append("• No wars found for this clan in selected round.")
            lines.append("")

        pages = build_paginated_embeds(
            title="⚔️ CWL Round Summary",
            lines=lines,
            color=discord.Color.dark_gold(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • CWL Round",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="attacklog", aliases=["atklog"], description="Show recent war attacks for a player")
    @app_commands.describe(tag="Player tag", limit="Number of recent attacks to show (max 20)")
    async def attacklog(self, ctx: commands.Context, tag: str, limit: int = 10):
        await ctx.defer()

        from utils.helpers import normalize_tag, is_valid_tag

        target_tag = normalize_tag(tag)
        if not is_valid_tag(target_tag):
            return await ctx.send(
                embed=build_error_embed(
                    "E-ATTACKLOG-TAG",
                    "Invalid player tag format.",
                    "Use format like `#2PQUE2J`.",
                    context=f"input={tag}",
                )
            )

        lim = max(1, min(limit, 20))
        data = load_war_attack_log()

        all_rows: List[Dict[str, Any]] = []
        for c in self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None):
            rows = data.get(c["tag"], []) if isinstance(data, dict) else []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("attacker_tag", "")).upper() == target_tag.upper():
                    all_rows.append(row)

        if not all_rows:
            return await ctx.send(
                embed=build_error_embed(
                    "E-ATTACKLOG-NODATA",
                    "No war attack history found for that player yet.",
                    "Wait until at least one war ends with logged attacks, then run again.",
                    context=f"tag={target_tag}",
                )
            )

        all_rows.sort(key=lambda r: str(r.get("timestamp", "")), reverse=True)
        rows = all_rows[:lim]

        player_name = str(rows[0].get("attacker_name") or target_tag)
        lines: List[str] = []
        for row in rows:
            stars = _as_int(row.get("stars"), 0)
            destruction = float(row.get("destruction", 0.0) or 0.0)
            eff = str(row.get("efficiency") or "N/A")
            opp = str(row.get("opponent_name") or "Unknown")
            defender = str(row.get("defender_tag") or "")
            atk_th = _as_int(row.get("attacker_th"), 0)
            def_th = _as_int(row.get("defender_th"), 0)
            ts = str(row.get("timestamp") or "")
            ts_short = ts.replace("T", " ")[:16] if ts else "unknown"
            lines.append(
                f"• {ts_short} vs **{opp}** — {stars}★/{destruction:.0f}% | TH{atk_th}->TH{def_th} `{defender}`"
            )
            lines.append(f"  ↳ {eff}")

        pages = build_paginated_embeds(
            title=f"🗡️ Attack Log — {player_name}",
            lines=lines,
            color=discord.Color.dark_teal(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • War Attack Log",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="warrating", aliases=["wrate"], description="Rate the latest completed war and add an optional leadership note")
    @app_commands.describe(
        outcome="Outcome label: win/loss/tie",
        note="Optional leadership note",
        clan="(Optional) clan to rate; default = all",
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def warrating(self, ctx: commands.Context, outcome: str, note: Optional[str] = None, clan: Optional[str] = None):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer(ephemeral=True)

        outcome_norm = str(outcome or "").strip().lower()
        if outcome_norm not in {"win", "loss", "tie"}:
            return await ctx.send(
                embed=build_error_embed(
                    "E-WARRATE-OUTCOME",
                    "Invalid outcome value.",
                    "Use one of: `win`, `loss`, `tie`.",
                    context=f"input={outcome}",
                ),
                ephemeral=True,
            )

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for war rating",
                    include_all=False,
                )
                await ctx.send("Select a clan for war rating:", view=view, ephemeral=True)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.", ephemeral=True)
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.", ephemeral=True)
                clan = view.selected_tag

        from cogs.admin import resolve_clans

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None or not clans_to_check:
            return await ctx.send(
                embed=build_error_embed(
                    "E-WARRATE-CLAN",
                    "Clan not found in monitored list.",
                    "Use a monitored clan tag from autocomplete or configure clan tracking first.",
                    context=f"clan={clan}",
                ),
                ephemeral=True,
            )
        if len(clans_to_check) != 1:
            return await ctx.send(
                embed=build_error_embed(
                    "E-WARRATE-MULTI",
                    "War rating supports one clan at a time.",
                    "Select a single clan before submitting a rating.",
                ),
                ephemeral=True,
            )

        target = clans_to_check[0]
        data = load_war_results()
        rows = data.get(target["tag"], []) if isinstance(data, dict) else []
        if not isinstance(rows, list) or not rows:
            return await ctx.send(
                embed=build_error_embed(
                    "E-WARRATE-NOWARS",
                    "No completed wars found to rate.",
                    "Wait for a war to end, then run `warrating` again.",
                    context=f"clan={target.get('tag')}",
                ),
                ephemeral=True,
            )

        latest = rows[-1]
        if not isinstance(latest, dict):
            return await ctx.send(
                embed=build_error_embed(
                    "E-WARRATE-ROW",
                    "Could not load latest war result row.",
                    "Run `warhistory` to verify data, then retry.",
                ),
                ephemeral=True,
            )

        latest["leadership_rating"] = outcome_norm.upper()
        latest["leadership_note"] = str((note or "").strip())[:240]
        latest["rated_by"] = str(getattr(ctx.author, "id", ""))
        latest["rated_at"] = datetime.now(timezone.utc).isoformat()

        rows[-1] = latest
        data[target["tag"]] = rows
        save_war_results(data)

        opp_name = str(latest.get("opponent_name") or "Unknown")
        emb = discord.Embed(
            title="📝 War Rating Saved",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
            description=f"Updated latest war for **{target['name']}** vs **{opp_name}**",
        )
        emb.add_field(name="Rating", value=f"**{outcome_norm.upper()}**", inline=True)
        emb.add_field(name="Result", value=f"{latest.get('clan_stars', 0)}-{latest.get('opponent_stars', 0)}★", inline=True)
        if latest.get("leadership_note"):
            emb.add_field(name="Note", value=str(latest.get("leadership_note")), inline=False)
        emb.set_footer(text="CC2 Clash Bot • War Rating")
        await ctx.send(embed=emb, ephemeral=True)

    @commands.hybrid_command(name="wartrends", aliases=["wt"], description="Show long-term war trend stats")
    @app_commands.describe(clan="(Optional) clan to check; default = all", wars="Wars window (10-30, default 20)")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def wartrends(self, ctx: commands.Context, clan: Optional[str] = None, wars: int = 20):
        await ctx.defer()
        wars = max(10, min(wars, 30))

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for war trends",
                    include_all=True,
                )
                await ctx.send("Select a clan for war trends:", view=view)
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

        data = load_war_results()
        lines: List[str] = []
        for c in clans_to_check:
            rows = list(data.get(c["tag"], []))[-wars:]
            if not rows:
                lines.append(f"**{c['name']}** — no recorded war history yet.")
                continue

            wins = sum(1 for r in rows if str(r.get("result", "")).lower() == "win")
            losses = sum(1 for r in rows if str(r.get("result", "")).lower() == "loss")
            ties = sum(1 for r in rows if str(r.get("result", "")).lower() == "tie")

            points = float(wins) + (0.5 * float(ties))
            win_rate = (points / max(1, len(rows))) * 100.0

            avg_clan_stars = sum(float(r.get("clan_stars", 0) or 0) for r in rows) / max(1, len(rows))
            avg_opp_stars = sum(float(r.get("opponent_stars", 0) or 0) for r in rows) / max(1, len(rows))
            avg_clan_destr = sum(float(r.get("clan_destruction", 0.0) or 0.0) for r in rows) / max(1, len(rows))
            avg_opp_destr = sum(float(r.get("opponent_destruction", 0.0) or 0.0) for r in rows) / max(1, len(rows))

            spark = _build_result_sparkline(rows)
            latest_streak = int((rows[-1] or {}).get("win_streak", 0) or 0)

            lines.append(f"**{c['name']}** — last {len(rows)} wars")
            lines.append(f"W/L/T: **{wins}/{losses}/{ties}** • Win Rate: **{win_rate:.1f}%** • Current Win Streak: **{latest_streak}**")
            lines.append(f"Avg Stars: **{avg_clan_stars:.2f}** for • **{avg_opp_stars:.2f}** against")
            lines.append(f"Avg Destruction: **{avg_clan_destr:.1f}%** for • **{avg_opp_destr:.1f}%** against")
            lines.append(f"Trend: `{spark}` (█ win, ▒ tie, ░ loss)")
            lines.append("")

        pages = build_paginated_embeds(
            title="📈 War Trends",
            lines=lines,
            color=discord.Color.teal(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • War Trends",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="misstreak", aliases=["ms2"], description="List players with consecutive missed war attacks")
    @app_commands.describe(clan="(Optional) clan to check; default = all", min_streak="Minimum consecutive missed wars (default 2)")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def misstreak(self, ctx: commands.Context, clan: Optional[str] = None, min_streak: int = 2):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer(ephemeral=True)
        min_streak = max(1, min(min_streak, 10))

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for miss streaks",
                    include_all=True,
                )
                await ctx.send("Select a clan for miss streak report:", view=view, ephemeral=True)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.", ephemeral=True)
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.", ephemeral=True)
                clan = view.selected_tag

        from cogs.admin import resolve_clans

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found in monitored list.", ephemeral=True)

        stats_data = load_war_player_stats()
        lines: List[str] = []
        for c in clans_to_check:
            clan_stats = stats_data.get(c["tag"], {}) if isinstance(stats_data, dict) else {}
            if not isinstance(clan_stats, dict) or not clan_stats:
                lines.append(f"**{c['name']}** — no war player stats yet.")
                continue

            offenders: List[Tuple[int, int, str, str, str]] = []
            for tag_key, row in clan_stats.items():
                if not isinstance(row, dict):
                    continue
                streak = int(row.get("missed_streak", 0) or 0)
                if streak < min_streak:
                    continue
                missed = int(row.get("missed_attacks", 0) or 0)
                name = str(row.get("name") or tag_key)
                linked_user_id = get_linked_user_for_tag(tag_key)
                mention = f"<@{linked_user_id}>" if linked_user_id else "Not linked"
                offenders.append((streak, missed, name, str(tag_key), mention))

            if not offenders:
                lines.append(f"**{c['name']}** — no players at {min_streak}+ consecutive missed wars.")
                continue

            offenders.sort(key=lambda x: (x[0], x[1]), reverse=True)
            lines.append(f"**{c['name']}** — {len(offenders)} player(s) at {min_streak}+ missed-war streak")
            for streak, missed, name, tag_key, mention in offenders:
                lines.append(
                    f"• **{name}** `{tag_key}` — streak **{streak}** war(s), total missed attacks **{missed}** • {mention}"
                )
            lines.append("")

        pages = build_paginated_embeds(
            title="🚨 Missed War Streaks",
            lines=lines,
            color=discord.Color.orange(),
            per_page=10,
            footer_prefix="CC2 Clash Bot • Miss Streak",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="warperformance", aliases=["wp"], description="Show individual all-time war performance stats")
    @app_commands.describe(tag="Player tag (optional, uses linked account if omitted)")
    async def warperformance(self, ctx: commands.Context, tag: Optional[str] = None):
        await ctx.defer(ephemeral=True)

        from storage import get_linked_tag_for_user
        from utils.helpers import normalize_tag, is_valid_tag

        if tag:
            target_tag = normalize_tag(tag)
            if not is_valid_tag(target_tag):
                return await ctx.send("❌ Invalid tag format. Use format like #2PQUE2J.", ephemeral=True)
        else:
            linked = get_linked_tag_for_user(ctx.author.id)
            if not linked:
                return await ctx.send("❌ No linked account. Use /link or pass a tag.", ephemeral=True)
            target_tag = normalize_tag(linked)

        stats_data = load_war_player_stats()
        found = None
        found_clan = None
        for c in self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None):
            row = (stats_data.get(c["tag"], {}) or {}).get(target_tag)
            if row:
                if found is None:
                    found = {
                        "name": row.get("name", target_tag),
                        "wars_participated": 0,
                        "attacks_used": 0,
                        "total_possible_attacks": 0,
                        "stars_earned": 0,
                        "destruction_sum": 0.0,
                        "missed_attacks": 0,
                        "missed_streak": 0,
                        "giant_slayer_3stars": 0,
                    }
                    found_clan = c

                found["name"] = row.get("name", found.get("name", target_tag))
                found["wars_participated"] += int(row.get("wars_participated", 0) or 0)
                found["attacks_used"] += int(row.get("attacks_used", 0) or 0)
                found["total_possible_attacks"] += int(row.get("total_possible_attacks", 0) or 0)
                found["stars_earned"] += int(row.get("stars_earned", 0) or 0)
                found["destruction_sum"] += float(row.get("destruction_sum", 0.0) or 0.0)
                found["missed_attacks"] += int(row.get("missed_attacks", 0) or 0)
                found["missed_streak"] = max(int(found.get("missed_streak", 0) or 0), int(row.get("missed_streak", 0) or 0))
                found["giant_slayer_3stars"] += int(row.get("giant_slayer_3stars", 0) or 0)

        if not found:
            return await ctx.send("No war performance data found for that player yet.", ephemeral=True)

        wars = int(found.get("wars_participated", 0) or 0)
        used = int(found.get("attacks_used", 0) or 0)
        possible = int(found.get("total_possible_attacks", 0) or 0)
        stars = int(found.get("stars_earned", 0) or 0)
        destruction_sum = float(found.get("destruction_sum", 0.0) or 0.0)
        missed = int(found.get("missed_attacks", 0) or 0)
        missed_streak = int(found.get("missed_streak", 0) or 0)
        giant_slayer = int(found.get("giant_slayer_3stars", 0) or 0)

        participation = (used / possible * 100.0) if possible > 0 else 0.0
        avg_stars = (stars / used) if used > 0 else 0.0
        avg_destruction = (destruction_sum / used) if used > 0 else 0.0
        perf_label, perf_icon = _war_performance_band(participation, avg_stars, missed_streak)
        action_hint = _war_performance_action_hint(participation, avg_stars, missed_streak)

        embed = discord.Embed(
            title=f"⚔️ War Performance — {found.get('name', target_tag)}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Tag", value=f"`{target_tag}`", inline=True)
        embed.add_field(name="Clan", value=(found_clan["name"] if found_clan else "Unknown"), inline=True)
        embed.add_field(name="Wars Participated", value=str(wars), inline=True)
        embed.add_field(name="Participation Rate", value=f"{participation:.1f}%", inline=True)
        embed.add_field(name="Avg Stars / Attack", value=f"{avg_stars:.2f}", inline=True)
        embed.add_field(name="Avg Destruction", value=f"{avg_destruction:.1f}%", inline=True)
        embed.add_field(name="Total Missed Attacks", value=str(missed), inline=True)
        embed.add_field(name="Current Missed Streak", value=str(missed_streak), inline=True)
        embed.add_field(name="Total Stars", value=str(stars), inline=True)
        embed.add_field(name="Giant Slayer 3★", value=str(giant_slayer), inline=True)
        embed.add_field(name="Performance Band", value=f"{perf_icon} **{perf_label}**", inline=True)
        embed.add_field(name="Coaching Next Step", value=action_hint, inline=False)
        embed.set_footer(text="CC2 Clash Bot • War Performance")
        await ctx.send(embed=embed, ephemeral=True)

    @commands.hybrid_command(name="rankings", aliases=["rank"], description="View trophy rankings for clans or players")
    @app_commands.describe(
        type="Rankings type: 'clan' or 'player'",
        location="Location ID or country code (default: Global)",
        limit="Number of results to show (default: 25)"
    )
    async def rankings(self, ctx: commands.Context, type: str = "clan", location: str = "32000000", limit: int = 25):
        await ctx.defer()
        
        location_id = 32000000  # Global default
        
        # Try to parse location as ID, or convert country code
        try:
            location_id = int(location)
        except ValueError:
            # Handle 2-letter country codes
            country_codes = {
                "us": 32000001, "gb": 32000002, "de": 32000003, "fr": 32000004,
                "ca": 32000005, "au": 32000006, "br": 32000007, "es": 32000008,
                "mx": 32000009, "in": 32000010, "jp": 32000011, "cn": 32000012,
            }
            location_id = country_codes.get(location.lower(), 32000000)
        
        type_lower = type.lower().strip()
        if type_lower not in ["clan", "player", "c", "p"]:
            return await ctx.send("❌ Type must be 'clan' or 'player'.", ephemeral=True)
        
        is_clan = type_lower in ["clan", "c"]
        limit = max(1, min(limit, 100))
        
        if is_clan:
            result = await _fetch_clan_rankings(self.bot, location_id)
            title_suffix = "Clan"
        else:
            result = await _fetch_player_rankings(self.bot, location_id)
            title_suffix = "Player"
        
        if not result:
            return await ctx.send(f"❌ Could not fetch {title_suffix.lower()} rankings for location {location_id}.", ephemeral=True)
        
        items = result.get("items", []) if isinstance(result, dict) else []
        if not items:
            return await ctx.send(f"No {title_suffix.lower()} rankings available for that location.", ephemeral=True)
        
        title = f"🏆 {title_suffix} Rankings (Location {location_id})"
        embeds = _format_rankings(title, items, limit)
        
        if isinstance(ctx, discord.Interaction):
            await ctx.send(embed=embeds[0], ephemeral=True)
            if len(embeds) > 1:
                for embed in embeds[1:]:
                    await ctx.channel.send(embed=embed)
        else:
            await send_paginated_embeds(ctx, embeds)

    @commands.hybrid_command(name="labels", aliases=["lbl"], description="Show available clan or player labels for filtering")
    @app_commands.describe(
        type="Label type: 'clan' or 'player'"
    )
    async def labels(self, ctx: commands.Context, type: str = "clan"):
        await ctx.defer()
        
        type_lower = type.lower().strip()
        if type_lower not in ["clan", "player", "c", "p"]:
            return await ctx.send("❌ Type must be 'clan' or 'player'.", ephemeral=True)
        
        label_type = "clans" if type_lower in ["clan", "c"] else "players"
        result = await _fetch_labels(self.bot, label_type)
        
        if not result:
            return await ctx.send(f"❌ Could not fetch {label_type} labels.", ephemeral=True)
        
        items = result.get("items", []) if isinstance(result, dict) else []
        if not items:
            return await ctx.send(f"No {label_type} labels available.", ephemeral=True)
        
        embed = _format_labels(items, label_type)
        await ctx.send(embed=embed, ephemeral=True)

    @commands.hybrid_command(name="locations", aliases=["loc"], description="Show all locations for rankings and filtering")
    @app_commands.describe(
        search="Optional search term to filter locations by name or ID"
    )
    async def locations(self, ctx: commands.Context, search: Optional[str] = None):
        await ctx.defer()
        
        items = await _fetch_locations(self.bot)
        
        if not items:
            return await ctx.send("❌ Could not fetch locations.", ephemeral=True)
        
        embeds = _format_locations_list(items, search)
        
        if isinstance(ctx, discord.Interaction):
            await ctx.send(embed=embeds[0], ephemeral=True)
            if len(embeds) > 1:
                for embed in embeds[1:]:
                    await ctx.channel.send(embed=embed)
        else:
            await send_paginated_embeds(ctx, embeds)


async def setup(bot):
    await bot.add_cog(WarCog(bot))
