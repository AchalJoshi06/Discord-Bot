"""Administration, linking, bases, roster, and clan management commands."""
import logging
import io
import csv
import os
import shutil
import asyncio
import zipfile
import re
import ast
import operator
import urllib.parse
import subprocess
import sys
from collections import Counter
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands, tasks

from config import (
    ANNOUNCE_CHANNEL_ID, TH_COLORS, BASE_TYPES,
    BASES_FILE, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID, INACTIVE_DAYS_THRESHOLD, AUDIT_CHANNEL_ID,
    BASE_LAYOUT_CHANNEL_ID, ATTACK_STRATEGY_CHANNEL_ID,
)
from storage import (
    load_links, save_links, load_clans, save_clans,
    get_linked_tag_for_user, load_strict_cache,
    get_linked_tags_for_user, get_primary_tag_for_user, set_primary_tag_for_user,
    load_bases, save_bases, load_member_activity, load_war_player_stats,
    load_attack_strategies, save_attack_strategies,
    load_war_results, load_raid_history,
    load_transfers_data,
    create_personal_reminder, load_personal_reminders, delete_personal_reminder,
    load_guild_clans, save_guild_clans,
    load_settings, save_settings, load_json,
)
from cache import api_cache, request_deduplicator
from embeds import build_info_embed, build_join_embed
from cogs.profiles import (
    clan_autocomplete, PlayerProfileView, _detect_minion_prince,
)
from utils.helpers import safe_send, has_leadership_role, has_admin_role, build_paginated_embeds, send_paginated_embeds, audit_log, ClanSelectView, build_error_embed
from calculations import calculate_weighted_rush_score, suggest_promotion, calculate_clan_health_score, extract_hero_levels

logger = logging.getLogger("cc2bot.cogs.admin")


_CALC_ALLOWED_BINOPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_CALC_ALLOWED_UNARY = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


# ── shared helpers ──

_POLL_ICON_SWORDS = "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/2694.png"
_POLL_ICON_SCROLL = "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/1f4dc.png"
_BASE_LINK_RE = re.compile(r"https?://link\.clashofclans\.com/\S*action=OpenLayout\S*", re.IGNORECASE)
_BASE_LAYOUT_ID_RE = re.compile(r"TH(\d{1,2}):([A-Za-z0-9_-]+):")
_GENERIC_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_TH_TEXT_RE = re.compile(r"\bTH\s*(\d{1,2})\b", re.IGNORECASE)

_CLAN_DASHBOARD_SECTIONS: Dict[str, Dict[str, str]] = {
    "overview": {
        "title": "Clan Overview",
        "description": "Clan summary and overview.",
    },
    "heroes_weight": {
        "title": "Heroes/War Weight",
        "description": "Heroes and Town Hall levels of the clan members.",
    },
    "discord_links": {
        "title": "Discord Links",
        "description": "Discord links of the clan members.",
    },
    "war_preferences": {
        "title": "War Preferences",
        "description": "War preferences of the clan members.",
    },
    "tags_roles": {
        "title": "Player Tags and Roles",
        "description": "Player tags and roles of the clan members.",
    },
    "trophies_leagues": {
        "title": "Trophies and Leagues",
        "description": "Trophies and leagues of the clan members.",
    },
    "last_joining": {
        "title": "Last Joining Date",
        "description": "Last join and leave/join count of the clan members.",
    },
    "player_progress": {
        "title": "Player Progress",
        "description": "Player progress of the clan members.",
    },
    "attacks_defenses": {
        "title": "Attacks & Defenses",
        "description": "Attacks and defenses of the clan members.",
    },
}


def _fmt_num(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except Exception:
        return "0"


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        raw = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _clan_th_distribution(member_list: List[Dict[str, Any]]) -> str:
    th_counter = Counter(int(m.get("townHallLevel", 0) or 0) for m in member_list)
    lines: List[str] = []
    for th, count in sorted(th_counter.items(), reverse=True):
        if th <= 0:
            continue
        lines.append(f"TH{th}: {_fmt_num(count)}")
    return "\n".join(lines[:10]) or "No Town Hall data."


def _clan_badge_url(clan_data: Dict[str, Any]) -> str:
    return str(
        ((clan_data.get("badgeUrls") or {}).get("large")
         or (clan_data.get("badgeUrls") or {}).get("medium")
         or (clan_data.get("badgeUrls") or {}).get("small")
         or "")
    )


def _short(text: Any, limit: int) -> str:
    raw = str(text or "")
    if len(raw) <= limit:
        return raw
    return raw[: max(1, limit - 3)].rstrip() + "..."


def _age_label(ts: Optional[datetime], now: Optional[datetime] = None) -> str:
    if not ts:
        return "---"
    now_dt = now or datetime.now(timezone.utc)
    delta = max(timedelta(0), now_dt - ts)
    days = delta.days
    if days >= 60:
        return f"{days // 30}mo"
    if days >= 1:
        return f"{days}d"
    hours = max(0, int(delta.total_seconds() // 3600))
    return f"{hours}h"


def _build_clan_overview_embed(clan_data: Dict[str, Any]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = ((clan_data.get("badgeUrls") or {}).get("large")
             or (clan_data.get("badgeUrls") or {}).get("medium")
             or (clan_data.get("badgeUrls") or {}).get("small"))
    member_list = clan_data.get("memberList") or []
    desc = str(clan_data.get("description") or "No clan description available.")

    emb = discord.Embed(
        title=f"🏰 {name} ({tag})",
        description=desc[:600],
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)

    members = int(clan_data.get("members", len(member_list)) or 0)
    max_members = int(clan_data.get("maxMembers", 50) or 50)
    war_wins = int(clan_data.get("warWins", 0) or 0)
    war_losses = int(clan_data.get("warLosses", 0) or 0)
    war_ties = int(clan_data.get("warTies", 0) or 0)

    emb.add_field(
        name="Core Stats",
        value=(
            f"🏰 Clan Level: {_fmt_num(clan_data.get('clanLevel', 0))}\n"
            f"👥 Members: {_fmt_num(members)}/{_fmt_num(max_members)}\n"
            f"🏆 Trophies: {_fmt_num(clan_data.get('clanPoints', 0))}\n"
            f"🛠 Builder: {_fmt_num(clan_data.get('clanBuilderBasePoints', clan_data.get('versusPoints', 0)))}\n"
            f"🪙 Capital: {_fmt_num(clan_data.get('clanCapitalPoints', 0))}"
        ),
        inline=True,
    )

    emb.add_field(
        name="War & League",
        value=(
            f"⚔️ War League: {((clan_data.get('warLeague') or {}).get('name') or 'Unranked')}\n"
            f"📖 War Log: {'Public' if clan_data.get('isWarLogPublic') else 'Private'}\n"
            f"✅ {war_wins} Won  ❌ {war_losses} Lost  🤝 {war_ties} Tied\n"
            f"🔥 Win Streak: {_fmt_num(clan_data.get('warWinStreak', 0))}\n"
            f"📅 War Frequency: {clan_data.get('warFrequency') or 'Unknown'}"
        ),
        inline=True,
    )

    total_donations = sum(int(m.get("donations", 0) or 0) for m in member_list)
    total_attack_wins = sum(int(m.get("attackWins", 0) or 0) for m in member_list)
    active_members = sum(
        1 for m in member_list
        if int(m.get("donations", 0) or 0) > 0
        or int(m.get("donationsReceived", 0) or 0) > 0
        or int(m.get("attackWins", 0) or 0) > 0
    )
    emb.add_field(
        name="Season Snapshot",
        value=(
            f"📈 Active Members: {_fmt_num(active_members)}\n"
            f"⚔️ Total Attacks: {_fmt_num(total_attack_wins)}\n"
            f"📦 Total Donations: {_fmt_num(total_donations)}"
        ),
        inline=False,
    )

    emb.add_field(name="Town Halls", value=_clan_th_distribution(member_list), inline=False)
    emb.set_footer(text="Clan Dashboard • Section: Clan Overview")
    return emb


def _build_heroes_weight_embed(
    clan_data: Dict[str, Any],
    player_map: Dict[str, Dict[str, Any]],
    page: int = 0,
    sort_mode: str = "power",
) -> Tuple[discord.Embed, int]:
    """Build paginated heroes embed. Returns (embed, total_pages).
    
    sort_mode: "power" (high to low), "th_desc" (high to low), "th_asc" (low to high)
    """
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []
    rows: List[Dict[str, Any]] = []
    th_totals: Dict[int, List[int]] = {}

    for m in member_list:
        ptag = str(m.get("tag") or "")
        if not ptag:
            continue
        pdata = player_map.get(ptag) or {}
        hero_levels = extract_hero_levels(pdata) if pdata else {}
        hero_sum = sum(int(v or 0) for v in hero_levels.values())
        th = int((pdata.get("townHallLevel") or m.get("townHallLevel") or 0) or 0)
        if th > 0:
            th_totals.setdefault(th, []).append(hero_sum)
        rows.append({
            "name": str(m.get("name") or ptag),
            "tag": ptag,
            "th": th,
            "hero_sum": hero_sum,
            "heroes": {
                "BK": int(hero_levels.get("BK", 0) or 0),
                "AQ": int(hero_levels.get("AQ", 0) or 0),
                "GW": int(hero_levels.get("GW", 0) or 0),
                "RC": int(hero_levels.get("RC", 0) or 0),
                "MP": int(hero_levels.get("MP", 0) or 0),
            },
        })

    # Apply sorting based on sort_mode
    if sort_mode == "power":
        rows.sort(key=lambda r: (r["hero_sum"], r["th"]), reverse=True)
    elif sort_mode == "th_desc":
        rows.sort(key=lambda r: (r["th"], r["hero_sum"]), reverse=True)
    elif sort_mode == "th_asc":
        rows.sort(key=lambda r: (r["th"], r["hero_sum"]), reverse=False)
    else:
        rows.sort(key=lambda r: (r["hero_sum"], r["th"]), reverse=True)

    # Pagination: 20 members per page
    page_size = 20
    total_pages = max(1, (len(rows) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_rows = rows[start_idx:end_idx]

    # Build table
    table_lines: List[str] = ["TH  BK  AQ  GW  RC  MP  NAME"]
    for idx, r in enumerate(page_rows, start=start_idx + 1):
        name_txt = str(r.get("name") or "Unknown")
        if len(name_txt) > 15:
            name_txt = name_txt[:12].rstrip() + "..."
        table_lines.append(
            f"{int(r['th']):>2}  {int(r['heroes']['BK']):>2}  {int(r['heroes']['AQ']):>2}  "
            f"{int(r['heroes']['GW']):>2}  {int(r['heroes']['RC']):>2}  {int(r['heroes']['MP']):>2}  {name_txt}"
        )

    th_lines: List[str] = []
    for th in sorted(th_totals.keys(), reverse=True):
        values = th_totals[th]
        avg = (sum(values) / max(1, len(values))) if values else 0.0
        th_lines.append(f"TH{th}: avg hero power {avg:.1f} ({len(values)} players)")

    sort_label = {
        "power": "by Hero Power",
        "th_desc": "by TH (High→Low)",
        "th_asc": "by TH (Low→High)",
    }.get(sort_mode, "by Hero Power")

    emb = discord.Embed(
        title=f"🛡️ {name} ({tag}) • Heroes/War Weight",
        description=f"Hero list sorted {sort_label}. Page {page + 1}/{total_pages} ({len(rows)} total members)",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(name="Hero Levels", value=f"```\n{chr(10).join(table_lines)}\n```" if len(table_lines) > 1 else "No hero data available.", inline=False)
    if page == 0:  # Only show TH summary on first page
        emb.add_field(name="By Town Hall", value="\n".join(th_lines[:12]) or "No TH averages available.", inline=False)
    emb.set_footer(text=f"Clan Dashboard • Section: Heroes/War Weight • Page {page + 1}/{total_pages}")
    return emb, total_pages


def _build_discord_links_embed(
    clan_data: Dict[str, Any],
    links_data: Dict[str, str],
    guild: Optional[discord.Guild] = None,
    page: int = 0,
    sort_mode: str = "th_desc",
) -> Tuple[discord.Embed, int]:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []

    in_server_rows: List[Dict[str, Any]] = []
    not_in_server_rows: List[Dict[str, Any]] = []
    not_linked_rows: List[Dict[str, Any]] = []

    def _truncate(txt: str, limit: int) -> str:
        if len(txt) <= limit:
            return txt
        return txt[: max(1, limit - 3)].rstrip() + "..."

    def _fmt_row(row: Dict[str, Any]) -> str:
        return f"{int(row.get('th', 0)):>2}  {_truncate(str(row.get('name') or 'Unknown'), 14):<14}  {_truncate(str(row.get('rhs') or ''), 14)}"

    for m in member_list:
        ptag = str(m.get("tag") or "")
        if not ptag:
            continue

        th = int(m.get("townHallLevel", 0) or 0)
        pname = str(m.get("name") or ptag)
        uid = str(links_data.get(ptag) or "").strip()
        if uid:
            guild_member = None
            if guild is not None:
                try:
                    guild_member = guild.get_member(int(uid))
                except Exception:
                    guild_member = None

            if guild_member is not None:
                in_server_rows.append(
                    {
                        "th": th,
                        "name": pname,
                        "rhs": str(getattr(guild_member, "display_name", "") or getattr(guild_member, "name", "") or uid),
                    }
                )
            else:
                not_in_server_rows.append(
                    {
                        "th": th,
                        "name": pname,
                        "rhs": f"<@{uid}>",
                    }
                )
        else:
            not_linked_rows.append(
                {
                    "th": th,
                    "name": pname,
                    "rhs": ptag,
                }
            )

    total_members = len(member_list)
    linked_count = len(in_server_rows) + len(not_in_server_rows)
    unlinked = len(not_linked_rows)
    coverage = (linked_count / total_members * 100.0) if total_members else 0.0

    reverse_sort = sort_mode != "th_asc"
    in_server_rows.sort(key=lambda r: (int(r.get("th", 0)), str(r.get("name", "")).lower()), reverse=reverse_sort)
    not_in_server_rows.sort(key=lambda r: (int(r.get("th", 0)), str(r.get("name", "")).lower()), reverse=reverse_sort)
    not_linked_rows.sort(key=lambda r: (int(r.get("th", 0)), str(r.get("name", "")).lower()), reverse=reverse_sort)

    page_size = 20
    total_pages = max(
        1,
        (len(in_server_rows) + page_size - 1) // page_size,
        (len(not_in_server_rows) + page_size - 1) // page_size,
        (len(not_linked_rows) + page_size - 1) // page_size,
    )
    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    end = start + page_size

    in_server_page = in_server_rows[start:end]
    not_in_server_page = not_in_server_rows[start:end]
    not_linked_page = not_linked_rows[start:end]

    in_server_lines = [_fmt_row(r) for r in in_server_page]
    not_in_server_lines = [_fmt_row(r) for r in not_in_server_page]
    not_linked_lines = [_fmt_row(r) for r in not_linked_page]

    sort_label = "TH High->Low" if reverse_sort else "TH Low->High"

    emb = discord.Embed(
        title=f"🔗 {name} ({tag}) • Discord Links",
        description=f"Roster link status with server presence. Page {page + 1}/{total_pages} • Sort: {sort_label}",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(
        name="Link Coverage",
        value=(
            f"✅ Linked: {_fmt_num(linked_count)}\n"
            f"❌ Unlinked: {_fmt_num(unlinked)}\n"
            f"📊 Coverage: {coverage:.1f}%"
        ),
        inline=False,
    )

    emb.add_field(
        name=f"Players In Server: {_fmt_num(len(in_server_rows))}",
        value=(f"```\nTH  NAME            DISCORD\n{chr(10).join(in_server_lines)}\n```" if in_server_lines else "No linked members in this server on this page."),
        inline=False,
    )
    emb.add_field(
        name=f"Players Not In Server: {_fmt_num(len(not_in_server_rows))}",
        value=(f"```\nTH  NAME            LINKED\n{chr(10).join(not_in_server_lines)}\n```" if not_in_server_lines else "No linked members outside this server on this page."),
        inline=False,
    )
    emb.add_field(
        name=f"Players Not Linked: {_fmt_num(len(not_linked_rows))}",
        value=(f"```\nTH  NAME            TAG\n{chr(10).join(not_linked_lines)}\n```" if not_linked_lines else "No unlinked members on this page."),
        inline=False,
    )
    emb.set_footer(text=f"Clan Dashboard • Section: Discord Links • Page {page + 1}/{total_pages}")
    return emb, total_pages


def _build_war_preferences_embed(clan_data: Dict[str, Any], player_map: Dict[str, Dict[str, Any]]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []

    pref_counts = {"in": 0, "out": 0, "unknown": 0}
    in_rows: List[str] = []
    out_rows: List[str] = []
    now = datetime.now(timezone.utc)

    for m in member_list:
        ptag = str(m.get("tag") or "")
        pdata = player_map.get(ptag) or {}
        pref = str(pdata.get("warPreference") or "unknown").lower()
        if pref not in pref_counts:
            pref = "unknown"
        pref_counts[pref] += 1
        th = int((pdata.get("townHallLevel") or m.get("townHallLevel") or 0) or 0)
        last_seen = _parse_iso_dt(pdata.get("lastSeen") or pdata.get("lastSeenAt") or pdata.get("lastProgressSeen") or pdata.get("lastOnline"))
        age = _age_label(last_seen, now)
        row = f"{th:>2}  {age:>4}  {_short(m.get('name') or ptag, 18)}"
        if pref == "in":
            in_rows.append(row)
        if pref == "out":
            out_rows.append(row)

    in_rows = sorted(in_rows, reverse=True)
    out_rows = sorted(out_rows, reverse=True)

    emb = discord.Embed(
        title=f"⚔️ {name} ({tag}) • War Preferences",
        description="War Preferences and Last Opted In/Out",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(
        name=f"Opted-In - {_fmt_num(pref_counts['in'])}",
        value=(f"```\nTH  AGE  NAME\n{chr(10).join(in_rows[:28])}\n```" if in_rows else "No opted-in members."),
        inline=False,
    )
    emb.add_field(
        name=f"Opted-Out - {_fmt_num(pref_counts['out'])}",
        value=(f"```\nTH  AGE  NAME\n{chr(10).join(out_rows[:20])}\n```" if out_rows else "No opted-out members."),
        inline=False,
    )
    emb.add_field(
        name="Summary",
        value=(
            f"✅ In: {_fmt_num(pref_counts['in'])}\n"
            f"🚫 Out: {_fmt_num(pref_counts['out'])}\n"
            f"❓ Unknown: {_fmt_num(pref_counts['unknown'])}"
        ),
        inline=False,
    )
    emb.set_footer(text="Clan Dashboard • Section: War Preferences")
    return emb


def _build_tags_roles_embed(clan_data: Dict[str, Any]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []

    role_counter = Counter(str(m.get("role") or "member") for m in member_list)
    role_name = {
        "leader": "Leader",
        "coLeader": "Co-Leader",
        "admin": "Elder",
        "member": "Member",
    }

    role_lines: List[str] = []
    for rk in ("leader", "coLeader", "admin", "member"):
        role_lines.append(f"{role_name[rk]}: {_fmt_num(role_counter.get(rk, 0))}")

    sorted_members = sorted(
        member_list,
        key=lambda m: (
            3 if str(m.get("role") or "") == "leader" else
            2 if str(m.get("role") or "") == "coLeader" else
            1 if str(m.get("role") or "") == "admin" else 0,
            int(m.get("townHallLevel", 0) or 0),
            int(m.get("trophies", 0) or 0),
        ),
        reverse=True,
    )
    role_short = {
        "leader": "Lead",
        "coLeader": "Co",
        "admin": "Eld",
        "member": "Mem",
    }
    member_lines = [
        f"{role_short.get(str(m.get('role') or 'member'), 'Mem'):<4}  {_short(m.get('tag') or '', 10):<10}  {_short(m.get('name') or m.get('tag'), 18)}"
        for m in sorted_members
    ]

    emb = discord.Embed(
        title=f"👥 {name} ({tag}) • Player Tags and Roles",
        description=f"Role table for clan roster. Total {len(member_list)}/{int(clan_data.get('maxMembers', 50) or 50)}",
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(name="Role Counts", value="\n".join(role_lines), inline=False)
    emb.add_field(
        name="Roster",
        value=(f"```\nROLE  TAG         NAME\n{chr(10).join(member_lines[:43])}\n```" if member_lines else "No members found."),
        inline=False,
    )
    emb.set_footer(text="Clan Dashboard • Section: Player Tags and Roles")
    return emb


def _build_trophies_leagues_embed(clan_data: Dict[str, Any]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []

    sorted_members = sorted(member_list, key=lambda m: int(m.get("trophies", 0) or 0), reverse=True)
    top_lines = [
        f"{idx + 1:>2}  {int(m.get('trophies', 0) or 0):>5}  {_short(((m.get('league') or {}).get('name') or 'Unranked'), 10):<10}  {_short(m.get('name') or m.get('tag'), 16)}"
        for idx, m in enumerate(sorted_members[:43])
    ]

    avg_trophies = (
        sum(int(m.get("trophies", 0) or 0) for m in member_list) / max(1, len(member_list))
        if member_list else 0.0
    )
    avg_builder = (
        sum(int(m.get("versusTrophies", 0) or 0) for m in member_list) / max(1, len(member_list))
        if member_list else 0.0
    )

    emb = discord.Embed(
        title=f"🏆 {name} ({tag}) • Trophies and Leagues",
        description=f"Ranked trophy board • Total {len(member_list)}/{int(clan_data.get('maxMembers', 50) or 50)}",
        color=discord.Color.purple(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(name="Averages", value=f"Home Avg: {avg_trophies:.1f}\nBuilder Avg: {avg_builder:.1f}", inline=False)
    emb.add_field(
        name="Table",
        value=(f"```\n#   TROPHY  LEAGUE      NAME\n{chr(10).join(top_lines)}\n```" if top_lines else "No trophy data available."),
        inline=False,
    )
    emb.set_footer(text="Clan Dashboard • Section: Trophies and Leagues")
    return emb


def _build_last_joining_embed(clan_data: Dict[str, Any], transfer_events: List[Dict[str, Any]]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []

    current_tags = {str(m.get("tag") or "") for m in member_list if m.get("tag")}
    latest_join_for_tag: Dict[str, datetime] = {}
    join_counts: Dict[str, int] = {}
    leave_counts: Dict[str, int] = {}

    for ev in transfer_events:
        if not isinstance(ev, dict):
            continue
        ptag = str(ev.get("player_tag") or "")
        if not ptag:
            continue
        to_tag = str(((ev.get("to") or {}).get("tag") or "")).upper()
        from_tag = str(((ev.get("from") or {}).get("tag") or "")).upper()
        ts = _parse_iso_dt(ev.get("timestamp"))
        if to_tag == str(tag).upper():
            join_counts[ptag] = join_counts.get(ptag, 0) + 1
            if ts and (ptag not in latest_join_for_tag or ts > latest_join_for_tag[ptag]):
                latest_join_for_tag[ptag] = ts
        if from_tag == str(tag).upper():
            leave_counts[ptag] = leave_counts.get(ptag, 0) + 1

    rows: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for m in member_list:
        ptag = str(m.get("tag") or "")
        if not ptag:
            continue
        dt = latest_join_for_tag.get(ptag)
        joined = _age_label(dt, now)
        jcnt = join_counts.get(ptag, 0)
        lcnt = leave_counts.get(ptag, 0)
        rows.append(
            {
                "th": int(m.get("townHallLevel", 0) or 0),
                "in": jcnt,
                "out": lcnt,
                "name": _short(m.get("name") or ptag, 14),
                "last": joined,
                "has_date": bool(dt),
            }
        )

    rows.sort(key=lambda r: (0 if r["has_date"] else 1, str(r["last"])))
    known_join_dates = sum(1 for t in current_tags if t in latest_join_for_tag)

    emb = discord.Embed(
        title=f"🕒 {name} ({tag}) • Last Joining Date",
        description="Leave/Join trend table for current members.",
        color=discord.Color.dark_teal(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(
        name="Coverage",
        value=f"Known join dates: {_fmt_num(known_join_dates)}/{_fmt_num(len(current_tags))}",
        inline=False,
    )
    table_lines = [
        f"{int(r['th']):>2}  {int(r['in']):>2}  {int(r['out']):>3}  {str(r['name']):<14}  {str(r['last']):>4}"
        for r in rows[:43]
    ]
    emb.add_field(
        name="Members",
        value=(f"```\nTH  IN  OUT  NAME            LAST\n{chr(10).join(table_lines)}\n```" if table_lines else "No transfer history available."),
        inline=False,
    )
    emb.set_footer(text="Clan Dashboard • Section: Last Joining Date")
    return emb


def _build_player_progress_embed(clan_data: Dict[str, Any], member_activity: Dict[str, Dict[str, Any]]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []
    now = datetime.now(timezone.utc)

    stale_rows: List[Dict[str, Any]] = []
    progressing = 0
    no_data = 0
    progress_rows: List[str] = []
    for m in member_list:
        ptag = str(m.get("tag") or "")
        row = member_activity.get(ptag) if isinstance(member_activity, dict) else None
        if not isinstance(row, dict):
            no_data += 1
            continue

        seen = _parse_iso_dt(row.get("last_progress_seen") or row.get("last_seen"))
        if not seen:
            no_data += 1
            continue
        days = (now - seen).days
        if days <= 3:
            progressing += 1
        stale_rows.append({"name": m.get("name") or ptag, "days": days, "date": seen.strftime("%Y-%m-%d")})
        hero = int(row.get("heroes_upgraded_30d", 0) or row.get("hero_upgrades", 0) or row.get("hero", 0) or 0)
        pet = int(row.get("pets_upgraded_30d", 0) or row.get("pet_upgrades", 0) or row.get("pet", 0) or 0)
        troop = int(row.get("troops_upgraded_30d", 0) or row.get("troop_upgrades", 0) or row.get("troop", 0) or 0)
        spell = int(row.get("spells_upgraded_30d", 0) or row.get("spell_upgrades", 0) or row.get("spell", 0) or 0)
        progress_rows.append(
            f"{hero:>3}  {pet:>3}  {troop:>3}  {spell:>3}  {_short(m.get('name') or ptag, 16)}"
        )

    stale_rows.sort(key=lambda r: r["days"], reverse=True)
    lines = [f"• {r['name']}: {r['days']}d since progress ({r['date']})" for r in stale_rows[:15]]

    emb = discord.Embed(
        title=f"📈 {name} ({tag}) • Player Progress",
        description="Player Progress (Hero, Pet, Troop, Spell)",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(
        name="Progress Summary",
        value=(
            f"✅ Active (<=3d): {_fmt_num(progressing)}\n"
            f"⏳ Tracked but stale: {_fmt_num(max(0, len(stale_rows) - progressing))}\n"
            f"❓ No tracking data: {_fmt_num(no_data)}"
        ),
        inline=False,
    )
    emb.add_field(
        name="Progress Table",
        value=(f"```\nHRO  PET  TRP  SPL  NAME\n{chr(10).join(progress_rows[:43])}\n```" if progress_rows else "No progress rows available."),
        inline=False,
    )
    emb.add_field(name="Needs Attention", value="\n".join(lines) or "No stale progress found.", inline=False)
    emb.set_footer(text="Clan Dashboard • Section: Player Progress")
    return emb


def _build_attacks_defenses_embed(clan_data: Dict[str, Any]) -> discord.Embed:
    name = clan_data.get("name") or "Unknown Clan"
    tag = clan_data.get("tag") or "#UNKNOWN"
    badge = _clan_badge_url(clan_data)
    member_list = clan_data.get("memberList") or []

    sorted_rows = sorted(
        member_list,
        key=lambda m: (
            int(m.get("attackWins", 0) or 0),
            int(m.get("defenseWins", 0) or 0),
            str(m.get("name") or m.get("tag") or "").lower(),
        ),
        reverse=True,
    )
    table_lines = [
        f"{idx + 1:>2}  {int(m.get('attackWins', 0) or 0):>3}  {int(m.get('defenseWins', 0) or 0):>3}  {_short(m.get('name') or m.get('tag'), 16)}"
        for idx, m in enumerate(sorted_rows[:43])
    ]

    total_attacks = sum(int(m.get("attackWins", 0) or 0) for m in member_list)
    total_defenses = sum(int(m.get("defenseWins", 0) or 0) for m in member_list)

    emb = discord.Embed(
        title=f"🗡️ {name} ({tag}) • Attacks & Defenses",
        description="Season Attack/Defense table",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    if badge:
        emb.set_thumbnail(url=badge)
    emb.add_field(
        name="Totals",
        value=f"⚔️ Attack Wins: {_fmt_num(total_attacks)}\n🛡️ Defense Wins: {_fmt_num(total_defenses)}",
        inline=False,
    )
    emb.add_field(
        name="Table",
        value=(f"```\n#   ATK  DEF  NAME\n{chr(10).join(table_lines)}\n```" if table_lines else "No attack/defense data."),
        inline=False,
    )
    emb.set_footer(text="Clan Dashboard • Section: Attacks & Defenses")
    return emb


class ClanDashboardSectionSelect(discord.ui.Select):
    def __init__(self, selected_key: str):
        options: List[discord.SelectOption] = []
        for key, cfg in _CLAN_DASHBOARD_SECTIONS.items():
            options.append(
                discord.SelectOption(
                    label=cfg["title"][:100],
                    value=key,
                    description=cfg["description"][:100],
                    default=(key == selected_key),
                )
            )
        super().__init__(
            placeholder="Clan Overview",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=2,
        )

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        await self.view.switch_section(interaction, self.values[0])


class ClanDashboardClanSelect(discord.ui.Select):
    def __init__(self, clans: List[Dict[str, str]], selected_tag: str):
        options: List[discord.SelectOption] = []
        seen: set[str] = set()
        for c in clans:
            if not isinstance(c, dict):
                continue
            tag = str(c.get("tag") or "").upper().strip()
            if not tag or tag in seen:
                continue
            seen.add(tag)
            name = str(c.get("name") or tag)
            options.append(
                discord.SelectOption(
                    label=f"{name} ({tag})"[:100],
                    value=tag,
                    default=(tag == selected_tag),
                )
            )
        if not options:
            options.append(discord.SelectOption(label="No clans", value="NONE", default=True))
        super().__init__(
            placeholder="Select clan",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=3,
        )

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        await self.view.switch_clan(interaction, self.values[0])


class ClanDashboardRefreshButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Refresh", emoji="🔄", style=discord.ButtonStyle.secondary, row=1)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        await self.view.refresh_current(interaction)


class ClanDashboardHeroesPrevButton(discord.ui.Button):
    def __init__(self, disabled: bool = False):
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.primary, disabled=disabled, row=0)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        if self.view.selected_section == "heroes_weight":
            self.view.heroes_page = max(0, self.view.heroes_page - 1)
            await self.view.refresh_current(interaction)


class ClanDashboardHeroesNextButton(discord.ui.Button):
    def __init__(self, disabled: bool = False):
        super().__init__(label="Next ▶", style=discord.ButtonStyle.primary, disabled=disabled, row=0)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        if self.view.selected_section == "heroes_weight":
            self.view.heroes_page += 1
            await self.view.refresh_current(interaction)


class ClanDashboardHeroesSortButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Sort: Power", emoji="📊", style=discord.ButtonStyle.secondary, row=0)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        if self.view.selected_section == "heroes_weight":
            modes = ["power", "th_desc", "th_asc"]
            current_idx = modes.index(self.view.heroes_sort) if self.view.heroes_sort in modes else 0
            self.view.heroes_sort = modes[(current_idx + 1) % len(modes)]
            self.view.heroes_page = 0  # Reset to first page on sort change
            await self.view.refresh_current(interaction)


class ClanDashboardLinksPrevButton(discord.ui.Button):
    def __init__(self, disabled: bool = False):
        super().__init__(label="◀ Prev", style=discord.ButtonStyle.primary, disabled=disabled, row=0)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        if self.view.selected_section == "discord_links":
            self.view.discord_links_page = max(0, self.view.discord_links_page - 1)
            await self.view.refresh_current(interaction)


class ClanDashboardLinksNextButton(discord.ui.Button):
    def __init__(self, disabled: bool = False):
        super().__init__(label="Next ▶", style=discord.ButtonStyle.primary, disabled=disabled, row=0)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        if self.view.selected_section == "discord_links":
            self.view.discord_links_page += 1
            await self.view.refresh_current(interaction)


class ClanDashboardLinksSortButton(discord.ui.Button):
    def __init__(self, sort_mode: str):
        label = "Sort: TH ↓" if sort_mode != "th_asc" else "Sort: TH ↑"
        super().__init__(label=label, emoji="🔃", style=discord.ButtonStyle.secondary, row=0)

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, ClanDashboardView):
            return
        if self.view.selected_section == "discord_links":
            self.view.discord_links_sort = "th_asc" if self.view.discord_links_sort != "th_asc" else "th_desc"
            self.view.discord_links_page = 0
            await self.view.refresh_current(interaction)


class ClanDashboardView(discord.ui.View):
    def __init__(
        self,
        cog: "AdminCog",
        author_id: Optional[int],
        guild_id: Optional[int],
        clans: List[Dict[str, str]],
        selected_tag: str,
        selected_section: str = "overview",
    ):
        super().__init__(timeout=300)
        self.cog = cog
        self.author_id = author_id
        self.guild_id = guild_id
        self.clans = clans
        self.selected_tag = selected_tag
        self.selected_section = selected_section if selected_section in _CLAN_DASHBOARD_SECTIONS else "overview"
        self._clan_cache: Dict[str, Dict[str, Any]] = {}
        self._player_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.heroes_page: int = 0
        self.heroes_sort: str = "power"
        self._heroes_total_pages: int = 1
        self.discord_links_page: int = 0
        self.discord_links_sort: str = "th_desc"
        self._discord_links_total_pages: int = 1
        self._refresh_items()

    def _refresh_items(self) -> None:
        self.clear_items()
        self.add_item(ClanDashboardRefreshButton())

        badge_url = None
        cached = self._clan_cache.get(self.selected_tag)
        if isinstance(cached, dict):
            badge_url = ((cached.get("badgeUrls") or {}).get("large")
                         or (cached.get("badgeUrls") or {}).get("medium")
                         or (cached.get("badgeUrls") or {}).get("small"))
        if isinstance(badge_url, str) and badge_url:
            self.add_item(discord.ui.Button(label="Clan Badge", style=discord.ButtonStyle.link, url=badge_url, row=1))

        # Add heroes pagination buttons if on heroes section
        if self.selected_section == "heroes_weight":
            self.add_item(ClanDashboardHeroesPrevButton(disabled=self.heroes_page == 0))
            self.add_item(ClanDashboardHeroesSortButton())
            self.add_item(ClanDashboardHeroesNextButton(disabled=self.heroes_page >= self._heroes_total_pages - 1))
        elif self.selected_section == "discord_links":
            self.add_item(ClanDashboardLinksPrevButton(disabled=self.discord_links_page == 0))
            self.add_item(ClanDashboardLinksSortButton(self.discord_links_sort))
            self.add_item(ClanDashboardLinksNextButton(disabled=self.discord_links_page >= self._discord_links_total_pages - 1))

        self.add_item(ClanDashboardSectionSelect(self.selected_section))
        self.add_item(ClanDashboardClanSelect(self.clans, self.selected_tag))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is None or interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Only the command invoker can use this clan menu.", ephemeral=True)
        return False

    async def _get_clan_payload(self, clan_tag: str) -> Optional[Dict[str, Any]]:
        key = str(clan_tag or "").upper().strip()
        if key in self._clan_cache:
            return self._clan_cache[key]
        payload = await self.cog._fetch_clan_payload(key)
        if payload:
            self._clan_cache[key] = payload
        return payload

    async def _get_player_map(self, clan_tag: str, clan_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        key = str(clan_tag or "").upper().strip()
        if key in self._player_cache:
            return self._player_cache[key]
        tags = [str(m.get("tag") or "") for m in (clan_payload.get("memberList") or []) if m.get("tag")]
        players = await self.cog.bot.fetch_players(tags, concurrency=10) if tags else {}
        clean_map: Dict[str, Dict[str, Any]] = {}
        for tag, pdata in (players or {}).items():
            if isinstance(pdata, dict):
                clean_map[str(tag)] = pdata
        self._player_cache[key] = clean_map
        return clean_map

    async def build_embed(self) -> discord.Embed:
        clan_payload = await self._get_clan_payload(self.selected_tag)
        if not clan_payload:
            return build_error_embed("Could not fetch clan data from the API.")

        section_key = self.selected_section
        player_map: Dict[str, Dict[str, Any]] = {}
        if section_key in {"heroes_weight", "war_preferences"}:
            player_map = await self._get_player_map(self.selected_tag, clan_payload)

        if section_key == "overview":
            return _build_clan_overview_embed(clan_payload)
        if section_key == "heroes_weight":
            emb, total_pages = _build_heroes_weight_embed(clan_payload, player_map, self.heroes_page, self.heroes_sort)
            self._heroes_total_pages = total_pages
            return emb
        if section_key == "discord_links":
            links = load_links() or {}
            guild = self.cog.bot.get_guild(self.guild_id) if self.guild_id else None
            emb, total_pages = _build_discord_links_embed(
                clan_payload,
                links if isinstance(links, dict) else {},
                guild=guild,
                page=self.discord_links_page,
                sort_mode=self.discord_links_sort,
            )
            self._discord_links_total_pages = total_pages
            return emb
        if section_key == "war_preferences":
            return _build_war_preferences_embed(clan_payload, player_map)
        if section_key == "tags_roles":
            return _build_tags_roles_embed(clan_payload)
        if section_key == "trophies_leagues":
            return _build_trophies_leagues_embed(clan_payload)
        if section_key == "last_joining":
            transfer_data = load_transfers_data() or {}
            events = transfer_data.get("events", []) if isinstance(transfer_data, dict) else []
            return _build_last_joining_embed(clan_payload, events if isinstance(events, list) else [])
        if section_key == "player_progress":
            activity = load_member_activity() or {}
            clan_activity = activity.get(str(self.selected_tag), {}) if isinstance(activity, dict) else {}
            return _build_player_progress_embed(clan_payload, clan_activity if isinstance(clan_activity, dict) else {})
        if section_key == "attacks_defenses":
            return _build_attacks_defenses_embed(clan_payload)
        return _build_clan_overview_embed(clan_payload)

    async def switch_section(self, interaction: discord.Interaction, section_key: str) -> None:
        if section_key not in _CLAN_DASHBOARD_SECTIONS:
            section_key = "overview"
        self.selected_section = section_key
        if section_key != "heroes_weight":
            self.heroes_page = 0  # Reset pagination when switching away from heroes
        if section_key != "discord_links":
            self.discord_links_page = 0
        self._refresh_items()
        await interaction.response.defer()
        embed = await self.build_embed()
        self._refresh_items()
        await interaction.edit_original_response(embed=embed, view=self)

    async def switch_clan(self, interaction: discord.Interaction, clan_tag: str) -> None:
        if clan_tag == "NONE":
            return
        self.selected_tag = str(clan_tag).upper().strip()
        self._refresh_items()
        await interaction.response.defer()
        embed = await self.build_embed()
        self._refresh_items()
        await interaction.edit_original_response(embed=embed, view=self)

    async def refresh_current(self, interaction: discord.Interaction) -> None:
        self._clan_cache.pop(self.selected_tag, None)
        self._player_cache.pop(self.selected_tag, None)
        await interaction.response.defer()
        embed = await self.build_embed()
        self._refresh_items()
        await interaction.edit_original_response(embed=embed, view=self)

_HELP_SECTIONS: Dict[str, Dict[str, Any]] = {
    "quick_start": {
        "title": "Quick Start",
        "emoji": "🚀",
        "description": "Start here if you are new.",
        "lines": [
            "• `help` (`h`) - open help",
            "• `clan [clan]` (`cl`) - interactive clan dashboard",
            "• `link <#TAG>` (`ln`) - link account",
            "• `info [#TAG]` (`i`) - player profile",
            "• `donations [#TAG]` (`don`) - donation stats",
            "• `challenge` (`ch`) - weekly challenge status",
            "• Tip: use `cc2 <command>` or `/command`",
        ],
    },
    "profile": {
        "title": "Profile & Progress",
        "emoji": "📊",
        "description": "Player analysis and progression tools.",
        "lines": [
            "• `profile [#TAG]` (`pf`)",
            "• `compare <tag_a> <tag_b>` (`cmp`)",
            "• `upgradepriority [#TAG]` (`upg`)",
            "• `upgradecheck [min_heroes] [clan]` (`uc`)",
            "• `rushhistory [#TAG] [limit]` (`rhs`)",
            "• `achievements [#TAG]` (`ach`)",
            "• `milestone [#TAG]` (`ms`)",
        ],
    },
    "war": {
        "title": "War Commands",
        "emoji": "⚔️",
        "description": "War status, scouting, trends, and performance.",
        "lines": [
            "• `whohavenotattacked [clan]` (`wna`)",
            "• `warmap [clan]` (`wm`)",
            "• `opponentlineup [clan]` (`ol`)",
            "• `warpreview [clan]` (`wpv`)",
            "• `cwlgroup [clan]` (`cwl`)",
            "• `cwlround [clan] [round_no]` (`cwlr`)",
            "• `warhistory [clan] [limit]` (`wh`)",
            "• `wartrends [clan] [wars]` (`wt`)",
            "• `warperformance [#TAG]` (`wp`)",
            "• `warreminder <on|off>` (`wr`)",
            "• `rankings <clan|player> [location] [limit]` (`rank`)",
            "• `labels <clan|player>` (`lbl`)",
            "• `locations [search]` (`loc`)",
        ],
    },
    "raid": {
        "title": "Raid Commands",
        "emoji": "🏰",
        "description": "Raid completion, reports, and reminders.",
        "lines": [
            "• `raidstatus [clan]` (`rs`)",
            "• `raidsleft [clan]` (`rl`)",
            "• `raidreport [clan]` (`rrpt`)",
            "• `raidhistory [clan] [limit]` (`rh`)",
            "• `raidtrends [clan] [weekends]` (`rt`)",
            "• `capitalstatus [clan]` (`cps`)",
            "• `capitalrank [clan] [location_id]` (`cprank`)",
            "• `capitalleagues [league_id]` (`cpleagues`)",
            "• `raidreminder <on|off>` (`rr`)",
        ],
    },
    "leaderboards": {
        "title": "Donations & Leaderboards",
        "emoji": "🏆",
        "description": "Snapshots, donation history, and ranks.",
        "lines": [
            "• `donationhistory [clan] [months] [scope]` (`dh`)",
            "• `takesnapshot [clan] [scope]` (`ts`)",
            "• `top [category] [clan] [scope]` (`lb`)",
            "• `myrank [category] [clan] [tag] [scope]` (`mr`)",
        ],
    },
    "utility": {
        "title": "Account & Utility",
        "emoji": "🧰",
        "description": "Account management and utility commands.",
        "lines": [
            "• `setmain <#TAG>` (`mainacc`), `unlink` (`unln`), `whois` (`wi`)",
            "• `clan [clan]` (`cl`) - clan overview with section dropdowns",
            "• `status` (`st`), `calculate <expression>` (`calc`), `botstats` (`bs`)",
            "• `remind <message> <duration>` (`rm`), `roster [clan]` (`ros`)",
            "• `addbase ...` (`ab`) / `fetchbase ...` (`fb`)",
            "• `addattack ...` (`aatk`) / `fetchattack ...` (`fatk`)",
        ],
    },
    "leadership": {
        "title": "Leadership & Admin",
        "emoji": "🛡️",
        "description": "Leadership and admin-only controls.",
        "lines": [
            "• `kicksuggestions [clan]` (`ks`), `inactive [clan] [days]` (`ia`)",
            "• `promotionsuggestions [clan]` (`ps`), `findplayer <name> [scope]` (`fp`)",
            "• `familyreport [scope]` (`fr`), `poll ...` (`pl`), `addachievement ...` (`addach`)",
            "• `config` / `config set` / `config get` (`cfg` / `cset` / `cget`)",
            "• `maintenance <on|off> [message]` (`maint`), `maintstatus` (`mstat`)",
            "• `restart [auto|close|relaunch]` (`reboot`)",
            "• `onboardingdm <@member> [force]` (`odm`)",
            "• Admin tools: `clearbot` (`cb`), `clear` (`cg`), `createevent` (`ce`)",
            "• Slash-only: `/clearcache`, `/cleanup`, `/syncroles`, `/addclan`, `/removeclan`, `/setbase`, `/getbase`, `/basebook`",
        ],
    },
}


def _build_help_embed(section_key: str) -> discord.Embed:
    section = _HELP_SECTIONS.get(section_key, _HELP_SECTIONS["quick_start"])
    title = str(section.get("title") or "Help")
    emoji = str(section.get("emoji") or "📘")
    description = str(section.get("description") or "")
    lines = section.get("lines", []) if isinstance(section.get("lines", []), list) else []

    text = "\n".join(str(line) for line in lines)
    if len(text) > 3800:
        text = text[:3800].rstrip() + "\n..."

    embed = discord.Embed(
        title=f"{emoji} CC2 Help • {title}",
        description=description,
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Commands", value=(text or "No entries."), inline=False)
    embed.set_footer(text="Use the dropdown below to jump to any section")
    return embed


class HelpSectionSelect(discord.ui.Select):
    def __init__(self, selected_key: str):
        options: List[discord.SelectOption] = []
        for key, section in _HELP_SECTIONS.items():
            options.append(
                discord.SelectOption(
                    label=str(section.get("title") or key),
                    value=key,
                    description=str(section.get("description") or "")[:100],
                    emoji=str(section.get("emoji") or None),
                    default=(key == selected_key),
                )
            )

        super().__init__(
            placeholder="Jump to help section...",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        if not isinstance(self.view, HelpCommandView):
            return
        await self.view.switch_section(interaction, self.values[0])


class HelpCommandView(discord.ui.View):
    def __init__(self, author_id: Optional[int], selected_key: str = "quick_start"):
        super().__init__(timeout=300)
        self.author_id = author_id
        self.selected_key = selected_key
        self.add_item(HelpSectionSelect(selected_key))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is None or interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Only the command invoker can use this help menu.", ephemeral=True)
        return False

    async def switch_section(self, interaction: discord.Interaction, section_key: str) -> None:
        self.selected_key = section_key if section_key in _HELP_SECTIONS else "quick_start"
        self.clear_items()
        self.add_item(HelpSectionSelect(self.selected_key))
        await interaction.response.edit_message(embed=_build_help_embed(self.selected_key), view=self)


def _poll_title(question: str) -> str:
    base = (question or "Community Poll").strip()
    if len(base) > 80:
        base = base[:77].rstrip() + "..."
    return f"🎯 {base} — Community Vote"


def _poll_icon_url(question: str) -> str:
    q = (question or "").lower()
    if "quest" in q or "event" in q:
        return _POLL_ICON_SCROLL
    return _POLL_ICON_SWORDS


def _format_poll_remaining(close_at: datetime) -> str:
    now = datetime.now(timezone.utc)
    remaining = max(0, int((close_at - now).total_seconds()))
    if remaining <= 0:
        return "⏰ Closed"
    hours = remaining // 3600
    if hours >= 1:
        return f"⏰ Closes in {hours} hour{'s' if hours != 1 else ''}"
    minutes = max(1, remaining // 60)
    return f"⏰ Closes in {minutes} minute{'s' if minutes != 1 else ''}"


def _resolve_restart_relaunch(mode: str) -> Optional[bool]:
    """Resolve restart behavior from mode.

    Returns:
    - True: spawn new process then close current bot.
    - False: close current bot only (supervisor expected to restart).
    - None: invalid mode value.
    """
    value = str(mode or "auto").strip().lower()
    if value == "relaunch":
        return True
    if value == "close":
        return False
    if value == "auto":
        disable_auto = str(os.environ.get("CC2_DISABLE_AUTO_RELAUNCH", "0")).strip().lower() in {"1", "true", "yes", "on"}
        if disable_auto:
            return False
        return os.name == "nt"
    return None


class ConfirmDangerView(discord.ui.View):
    """Simple Yes/Cancel confirmation view for destructive admin commands."""

    def __init__(self, author_id: int, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.author_id = author_id
        self.confirmed = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Only the command invoker can use these buttons.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Yes, proceed", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="✅ Confirmed. Running action...", view=self)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="❎ Cancelled.", view=self)
        self.stop()


def _poll_progress_bar(count: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "░" * width
    filled = int(round((float(count) / float(total)) * width))
    filled = max(0, min(width, filled))
    return ("█" * filled) + ("░" * (width - filled))


def _parse_poll_duration_seconds(value: str) -> Optional[int]:
    text = (value or "").strip().lower()
    if not text:
        return 3600

    # Backward-compat: plain integer means hours.
    if text.isdigit():
        return int(text) * 3600

    m = re.fullmatch(r"(\d+)\s*([mhd])", text)
    if not m:
        return None

    amount = int(m.group(1))
    unit = m.group(2)
    if unit == "m":
        return amount * 60
    if unit == "h":
        return amount * 3600
    return amount * 86400


def _format_poll_duration_label(total_seconds: int) -> str:
    seconds = max(0, int(total_seconds))
    if seconds % 86400 == 0:
        days = seconds // 86400
        return f"{days} day(s)"
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} hour(s)"
    minutes = max(1, seconds // 60)
    return f"{minutes} minute(s)"


def _build_poll_embed(
    *,
    question: str,
    choices: List[str],
    emojis: List[str],
    mode_val: str,
    creator_name: str,
    close_at: datetime,
    counts: List[int],
    is_closed: bool,
    minimum_votes: int,
    winners: Optional[List[str]] = None,
) -> discord.Embed:
    total_votes = int(sum(counts)) if counts else 0
    is_inconclusive = bool(is_closed and total_votes < int(max(1, minimum_votes)))
    status_text = "🟢 Active"
    if is_closed and is_inconclusive:
        status_text = "🟡 Inconclusive"
    elif is_closed:
        status_text = "🔴 Closed"
    mode_text = "Single Choice" if mode_val == "single" else "Multi Choice"
    if not is_closed:
        color = discord.Color.green()
    elif is_inconclusive:
        color = discord.Color.gold()
    else:
        color = discord.Color.red()

    description_lines = [
        f"Status: **{status_text}**",
        _format_poll_remaining(close_at) if not is_closed else "⏰ Poll is closed",
    ]
    if is_closed and is_inconclusive:
        description_lines.append(
            f"Minimum votes not reached: **{total_votes}/{int(max(1, minimum_votes))}**"
        )
    elif is_closed and winners:
        if len(winners) == 1:
            description_lines.append(f"🏆 Winner: **{winners[0]}**")
        else:
            description_lines.append("🏆 Tie: " + ", ".join(f"**{w}**" for w in winners))

    emb = discord.Embed(
        title=_poll_title(question),
        description="\n".join(description_lines),
        color=color,
        timestamp=datetime.now(timezone.utc),
    )

    options_lines = []
    for i, choice in enumerate(choices):
        votes = int(counts[i]) if i < len(counts) else 0
        bar = _poll_progress_bar(votes, total_votes)
        if total_votes > 0:
            pct = int(round((float(votes) / float(total_votes)) * 100.0))
            options_lines.append(f"{emojis[i]} {choice}  {bar}  {pct}% ({votes})")
        else:
            options_lines.append(f"{emojis[i]} {choice}  {bar}  {votes} votes")
    emb.add_field(name="Options", value="\n".join(options_lines), inline=False)
    emb.add_field(name="Mode", value=mode_text, inline=True)
    emb.set_thumbnail(url=_poll_icon_url(question))
    emb.set_footer(text=f"Poll by {creator_name} • {total_votes} votes")
    return emb

def _normalize_tag(tag: str) -> str:
    tag = (tag or "").strip().upper()
    if tag and not tag.startswith("#"):
        tag = "#" + tag
    return tag


def _is_valid_clan_tag(tag: str) -> bool:
    return bool(re.match(r"^#[A-Z0-9]{5,9}$", tag or ""))


def resolve_clans(bot, clan_arg: Optional[str], guild_id: Optional[int] = None) -> Optional[List[Dict[str, str]]]:
    """Return list of clan dicts matching *clan_arg*, or all clans if None/'ALL'.

    Returns ``None`` when a specific tag was given but not found.
    """
    scoped_clans = bot.get_scoped_clans(guild_id)
    if not clan_arg or clan_arg == "ALL":
        return list(scoped_clans)
    tag_norm = _normalize_tag(clan_arg)
    for c in scoped_clans:
        if c["tag"].upper() == tag_norm:
            return [c]
    return None


def _get_clan_by_tag(bot, tag: str, guild_id: Optional[int] = None) -> Optional[Dict[str, str]]:
    tag_norm = _normalize_tag(tag)
    for c in bot.get_scoped_clans(guild_id):
        if c["tag"].upper() == tag_norm:
            return c
    return None


def _resolve_scope_clans(bot, guild_id: Optional[int], scope: str) -> List[Dict[str, str]]:
    scope_norm = (scope or "guild").strip().lower()
    if scope_norm == "family":
        return bot.get_all_monitored_clans()
    return bot.get_scoped_clans(guild_id)


def _inactive_severity(days: int, threshold: int) -> tuple[str, str]:
    threshold = max(1, int(threshold))
    days = max(0, int(days))
    if days >= (threshold * 3):
        return "Critical", "🔴"
    if days >= (threshold * 2):
        return "High", "🟠"
    return "Watch", "🟡"


def _inactive_action_hint(flagged_count: int, total_members: int, threshold: int) -> str:
    flagged_count = max(0, int(flagged_count))
    total_members = max(0, int(total_members))
    threshold = max(1, int(threshold))

    if flagged_count == 0 or total_members == 0:
        return "No urgent action needed. Keep normal check-ins and continue tracking freshness."

    ratio = float(flagged_count) / float(max(1, total_members))
    if ratio >= 0.40:
        return (
            f"High inactivity load. Run direct leadership check-ins now and review removal candidates at {threshold * 2}+ days."
        )
    if ratio >= 0.20:
        return (
            "Moderate inactivity load. Prioritize reminders for highest-risk members and monitor response in the next 24h."
        )
    return "Low inactivity load. Send a gentle reminder and re-check after next activity cycle."


def _promotion_confidence(readiness: float) -> tuple[str, str]:
    value = max(0.0, float(readiness or 0.0))
    if value >= 82.0:
        return "Promote Now", "🟢"
    if value >= 68.0:
        return "Review Soon", "🟡"
    if value >= 55.0:
        return "Coach First", "🟠"
    return "Not Ready", "🔴"


def _promotion_action_hint(readiness: float, blockers: List[str]) -> str:
    label, _ = _promotion_confidence(readiness)
    blocker_set = {str(b).strip().lower() for b in (blockers or [])}

    if label == "Promote Now":
        return "Eligible for immediate promotion review."
    if label == "Review Soon":
        if "low activity" in blocker_set:
            return "Close to target; confirm consistent activity before approval."
        return "Near-ready; review role fit and recent contribution consistency."
    if label == "Coach First":
        if "high rush score" in blocker_set:
            return "Focus hero/lab catch-up before promotion discussion."
        if "low donation ratio" in blocker_set:
            return "Improve donation support ratio before next review window."
        return "Set short coaching goals and reassess after next cycle."
    return "Hold promotion; require sustained improvement across key metrics."


def _safe_calculate_expression(expression: str) -> float:
    """Safely evaluate a basic math expression without eval()."""
    node = ast.parse(expression, mode="eval")

    def _eval(n: ast.AST) -> float:
        if isinstance(n, ast.Expression):
            return _eval(n.body)

        if isinstance(n, ast.BinOp):
            op_type = type(n.op)
            if op_type not in _CALC_ALLOWED_BINOPS:
                raise ValueError("Unsupported operator")
            left = _eval(n.left)
            right = _eval(n.right)
            return float(_CALC_ALLOWED_BINOPS[op_type](left, right))

        if isinstance(n, ast.UnaryOp):
            op_type = type(n.op)
            if op_type not in _CALC_ALLOWED_UNARY:
                raise ValueError("Unsupported unary operator")
            return float(_CALC_ALLOWED_UNARY[op_type](_eval(n.operand)))

        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float)):
            return float(n.value)

        raise ValueError("Only numbers and basic operators are allowed")

    return _eval(node)


class AdminCog(commands.Cog, name="Admin"):
    """Bot management, linking, base storage, roster export, and clan config."""

    def __init__(self, bot):
        self.bot = bot
        self._last_backup_date: Optional[str] = None
        self._short_reminder_tasks: set[asyncio.Task] = set()
        self._last_kick_review_week: Optional[str] = None

    async def cog_load(self):
        self.daily_backup_loop.start()
        self.reminder_dispatch_loop.start()
        self.weekly_kick_review_loop.start()

    async def cog_unload(self):
        self.daily_backup_loop.cancel()
        self.reminder_dispatch_loop.cancel()
        self.weekly_kick_review_loop.cancel()
        for task in list(self._short_reminder_tasks):
            task.cancel()

    @staticmethod
    def _parse_duration_seconds(text: str) -> Optional[int]:
        """Parse duration strings like 90s, 15m, 2h, 1d, 1h30m."""
        value = (text or "").strip().lower()
        if not value:
            return None
        matches = re.findall(r"(\d+)\s*([smhd])", value)
        if not matches:
            return None

        unit_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        total = 0
        for amount, unit in matches:
            total += int(amount) * unit_map[unit]
        return total if total > 0 else None

    @staticmethod
    def _parse_event_start_utc(text: str) -> Optional[datetime]:
        """Parse UTC event start text.

        Supported formats:
        - YYYY-MM-DD HH:MM
        - YYYY-MM-DDTHH:MM
        - YYYY-MM-DD HH:MM:SS
        - ISO strings with timezone (converted to UTC)
        - in 2h / in 90m / in 1d2h
        """
        raw = (text or "").strip()
        if not raw:
            return None

        lowered = raw.lower()
        if lowered.startswith("in "):
            seconds = AdminCog._parse_duration_seconds(lowered[3:].strip())
            if seconds is None:
                return None
            return datetime.now(timezone.utc) + timedelta(seconds=seconds)

        normalized = raw.replace("Z", "+00:00")
        dt: Optional[datetime] = None
        # Try ISO parser first.
        try:
            dt = datetime.fromisoformat(normalized)
        except Exception:
            dt = None

        # Fallbacks for common UTC-only formats without offset.
        if dt is None:
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                    break
                except Exception:
                    continue

        if dt is None:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    async def _deliver_personal_reminder(self, user_id: int, message: str, channel_id: Optional[int] = None):
        user = None
        try:
            user = await self.bot.fetch_user(int(user_id))
            await user.send(f"⏰ **Reminder**\n{message}")
            return
        except Exception:
            pass

        # Fallback: post in channel when DM fails.
        if channel_id:
            try:
                channel = self.bot.get_channel(int(channel_id)) or await self.bot.fetch_channel(int(channel_id))
                if channel:
                    mention = user.mention if user else f"<@{int(user_id)}>"
                    await channel.send(f"⏰ Reminder for {mention}: {message}")
            except Exception:
                pass

    async def _schedule_short_reminder(self, user_id: int, message: str, delay_seconds: int, channel_id: Optional[int] = None):
        try:
            await asyncio.sleep(delay_seconds)
            await self._deliver_personal_reminder(user_id, message, channel_id=channel_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    async def _restart_sequence(self, relaunch: bool = False) -> None:
        """Run a best-effort restart in the background after command response is sent."""
        await asyncio.sleep(1.5)

        if relaunch:
            try:
                root = Path(__file__).resolve().parent.parent
                entry = root / "discordwelcomebot.py"
                cmd = [sys.executable, str(entry)]

                popen_kwargs: Dict[str, Any] = {
                    "cwd": str(root),
                    "stdin": subprocess.DEVNULL,
                    "stdout": subprocess.DEVNULL,
                    "stderr": subprocess.DEVNULL,
                    "close_fds": True,
                }
                if os.name == "nt":
                    popen_kwargs["creationflags"] = (
                        getattr(subprocess, "DETACHED_PROCESS", 0)
                        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                    )

                subprocess.Popen(cmd, **popen_kwargs)
                logger.info("Restart command spawned new bot process.")
            except Exception as exc:
                logger.exception("Failed to relaunch bot process: %s", exc)

        try:
            await self.bot.close()
        except Exception:
            pass

    def _collect_backup_paths(self) -> List[Path]:
        """Collect JSON files + SQLite file for daily backup archive."""
        root = Path(__file__).resolve().parent.parent
        candidates: List[Path] = []

        db_file = root / "bot_data.sqlite3"
        if db_file.exists():
            candidates.append(db_file)

        for p in root.glob("*.json"):
            if p.is_file():
                candidates.append(p)

        return sorted(candidates, key=lambda x: x.name.lower())

    def _create_daily_backup(self) -> Optional[Path]:
        """Create backup zip and rotate to last 7 files."""
        root = Path(__file__).resolve().parent.parent
        backup_dir = root / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)

        date_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out_path = backup_dir / f"backup_{date_key}.zip"
        inputs = self._collect_backup_paths()
        if not inputs:
            return None

        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fp in inputs:
                zf.write(fp, arcname=fp.name)

        archives = sorted(backup_dir.glob("backup_*.zip"), key=lambda p: p.name, reverse=True)
        for old in archives[7:]:
            try:
                old.unlink()
            except Exception:
                pass

        return out_path

    @tasks.loop(minutes=60)
    async def daily_backup_loop(self):
        now = datetime.now(timezone.utc)
        # Run once daily shortly after 03:00 UTC.
        if now.hour != 3:
            return

        today = now.strftime("%Y-%m-%d")
        if self._last_backup_date == today:
            return

        try:
            out = self._create_daily_backup()
            if out is not None:
                logger.info("Daily backup created: %s", out.name)
                self._last_backup_date = today
        except Exception as e:
            logger.error("Daily backup failed: %s", e)

    @daily_backup_loop.before_loop
    async def before_daily_backup(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=30)
    async def reminder_dispatch_loop(self):
        now = datetime.now(timezone.utc)
        rows = load_personal_reminders()
        if not rows:
            return

        for row in rows:
            if not isinstance(row, dict):
                continue
            reminder_id = int(row.get("id", 0) or 0)
            due_at_raw = str(row.get("due_at", "") or "")
            if not reminder_id or not due_at_raw:
                continue
            try:
                due_at = datetime.fromisoformat(due_at_raw.replace("Z", "+00:00"))
            except Exception:
                continue
            if due_at > now:
                continue

            payload = row.get("payload", {}) if isinstance(row.get("payload", {}), dict) else {}
            user_id = int(payload.get("user_id", 0) or 0)
            message = str(payload.get("message", "")).strip()
            channel_id = payload.get("channel_id")
            if user_id and message:
                await self._deliver_personal_reminder(user_id, message, channel_id=channel_id)
            delete_personal_reminder(reminder_id)

    @reminder_dispatch_loop.before_loop
    async def before_reminder_dispatch(self):
        await self.bot.wait_until_ready()

    async def _collect_kick_suggestion_lines(self, clans_to_check: List[Dict[str, str]]) -> List[str]:
        from calculations import calculate_weighted_rush_score, calculate_activity_score
        from cogs.profiles import _exclude_minion_prince

        output: List[str] = []
        war_stats_data = load_war_player_stats()
        for c in clans_to_check:
            ml = await self.bot.get_clan_member_list(c["tag"])
            if not ml:
                continue
            tags = [m.get("tag") for m in ml if m.get("tag")]
            pc = await self.bot.fetch_players(tags)
            war = await self.bot.get_current_war(c["tag"])
            cw_members = (war.get("clan") or {}).get("members") if war and war.get("state") == "inWar" else []
            activity_map = load_member_activity().get(c["tag"], {})

            bad: List[str] = []
            for m in ml:
                tag = m.get("tag")
                if not tag:
                    continue
                player = pc.get(tag)
                if not player:
                    continue
                row_stats = ((war_stats_data.get(c["tag"], {}) if isinstance(war_stats_data.get(c["tag"], {}), dict) else {}).get(tag, {}))
                rush = calculate_weighted_rush_score(_exclude_minion_prince(player))
                rushed = rush and rush["is_rushed"]
                activity = calculate_activity_score(player)
                low_activity = float(activity.get("score", 0.0)) < 40.0
                missed_streak = int(row_stats.get("missed_streak", 0) or 0) if isinstance(row_stats, dict) else 0
                streak_flag = missed_streak >= 2
                inactive_flag = False
                days_inactive = 0
                rec = activity_map.get(tag, {}) if isinstance(activity_map, dict) else {}
                last_seen = rec.get("last_seen")
                if last_seen:
                    try:
                        last_dt = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                        days_inactive = max(0, (datetime.now(timezone.utc) - last_dt).days)
                        inactive_flag = days_inactive >= INACTIVE_DAYS_THRESHOLD
                    except Exception:
                        inactive_flag = False
                missed = False
                if cw_members:
                    found = next((x for x in cw_members if x.get("tag") == tag), None)
                    if found and len(found.get("attacks", [])) == 0:
                        missed = True

                if rushed or missed or low_activity or inactive_flag or streak_flag:
                    reasons = []
                    if rushed:
                        reasons.append(f"Rushed {rush['score']}%")
                    if missed:
                        reasons.append("No war hit")
                    if low_activity:
                        reasons.append(f"Activity {activity.get('score', 0):.1f}/100")
                    if inactive_flag:
                        reasons.append(f"Inactive {days_inactive}d")
                    if streak_flag:
                        reasons.append(f"Missed streak {missed_streak} wars")
                    bad.append(f"• {player.get('name')} `{tag}` — {', '.join(reasons)}")

            if bad:
                output.append(f"**{c['name']}:**\n" + "\n".join(bad))
        return output

    def _collect_leadership_members(self) -> Dict[int, discord.Member]:
        recipients: Dict[int, discord.Member] = {}
        for guild in self.bot.guilds:
            for member in getattr(guild, "members", []):
                if has_leadership_role(member, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
                    recipients[member.id] = member
        return recipients

    @tasks.loop(minutes=60)
    async def weekly_kick_review_loop(self):
        now = datetime.now(timezone.utc)
        kick_review_day = int(self.bot.resolve_effective_setting("kick_review_day", 0) or 0)
        kick_review_day = max(0, min(6, kick_review_day))

        # Run once a week at 12:00 UTC on configured weekday.
        if now.weekday() != kick_review_day or now.hour != 12:
            return

        week_key = now.strftime("%G-W%V")
        if self._last_kick_review_week == week_key:
            return

        clans_to_check = self.bot.get_all_monitored_clans()
        lines = await self._collect_kick_suggestion_lines(clans_to_check)

        recipients = self._collect_leadership_members()
        if not recipients:
            return

        emb = discord.Embed(
            title="🧭 Weekly Kick Review Digest",
            color=discord.Color.orange(),
            timestamp=now,
            description=(
                "Automated weekly leadership digest from kick analysis. "
                "Use `/kicksuggestions` for interactive follow-up."
            ),
        )
        if not lines:
            emb.add_field(name="Summary", value="No kick suggestions this week. Clan health looks stable.", inline=False)
        else:
            summary = "\n\n".join(lines)
            if len(summary) > 3800:
                summary = summary[:3800] + "\n..."
            emb.add_field(name="Candidates", value=summary, inline=False)
        emb.set_footer(text="CC2 Clash Bot • Weekly Leadership Digest")

        for member in recipients.values():
            try:
                await member.send(embed=emb)
                await asyncio.sleep(0.08)
            except Exception:
                continue

        self._last_kick_review_week = week_key

    @weekly_kick_review_loop.before_loop
    async def before_weekly_kick_review(self):
        await self.bot.wait_until_ready()

    async def _ensure_leadership_ctx(self, ctx: commands.Context) -> bool:
        if not has_leadership_role(ctx.author, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            await ctx.send("❌ Leadership role required for this command.")
            return False
        return True

    async def _ensure_admin_ctx(self, ctx: commands.Context) -> bool:
        if not has_admin_role(ctx.author, BOT_ADMIN_ROLE_ID):
            await ctx.send("❌ Admin permission required for this command.")
            return False
        return True

    async def _ensure_leadership_interaction(self, interaction: discord.Interaction) -> bool:
        if not has_leadership_role(interaction.user, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            if interaction.response.is_done():
                await interaction.followup.send("❌ Leadership role required for this command.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Leadership role required for this command.", ephemeral=True)
            return False
        return True

    @staticmethod
    def _recent_war_win_rate(clan_tag: str, max_wars: int = 10) -> float:
        data = load_war_results()
        clan_rows = data.get(clan_tag, []) if isinstance(data, dict) else []
        if not isinstance(clan_rows, list) or not clan_rows:
            return 0.0

        rows = clan_rows[-max(1, int(max_wars)):]
        points = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            result = str(row.get("result", "")).lower()
            if result == "win":
                points += 1.0
            elif result == "tie":
                points += 0.5
        return round((points / max(1, len(rows))) * 100.0, 2)

    @staticmethod
    def _recent_raid_completion_rate(clan_tag: str, max_weekends: int = 6) -> float:
        data = load_raid_history()
        clan_rows = data.get(clan_tag, {}) if isinstance(data, dict) else {}
        if not isinstance(clan_rows, dict) or not clan_rows:
            return 0.0

        weekends = sorted(clan_rows.items(), key=lambda x: x[0])[-max(1, int(max_weekends)):]
        used_total = 0
        limit_total = 0
        for _, row in weekends:
            if not isinstance(row, dict):
                continue
            members = row.get("members", {})
            if not isinstance(members, dict):
                continue
            for m in members.values():
                if not isinstance(m, dict):
                    continue
                used_total += int(m.get("attacks", 0) or 0)
                limit_total += int(m.get("limit", 6) or 6)

        if limit_total <= 0:
            return 0.0
        return round((used_total / limit_total) * 100.0, 2)

    # ═══════════════════════════════════
    # HELP (cc2 help)
    # ═══════════════════════════════════
    @commands.hybrid_command(name="help", aliases=["h"], description="Show command help and aliases")
    async def text_help(self, ctx: commands.Context):
        view = HelpCommandView(author_id=getattr(ctx.author, "id", None), selected_key="quick_start")
        await ctx.send(embed=_build_help_embed("quick_start"), view=view)

    async def _fetch_clan_payload(self, clan_tag: str) -> Optional[Dict[str, Any]]:
        tag_norm = str(clan_tag or "").upper().strip()
        if not _is_valid_clan_tag(tag_norm):
            return None
        return await self.bot.coc_get(f"/clans/{urllib.parse.quote(tag_norm)}")

    @commands.hybrid_command(name="clan", aliases=["cl"], description="Show interactive clan dashboard")
    @app_commands.describe(clan="Clan tag or monitored clan")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def clan_dashboard(self, ctx: commands.Context, clan: Optional[str] = None):
        guild_id = ctx.guild.id if ctx.guild else None
        clans = resolve_clans(self.bot, clan, guild_id=guild_id)
        if not clans:
            return await ctx.send(
                embed=build_error_embed("No monitored clans found for this server. Ask leadership to run `/addclan` first."),
            )

        selected = clans[0]
        selected_tag = str(selected.get("tag") or "").upper().strip()
        if not selected_tag:
            return await ctx.send(embed=build_error_embed("Could not resolve a valid clan tag."))

        view = ClanDashboardView(
            cog=self,
            author_id=getattr(ctx.author, "id", None),
            guild_id=getattr(ctx.guild, "id", None),
            clans=clans,
            selected_tag=selected_tag,
            selected_section="overview",
        )
        embed = await view.build_embed()
        await ctx.send(embed=embed, view=view)

    # Handle bare "cc2" (no sub-command)
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if message.content.strip().lower() == "cc2":
            await message.channel.send("Usage: `cc2 <command> [args]` — try `cc2 help`")
            return

        if not message.guild:
            return

        base_channels = self._resolve_base_channel_ids(message.guild.id)
        raw_attack_channel = self.bot.resolve_effective_setting(
            "attack_strategy_channel_id",
            ATTACK_STRATEGY_CHANNEL_ID,
            guild_id=message.guild.id,
        )
        try:
            attack_channel_id = int(raw_attack_channel or 0)
        except Exception:
            attack_channel_id = 0

        channel_id = getattr(message.channel, "id", 0)
        parent_id = getattr(getattr(message.channel, "parent", None), "id", 0)
        channel_matches_base = bool(base_channels and (channel_id in base_channels or parent_id in base_channels))
        channel_matches_attack = bool(attack_channel_id > 0 and (channel_id == attack_channel_id or parent_id == attack_channel_id))

        if channel_matches_base:
            link = self._extract_base_layout_link(message.content)
            if not link:
                text_l = (message.content or "").lower()
                if "clashofclans.com" in text_l:
                    await message.channel.send(
                        "❌ I could not parse that base link. Please paste the full layout URL with `action=OpenLayout&id=...`",
                        delete_after=20,
                    )
                return

            inferred_type = self._infer_base_type_from_text(message.content)
            meta = self._extract_layout_meta_from_link(link)
            th_val = meta.get("townHall") if isinstance(meta.get("townHall"), int) else None
            style_val = self._normalize_layout_style(meta.get("layoutStyle"))
            if not isinstance(th_val, int):
                return await message.channel.send(
                    "❌ Could not detect Town Hall from this link. Please use a valid layout link.",
                    delete_after=20,
                )
            owner_key = self._resolve_base_owner_key(th_val)

            auto_name = f"Auto {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
            ok, chosen_type, chosen_type_label, chosen_name = await self._confirm_base_save(
                source_message=message,
                owner_label=owner_key,
                base_type=inferred_type,
                default_name=auto_name,
                town_hall=th_val,
                layout_style=style_val,
            )
            if not ok:
                return

            created, total = self._save_base_entry(
                owner_key=owner_key,
                base_type=chosen_type or inferred_type,
                link=link,
                name=chosen_name or auto_name,
                added_by_id=message.author.id,
                town_hall=th_val,
                layout_style=style_val,
            )
            if created:
                await message.channel.send(
                    f"✅ Saved {chosen_type_label or inferred_type} base for {owner_key}. Total: {total}",
                    delete_after=20,
                )
            else:
                await message.channel.send(
                    f"ℹ️ This base link already exists for {owner_key} under {chosen_type_label or inferred_type}.",
                    delete_after=20,
                )
            return

        if channel_matches_attack:
            text = (message.content or "").strip()
            if len(text) < 6:
                return

            th = self._extract_town_hall_from_text(text)
            if th is None:
                return
            style = self._extract_style_from_text(text) or "GEN"
            url = self._extract_first_url(text)
            created, total = self._save_attack_strategy_entry(
                town_hall=th,
                style=style,
                title=f"Auto TH{th} {style}",
                strategy_text=text,
                link=url,
                added_by_id=message.author.id,
            )
            if created:
                await message.channel.send(
                    f"✅ Saved attack strategy for `TH{th}` `{style}`. Total: **{total}**",
                    delete_after=20,
                )
            else:
                await message.channel.send(
                    f"ℹ️ This attack strategy already exists for `TH{th}` `{style}`.",
                    delete_after=20,
                )
            return

    def _resolve_base_channel_ids(self, guild_id: Optional[int]) -> set[int]:
        """Resolve allowed channels for base add/fetch and auto-capture."""
        ids: set[int] = set()

        raw_ids = self.bot.resolve_effective_setting(
            "base_layout_channel_ids",
            None,
            guild_id=guild_id,
        )
        if isinstance(raw_ids, list):
            for v in raw_ids:
                try:
                    iv = int(v)
                    if iv > 0:
                        ids.add(iv)
                except Exception:
                    continue
        elif isinstance(raw_ids, str):
            for part in raw_ids.split(","):
                token = part.strip()
                if not token:
                    continue
                try:
                    iv = int(token)
                    if iv > 0:
                        ids.add(iv)
                except Exception:
                    continue

        raw_single = self.bot.resolve_effective_setting(
            "base_layout_channel_id",
            BASE_LAYOUT_CHANNEL_ID,
            guild_id=guild_id,
        )
        try:
            single = int(raw_single or 0)
            if single > 0:
                ids.add(single)
        except Exception:
            pass

        # Fallback for file-only settings when DB settings don't include new keys yet.
        if not ids:
            file_settings = load_json("settings.json")
            if isinstance(file_settings, dict):
                raw_ids_file = file_settings.get("base_layout_channel_ids")
                if isinstance(raw_ids_file, list):
                    for v in raw_ids_file:
                        try:
                            iv = int(v)
                            if iv > 0:
                                ids.add(iv)
                        except Exception:
                            continue
                raw_single_file = file_settings.get("base_layout_channel_id")
                try:
                    iv = int(raw_single_file or 0)
                    if iv > 0:
                        ids.add(iv)
                except Exception:
                    pass

        return ids

    @staticmethod
    def _format_channel_mentions(channel_ids: set[int]) -> str:
        if not channel_ids:
            return "(not configured)"
        return ", ".join(f"<#{cid}>" for cid in sorted(channel_ids))

    @staticmethod
    def _slugify_section_name(value: str) -> str:
        text = (value or "").strip().lower()
        text = re.sub(r"\s+", "_", text)
        text = re.sub(r"[^a-z0-9_-]", "", text)
        return text[:32].strip("_")

    @classmethod
    def _resolve_base_section(cls, requested: str, custom_section: Optional[str] = None) -> tuple[str, str]:
        req = (requested or "").strip().lower()
        if req in set(BASE_TYPES):
            return req, req

        if req == "custom":
            label = (custom_section or "").strip()
            slug = cls._slugify_section_name(label)
            if not slug:
                raise ValueError("Custom section name is required when base type is custom.")
            return f"custom:{slug}", f"custom:{label}"

        slug = cls._slugify_section_name(req)
        if not slug:
            raise ValueError("Invalid base type.")
        return f"custom:{slug}", f"custom:{req}"

    @staticmethod
    def _resolve_base_owner_key(town_hall: Optional[int], fallback_tag: Optional[str] = None) -> str:
        if isinstance(town_hall, int) and town_hall > 0:
            return f"TH{town_hall}"
        tag = (fallback_tag or "").strip().upper()
        return tag or "TH_UNKNOWN"

    async def _confirm_base_save(
        self,
        *,
        source_message: discord.Message,
        owner_label: str,
        base_type: str,
        default_name: str,
        town_hall: Optional[int],
        layout_style: Optional[str],
    ) -> tuple[bool, Optional[str], Optional[str], Optional[str]]:
        th_text = f"TH{town_hall}" if isinstance(town_hall, int) else "TH?"
        style_text = layout_style or "N/A"
        prompt = await source_message.channel.send(
            (
                f"⚠️ Base will be saved in **{th_text}** section (style **{style_text}**, type **{base_type}**) "
                f"for `{owner_label}`.\n"
                "Reply options (30s):\n"
                "`y` = save with defaults\n"
                "`y <type>` = choose type (war/legend/anti2/blizzard/custom)\n"
                "`y custom <section>` = save under custom section\n"
                "`y <type> | <base name>` = set custom base name\n"
                "`n` = cancel"
            ),
            delete_after=35,
        )

        def _check(msg: discord.Message) -> bool:
            if msg.author.id != source_message.author.id:
                return False
            if msg.channel.id != source_message.channel.id:
                return False
            text = (msg.content or "").strip().lower()
            return bool(text.startswith("y") or text.startswith("n"))

        try:
            reply = await self.bot.wait_for("message", check=_check, timeout=30.0)
        except asyncio.TimeoutError:
            await source_message.channel.send("⏱️ Base save cancelled (no confirmation received).", delete_after=15)
            return False, None, None, None

        raw = (reply.content or "").strip()
        lower = raw.lower()
        if lower in {"n", "no"}:
            await source_message.channel.send("❌ Base save cancelled.", delete_after=15)
            return False, None, None, None

        if lower.startswith("y") or lower.startswith("yes"):
            parsed = raw
            if lower.startswith("yes"):
                parsed = raw[3:].strip()
            elif lower.startswith("y"):
                parsed = raw[1:].strip()

            left = parsed
            custom_name = default_name
            if "|" in parsed:
                left, right = parsed.split("|", 1)
                if right.strip():
                    custom_name = right.strip()

            left = left.strip()
            selected_type = base_type
            custom_section = None
            if left:
                parts = left.split()
                selected_type = parts[0].lower()
                if selected_type == "custom":
                    custom_section = " ".join(parts[1:]).strip() or None

            try:
                section_key, section_label = self._resolve_base_section(selected_type, custom_section)
            except ValueError as ex:
                await source_message.channel.send(f"❌ {ex}", delete_after=20)
                return False, None, None, None

            try:
                await prompt.delete()
            except Exception:
                pass
            return True, section_key, section_label, custom_name

        await source_message.channel.send("❌ Base save cancelled.", delete_after=15)
        return False, None, None, None

    @staticmethod
    def _extract_base_layout_link(content: str) -> Optional[str]:
        text = (content or "").strip()
        if not text:
            return None

        # Handle accidental line breaks inside pasted URLs.
        compact = text.replace("\n", "").replace("\r", "")

        # Fast path for known format.
        m = _BASE_LINK_RE.search(compact)
        if m:
            return m.group(0).strip()

        # Fallback: parse any URLs and validate CoC open-layout query params.
        for u in _GENERIC_URL_RE.findall(compact):
            try:
                parsed = urllib.parse.urlparse(u)
                host = (parsed.netloc or "").lower()
                if "link.clashofclans.com" not in host:
                    continue
                q = urllib.parse.parse_qs(parsed.query)
                action = str((q.get("action") or [""])[0]).strip().lower()
                if action != "openlayout":
                    continue
                if not (q.get("id") or [""])[0]:
                    continue
                return u.strip()
            except Exception:
                continue

        return None

    @staticmethod
    def _infer_base_type_from_text(content: str) -> str:
        text = (content or "").lower()
        if "legend" in text:
            return "legend"
        if "anti2" in text or "anti-2" in text:
            return "anti2"
        if "blizzard" in text:
            return "blizzard"
        return "war"

    @staticmethod
    def _normalize_layout_style(style: Optional[str]) -> Optional[str]:
        s = (style or "").strip().upper()
        return s or None

    @staticmethod
    def _extract_layout_meta_from_link(link: str) -> Dict[str, Any]:
        """Extract TH/style metadata from a Clash layout link ID when available."""
        out: Dict[str, Any] = {}
        try:
            parsed = urllib.parse.urlparse(link or "")
            query = urllib.parse.parse_qs(parsed.query)
            raw_id = (query.get("id") or [""])[0]
            if not raw_id:
                return out
            decoded_id = urllib.parse.unquote(raw_id)
            out["layoutId"] = decoded_id
            m = _BASE_LAYOUT_ID_RE.search(decoded_id)
            if m:
                out["townHall"] = int(m.group(1))
                out["layoutStyle"] = m.group(2).upper()
        except Exception:
            return out
        return out

    @staticmethod
    def _save_base_entry(
        *,
        owner_key: str,
        base_type: str,
        link: str,
        name: str,
        added_by_id: int,
        town_hall: Optional[int] = None,
        layout_style: Optional[str] = None,
    ) -> tuple[bool, int]:
        bases = load_bases()
        owner_bases = bases.get(owner_key, {})
        entry_list = owner_bases.get(base_type, [])
        if not isinstance(entry_list, list):
            entry_list = []

        link_clean = (link or "").strip()
        if any(str(e.get("link", "")).strip() == link_clean for e in entry_list if isinstance(e, dict)):
            return False, len(entry_list)

        meta = AdminCog._extract_layout_meta_from_link(link_clean)
        th_val = None
        if isinstance(town_hall, int) and town_hall > 0:
            th_val = town_hall
        elif isinstance(meta.get("townHall"), int):
            th_val = int(meta.get("townHall"))

        style_val = AdminCog._normalize_layout_style(layout_style)
        if not style_val:
            style_val = AdminCog._normalize_layout_style(meta.get("layoutStyle"))

        entry_list.append(
            {
                "name": name,
                "link": link_clean,
                "addedBy": str(added_by_id),
                "addedAt": datetime.now(timezone.utc).isoformat(),
                "townHall": th_val,
                "layoutStyle": style_val,
                "layoutId": meta.get("layoutId"),
                "owner": owner_key,
            }
        )
        owner_bases[base_type] = entry_list
        bases[owner_key] = owner_bases
        save_bases(bases)
        return True, len(entry_list)

    @staticmethod
    def _extract_first_url(content: str) -> Optional[str]:
        m = _GENERIC_URL_RE.search(content or "")
        if not m:
            return None
        return m.group(0).strip()

    @staticmethod
    def _extract_town_hall_from_text(content: str) -> Optional[int]:
        m = _TH_TEXT_RE.search(content or "")
        if not m:
            return None
        try:
            val = int(m.group(1))
            return val if 1 <= val <= 18 else None
        except Exception:
            return None

    @staticmethod
    def _extract_style_from_text(content: str) -> Optional[str]:
        text = content or ""
        m = re.search(r"\bstyle\s*[:=]\s*([A-Za-z0-9_-]{2,12})\b", text, re.IGNORECASE)
        if m:
            return m.group(1).upper()
        return None

    @staticmethod
    def _save_attack_strategy_entry(
        *,
        town_hall: int,
        style: str,
        title: str,
        strategy_text: str,
        link: Optional[str],
        added_by_id: int,
    ) -> tuple[bool, int]:
        rows = load_attack_strategies()
        if not isinstance(rows, list):
            rows = []

        th = int(town_hall)
        style_norm = (style or "GEN").strip().upper() or "GEN"
        text_norm = (strategy_text or "").strip()
        link_norm = (link or "").strip() or None

        for row in rows:
            if not isinstance(row, dict):
                continue
            if int(row.get("townHall", 0) or 0) != th:
                continue
            if str(row.get("style", "")).strip().upper() != style_norm:
                continue
            if str(row.get("strategy", "")).strip() == text_norm and (str(row.get("link", "")).strip() or None) == link_norm:
                total = sum(
                    1
                    for r in rows
                    if isinstance(r, dict)
                    and int(r.get("townHall", 0) or 0) == th
                    and str(r.get("style", "")).strip().upper() == style_norm
                )
                return False, total

        rows.append(
            {
                "townHall": th,
                "style": style_norm,
                "title": (title or "Strategy").strip() or "Strategy",
                "strategy": text_norm,
                "link": link_norm,
                "addedBy": str(added_by_id),
                "addedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        save_attack_strategies(rows)
        total = sum(
            1
            for r in rows
            if isinstance(r, dict)
            and int(r.get("townHall", 0) or 0) == th
            and str(r.get("style", "")).strip().upper() == style_norm
        )
        return True, total

    # ═══════════════════════════════════
    # /link  +  cc2 link
    # ═══════════════════════════════════
    @commands.hybrid_command(name="link", aliases=["ln"], description="Link your Discord account to a Clash player tag")
    @app_commands.describe(tag="Your player tag (example: #2PQUE2J)")
    async def link(self, ctx: commands.Context, *, tag: str):
        tag_norm = _normalize_tag(tag)
        links = load_links()
        links[tag_norm] = str(ctx.author.id)
        save_links(links)
        emb = discord.Embed(title="🔗 Account Linked ✅", color=discord.Color.green(), timestamp=datetime.now(timezone.utc))
        emb.add_field(name="Discord User", value=f"{ctx.author.mention}", inline=True)
        emb.add_field(name="Player Tag", value=f"`{tag_norm}`", inline=True)
        emb.set_footer(text="CC2 Clash Bot • Account Linked")
        await ctx.send(embed=emb)

    @commands.hybrid_command(name="setmain", aliases=["mainacc"], description="Set your primary linked Clash account")
    @app_commands.describe(tag="Linked player tag to make primary (example: #2PQUE2J)")
    async def setmain(self, ctx: commands.Context, *, tag: str):
        tag_norm = _normalize_tag(tag)
        linked_tags = get_linked_tags_for_user(ctx.author.id)
        if not linked_tags:
            return await ctx.send("❌ You have no linked accounts. Use `cc2 link <#TAG>` first.")

        if tag_norm not in linked_tags:
            return await ctx.send(
                "❌ That tag is not linked to your Discord account.\n"
                f"Linked: {', '.join(f'`{t}`' for t in linked_tags)}"
            )

        ok = set_primary_tag_for_user(ctx.author.id, tag_norm)
        if not ok:
            return await ctx.send("❌ Failed to update primary account. Please try again.")

        emb = discord.Embed(title="✅ Primary Account Updated", color=discord.Color.green(), timestamp=datetime.now(timezone.utc))
        emb.add_field(name="Discord User", value=f"{ctx.author.mention}", inline=True)
        emb.add_field(name="Primary Tag", value=f"`{tag_norm}`", inline=True)
        emb.set_footer(text="CC2 Clash Bot • Primary Account")
        await ctx.send(embed=emb)

    # ═══════════════════════════════════
    # cc2 unlink / cc2 whois (text-only)
    # ═══════════════════════════════════
    @commands.command(name="unlink", aliases=["unln"])
    async def text_unlink(self, ctx: commands.Context):
        uid = str(ctx.author.id)
        links = load_links()
        removed = [k for k, v in links.items() if v == uid]
        for k in removed:
            del links[k]
        if removed:
            save_links(links)
            # Re-point primary account if needed
            remaining = get_linked_tags_for_user(ctx.author.id)
            if remaining:
                set_primary_tag_for_user(ctx.author.id, remaining[0])
            await ctx.send(f"✅ Unlinked: {', '.join(removed)}")
        else:
            await ctx.send("❌ No linked tags found.")

    @commands.command(name="whois", aliases=["wi"])
    async def text_whois(self, ctx: commands.Context):
        linked = get_linked_tags_for_user(ctx.author.id)
        if linked:
            primary = get_primary_tag_for_user(ctx.author.id)
            lines = [f"🔗 Linked tags for {ctx.author.mention}:"]
            for t in linked:
                if primary and t == primary:
                    lines.append(f"• {t} (main)")
                else:
                    lines.append(f"• {t}")
            await ctx.send("\n".join(lines))
        else:
            await ctx.send("🔍 No linked tags. Use `cc2 link <#TAG>`.")

    # ═══════════════════════════════════
    # /status  +  cc2 status
    # ═══════════════════════════════════
    @commands.hybrid_command(name="status", aliases=["st"], description="Show bot status and basic stats")
    async def status(self, ctx: commands.Context):
        u = self.bot.user
        guilds = len(self.bot.guilds)
        now = datetime.now(timezone.utc).isoformat()
        cog_count = len(self.bot.cogs)
        scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)

        clan_health_scores: List[float] = []
        for c in scoped:
            members = await self.bot.get_clan_member_list(c["tag"])
            members = members or []
            tags = [m.get("tag") for m in members if m.get("tag")]
            players = await self.bot.fetch_players(tags)
            player_rows = [players[t] for t in tags if t in players and isinstance(players[t], dict)]

            war_win = self._recent_war_win_rate(c["tag"]) 
            raid_completion = self._recent_raid_completion_rate(c["tag"])
            health = calculate_clan_health_score(player_rows, war_win, raid_completion)
            clan_health_scores.append(float(health.get("score", 0.0) or 0.0))

        family_health = (sum(clan_health_scores) / len(clan_health_scores)) if clan_health_scores else 0.0

        text = (
            f"**Bot:** {u}\n"
            f"**Guilds:** {guilds}\n"
            f"**Time:** {now}\n"
            f"**Cogs loaded:** {cog_count}\n"
            f"**Monitored clans:** {', '.join(c['name'] for c in scoped)}\n"
            f"**Avg Clan Health:** {family_health:.1f}/100"
        )
        await ctx.send(text)

    @commands.hybrid_command(name="calculate", aliases=["calc"], description="Calculate a basic math expression")
    @app_commands.describe(expression="Math expression, e.g. 2+3*4 or (10-2)/4")
    async def calculate(self, ctx: commands.Context, *, expression: str):
        expr = (expression or "").strip()
        if not expr:
            return await ctx.send("❌ Please provide an expression. Example: `cc2 calculate (2+3)*4`")

        if len(expr) > 120:
            return await ctx.send("❌ Expression is too long. Keep it under 120 characters.")

        try:
            result = _safe_calculate_expression(expr)
        except ZeroDivisionError:
            return await ctx.send("❌ Division by zero is not allowed.")
        except (SyntaxError, ValueError):
            return await ctx.send(
                "❌ Invalid expression. Use only numbers, parentheses, and operators: `+ - * / % **`."
            )
        except OverflowError:
            return await ctx.send("❌ Result is too large.")

        emb = discord.Embed(
            title="🧮 Calculator",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Expression", value=f"`{expr}`", inline=False)
        emb.add_field(name="Result", value=f"`{result:g}`", inline=False)
        emb.set_footer(text="CC2 Clash Bot • Calculator")
        await ctx.send(embed=emb)

    # ═══════════════════════════════════
    # /maintenance  +  cc2 maintenance
    # ═══════════════════════════════════
    @commands.hybrid_command(name="maintenance", aliases=["maint"], description="Enable/disable maintenance mode")
    @app_commands.describe(mode="on or off", message="Optional custom maintenance message")
    @app_commands.choices(
        mode=[
            app_commands.Choice(name="Enable", value="on"),
            app_commands.Choice(name="Disable", value="off"),
        ]
    )
    async def maintenance(self, ctx: commands.Context, mode: str = "on", *, message: Optional[str] = None):
        if not await self._ensure_leadership_ctx(ctx):
            return

        val = mode.lower() if isinstance(mode, str) else mode.value.lower()
        if val not in {"on", "off"}:
            return await ctx.send("❌ Mode must be `on` or `off`.")

        settings = load_settings()

        if val == "on":
            maintenance_text = (message or "").strip() or self.bot.maintenance_message or "🛠️ Bot is under maintenance. Please try again later."
            self.bot.maintenance_mode = True
            self.bot.maintenance_message = maintenance_text
            settings["maintenance_mode"] = True
            settings["maintenance_message"] = maintenance_text
            save_settings(settings)

            embed = discord.Embed(
                title="🛠️ Maintenance Mode Enabled",
                description=(
                    "Non-leadership users are now blocked from running commands.\n\n"
                    f"Message shown to users:\n{maintenance_text}"
                ),
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
        else:
            self.bot.maintenance_mode = False
            settings["maintenance_mode"] = False
            save_settings(settings)

            embed = discord.Embed(
                title="✅ Maintenance Mode Disabled",
                description="All users can run bot commands again.",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )

        embed.set_footer(text="CC2 Clash Bot • Maintenance")
        await ctx.send(embed=embed)
        await audit_log(
            self.bot,
            action="maintenance",
            actor=ctx.author,
            details=(
                f"mode={val}; message='{self.bot.maintenance_message}'"
                if val == "on" else "mode=off"
            ),
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    @commands.hybrid_command(name="maintstatus", aliases=["mstat"], description="Show current maintenance mode status")
    async def maintstatus(self, ctx: commands.Context):
        if not await self._ensure_leadership_ctx(ctx):
            return

        is_on = bool(getattr(self.bot, "maintenance_mode", False))
        msg = str(getattr(self.bot, "maintenance_message", "🛠️ Bot is under maintenance. Please try again later."))

        if is_on:
            embed = discord.Embed(
                title="🛠️ Maintenance Status: ON",
                description=(
                    "Non-leadership users are blocked from running commands.\n\n"
                    f"Current message:\n{msg}"
                ),
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
        else:
            embed = discord.Embed(
                title="✅ Maintenance Status: OFF",
                description="All users can run commands.",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )

        embed.set_footer(text="CC2 Clash Bot • Maintenance")
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="restart", aliases=["reboot"], description="Restart bot process (leadership)")
    @app_commands.describe(mode="Restart mode: auto, close, relaunch")
    async def restart(self, ctx: commands.Context, mode: str = "auto"):
        if not await self._ensure_leadership_ctx(ctx):
            return

        relaunch = _resolve_restart_relaunch(mode)
        if relaunch is None:
            return await ctx.send("❌ Invalid mode. Use: auto, close, relaunch.")

        resolved = "relaunch" if relaunch else "close"
        detail = (
            "Spawning a new process first, then closing current bot."
            if relaunch
            else "Closing current bot only; ensure a process supervisor auto-restarts it."
        )

        confirm_view = ConfirmDangerView(author_id=ctx.author.id)
        await ctx.send(
            f"⚠️ Restart confirmation required. Mode: **{resolved}**. {detail}\nProceed?",
            view=confirm_view,
            ephemeral=True,
        )
        timed_out = await confirm_view.wait()
        if timed_out or not confirm_view.confirmed:
            return

        await ctx.send(f"♻️ Restart requested by {ctx.author.mention}. Mode: **{resolved}**. {detail}")
        await audit_log(
            self.bot,
            action="restart",
            actor=ctx.author,
            details=f"requested_mode={mode} resolved_mode={resolved} guild={ctx.guild.id if ctx.guild else 'DM'}",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

        asyncio.create_task(self._restart_sequence(relaunch=relaunch))

    # ═══════════════════════════════════
    # /remind
    # ═══════════════════════════════════
    @commands.hybrid_command(name="remind", aliases=["rm"], description="Set a personal reminder")
    @app_commands.describe(message="Reminder message", duration="Duration like 30m, 2h, 1d, 1h30m")
    async def remind(self, ctx: commands.Context, message: str, duration: str):
        await ctx.defer(ephemeral=True)
        seconds = self._parse_duration_seconds(duration)
        if seconds is None:
            return await ctx.send("❌ Invalid duration. Examples: `30m`, `2h`, `1d`, `1h30m`.", ephemeral=True)

        # Keep short reminders in-memory (fast path), persist longer reminders.
        if seconds <= 600:
            task = asyncio.create_task(
                self._schedule_short_reminder(
                    user_id=ctx.author.id,
                    message=message,
                    delay_seconds=seconds,
                    channel_id=(ctx.channel.id if ctx.channel else None),
                )
            )
            self._short_reminder_tasks.add(task)
            task.add_done_callback(lambda t: self._short_reminder_tasks.discard(t))
            return await ctx.send(
                f"⏰ Reminder set for **{duration}** from now. I will DM you when it is due.",
                ephemeral=True,
            )

        due_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        reminder_id = create_personal_reminder(
            user_id=ctx.author.id,
            message=message,
            due_at_iso=due_at.isoformat(),
            channel_id=(ctx.channel.id if ctx.channel else None),
        )
        if not reminder_id:
            return await ctx.send("❌ Failed to store reminder. Please try again.", ephemeral=True)

        await ctx.send(
            f"✅ Reminder saved (ID `{reminder_id}`) for **{duration}** from now. "
            "I will deliver it even after restarts.",
            ephemeral=True,
        )

    # ═══════════════════════════════════
    # /clearbot  +  cc2 clearbot
    # ═══════════════════════════════════
    @commands.hybrid_command(name="clearbot", aliases=["cb"], description="Delete recent bot messages (admin only)")
    @app_commands.describe(limit="How many recent messages to check (max 500)")
    async def clearbot(self, ctx: commands.Context, limit: int = 200):
        if not await self._ensure_admin_ctx(ctx):
            return
        await ctx.defer(ephemeral=True)
        if limit > 500:
            limit = 500

        confirm_view = ConfirmDangerView(author_id=ctx.author.id)
        await ctx.send(
            f"⚠️ This will delete recent bot messages in this channel (scan limit: **{limit}**). Proceed?",
            view=confirm_view,
            ephemeral=True,
        )
        timed_out = await confirm_view.wait()
        if timed_out or not confirm_view.confirmed:
            return

        deleted = 0
        try:
            def _is_bot(m: discord.Message):
                return m.author.id == self.bot.user.id
            try:
                deleted_msgs = await ctx.channel.purge(limit=limit, check=_is_bot)
                deleted = len(deleted_msgs)
            except Exception:
                async for msg in ctx.channel.history(limit=limit):
                    if msg.author.id == self.bot.user.id:
                        try:
                            await msg.delete()
                            deleted += 1
                        except Exception:
                            await asyncio.sleep(0.05)
            await ctx.send(f"🧹 Deleted **{deleted}** bot messages.")
            await audit_log(
                self.bot,
                action="clearbot",
                actor=ctx.author,
                details=f"Deleted {deleted} bot messages in channel {getattr(ctx.channel, 'id', 'N/A')}.",
                audit_channel_id=AUDIT_CHANNEL_ID,
            )
        except Exception as e:
            await ctx.send(
                embed=build_error_embed(
                    "E-CLEARBOT-EXEC",
                    "Failed while deleting bot messages.",
                    "Check channel permissions and retry with a lower limit.",
                    context=str(e),
                )
            )

    # ═══════════════════════════════════
    # /clear  +  cc2 clear
    # ═══════════════════════════════════
    @commands.hybrid_command(name="clear", aliases=["cg"], description="Delete messages from a specific user")
    @app_commands.describe(user="User whose messages to delete", limit="How many recent messages to check (max 1000)")
    async def clear(self, ctx: commands.Context, user: discord.User, limit: int = 250):
        if not await self._ensure_admin_ctx(ctx):
            return

        # Try to get member from guild for better permission checks
        member = None
        if ctx.guild:
            try:
                member = await ctx.guild.fetch_member(user.id)
            except discord.NotFound:
                pass

        # Prevent deletion of messages from admin roles
        if member:
            if has_admin_role(member, BOT_ADMIN_ROLE_ID) or member.guild_permissions.administrator:
                await ctx.send(
                    embed=build_error_embed(
                        "E-CLEAR-TARGETADMIN",
                        "Cannot delete messages from users with admin permissions.",
                        "Choose a non-admin target user.",
                        context=f"target_user={user.id}",
                    )
                )
                return

        await ctx.defer(ephemeral=True)
        if limit > 1000:
            limit = 1000

        confirm_view = ConfirmDangerView(author_id=ctx.author.id)
        await ctx.send(
            (
                f"⚠️ This will delete messages from **{user.mention}** in this channel "
                f"(scan limit: **{limit}**). Proceed?"
            ),
            view=confirm_view,
            ephemeral=True,
        )
        timed_out = await confirm_view.wait()
        if timed_out or not confirm_view.confirmed:
            return

        deleted = 0

        try:
            def _is_user(m: discord.Message):
                return m.author.id == user.id

            try:
                deleted_msgs = await ctx.channel.purge(limit=limit, check=_is_user)
                deleted = len(deleted_msgs)
            except Exception:
                async for msg in ctx.channel.history(limit=limit):
                    if msg.author.id == user.id:
                        try:
                            await msg.delete()
                            deleted += 1
                        except Exception:
                            await asyncio.sleep(0.05)

            await ctx.send(f"🧹 Deleted **{deleted}** messages from **{user.mention}**.")
            await audit_log(
                self.bot,
                action="clear",
                actor=ctx.author,
                details=f"Deleted {deleted} messages from {user} in channel {getattr(ctx.channel, 'id', 'N/A')}.",
                audit_channel_id=AUDIT_CHANNEL_ID,
            )
        except Exception as e:
            await ctx.send(
                embed=build_error_embed(
                    "E-CLEAR-EXEC",
                    "Failed while deleting user messages.",
                    "Check channel permissions and retry with a lower limit.",
                    context=str(e),
                )
            )

    # ═══════════════════════════════════
    # /roster  +  cc2 roster
    # ═══════════════════════════════════
    @commands.hybrid_command(name="roster", aliases=["ros"], description="Export clan roster CSV")
    @commands.cooldown(1, 30, commands.BucketType.user)
    @app_commands.describe(clan="Clan to export (ALL CLANS to export all)")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def roster(self, ctx: commands.Context, clan: Optional[str] = None):
        """Export clan roster to CSV."""
        await ctx.defer()
        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for roster export",
                    include_all=True,
                )
                await ctx.send("Select a clan for roster export:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        if not clan or clan == "ALL":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["clan", "name", "tag", "townHall", "expLevel", "trophies", "role"])
            total_rows = 0
            preview_lines: List[str] = []
            for c in self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None):
                members = await self.bot.get_clan_member_list(c["tag"])
                if not members:
                    continue
                for m in members:
                    writer.writerow([c["name"], m.get("name"), m.get("tag"), m.get("townHallLevel"),
                                     m.get("expLevel"), m.get("trophies"), m.get("role")])
                    preview_lines.append(
                        f"• **{m.get('name', 'Unknown')}** `{m.get('tag', '')}` — "
                        f"{c['name']} • TH{m.get('townHallLevel', '?')} • 🏆 {int(m.get('trophies', 0) or 0):,}"
                    )
                    total_rows += 1
            if total_rows == 0:
                return await ctx.send("❌ No members found.")

            pages = build_paginated_embeds(
                title="📋 Roster Preview — All Clans",
                lines=preview_lines,
                color=discord.Color.blue(),
                per_page=12,
                footer_prefix="CC2 Clash Bot • Roster Preview",
            )
            await send_paginated_embeds(ctx, pages)

            output.seek(0)
            bio = io.BytesIO(output.getvalue().encode())
            bio.name = "roster_ALL.csv"
            return await ctx.send(file=discord.File(bio, filename=bio.name))

        clan_obj = _get_clan_by_tag(self.bot, clan)
        if not clan_obj:
            return await ctx.send("❌ Clan not found.")
        members = await self.bot.get_clan_member_list(clan_obj["tag"])
        if not members:
            return await ctx.send("❌ Empty or could not fetch.")
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["name", "tag", "townHall", "expLevel", "trophies", "role"])
        preview_lines: List[str] = []
        for m in members:
            writer.writerow([m.get("name"), m.get("tag"), m.get("townHallLevel"),
                             m.get("expLevel"), m.get("trophies"), m.get("role")])
            preview_lines.append(
                f"• **{m.get('name', 'Unknown')}** `{m.get('tag', '')}` — "
                f"TH{m.get('townHallLevel', '?')} • 🏆 {int(m.get('trophies', 0) or 0):,}"
            )

        pages = build_paginated_embeds(
            title=f"📋 Roster Preview — {clan_obj['name']}",
            lines=preview_lines,
            color=discord.Color.blue(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • Roster Preview",
        )
        await send_paginated_embeds(ctx, pages)

        output.seek(0)
        bio = io.BytesIO(output.getvalue().encode())
        bio.name = f"roster_{clan_obj['tag'].replace('#', '')}.csv"
        await ctx.send(file=discord.File(bio, filename=bio.name))

    # ═══════════════════════════════════
    # /kicksuggestions
    # ═══════════════════════════════════
    @commands.hybrid_command(name="kicksuggestions", aliases=["ks"], description="Show rush / missed war hit candidates")
    @commands.cooldown(1, 30, commands.BucketType.user)
    @app_commands.describe(clan="Clan to check; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def kicksuggestions(self, ctx: commands.Context, clan: Optional[str] = None):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for kick suggestions",
                    include_all=True,
                )
                await ctx.send("Select a clan for kick suggestions:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        output = await self._collect_kick_suggestion_lines(clans_to_check)

        if not output:
            await ctx.send("No kick suggestions. Clan looks good!")
        else:
            pages = build_paginated_embeds(
                title="⚠️ Kick Suggestions",
                lines=output,
                color=discord.Color.orange(),
                per_page=4,
                footer_prefix="CC2 Clash Bot • Kick Suggestions",
            )
            await send_paginated_embeds(ctx, pages)

        await audit_log(
            self.bot,
            action="kicksuggestions",
            actor=ctx.author,
            details=f"Ran kicksuggestions for clan={clan or 'ALL'}.",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /inactive
    # ═══════════════════════════════════
    @commands.hybrid_command(name="inactive", aliases=["ia"], description="Show members inactive for N+ days")
    @app_commands.checks.cooldown(1, 15.0)
    @app_commands.describe(
        clan="Clan to check; default = all",
        days="Days threshold (default from config)",
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def inactive(self, ctx: commands.Context, clan: Optional[str] = None, days: Optional[int] = None):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        # Prefix usability: allow `cc2 inactive 7` as days-only shorthand.
        if days is None and isinstance(clan, str):
            clan_token = clan.strip()
            if clan_token.isdigit():
                days = int(clan_token)
                clan = None
            elif clan_token.lower().endswith("d") and clan_token[:-1].isdigit():
                days = int(clan_token[:-1])
                clan = None

        threshold = max(1, int(days or INACTIVE_DAYS_THRESHOLD))

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for inactivity report",
                    include_all=True,
                )
                await ctx.send("Select a clan for inactivity report:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        activity = load_member_activity()
        now = datetime.now(timezone.utc)

        def _parse_activity_ts(raw: Any) -> Optional[datetime]:
            if not raw:
                return None
            try:
                dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except Exception:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        lines: List[str] = []
        total_flagged = 0
        total_members = 0
        for c in clans_to_check:
            clan_data = (activity.get(c["tag"], {}) if isinstance(activity, dict) else {}) or {}
            stale = []

            members = await self.bot.get_clan_member_list(c["tag"])
            members = members or []
            current_member_map = {m.get("tag"): m for m in members if m.get("tag")}
            total_members += len(current_member_map)

            untracked_count = 0
            for ptag, mrow in current_member_map.items():
                rec = clan_data.get(ptag, {}) if isinstance(clan_data.get(ptag, {}), dict) else {}
                if not rec:
                    untracked_count += 1
                    continue

                # Use the newest valid timestamp between progress and presence.
                # This avoids stale progress-only values falsely flagging active members.
                candidates = [
                    _parse_activity_ts(rec.get("last_progress_seen")),
                    _parse_activity_ts(rec.get("last_seen")),
                ]
                candidates = [dt for dt in candidates if dt is not None]
                if not candidates:
                    untracked_count += 1
                    continue

                last_dt = max(candidates)
                try:
                    inactive_days = max(0, (now - last_dt).days)
                    if inactive_days >= threshold:
                        level, icon = _inactive_severity(inactive_days, threshold)
                        stale.append((inactive_days, rec.get("name", mrow.get("name", ptag)), ptag, level, icon))
                except Exception:
                    continue

            stale.sort(key=lambda x: x[0], reverse=True)
            total_flagged += len(stale)
            lines.append(f"**{c['name']}** — {len(stale)} member(s) inactive {threshold}+ day(s) out of {len(current_member_map)}")
            if stale:
                risk_split = {"Critical": 0, "High": 0, "Watch": 0}
                for _, _, _, level, _ in stale:
                    risk_split[level] = int(risk_split.get(level, 0)) + 1
                lines.append(
                    "• Risk split: "
                    f"🔴 Critical {risk_split['Critical']} | "
                    f"🟠 High {risk_split['High']} | "
                    f"🟡 Watch {risk_split['Watch']}"
                )
                lines.append(
                    f"• Suggested action: {_inactive_action_hint(len(stale), len(current_member_map), threshold)}"
                )
            else:
                lines.append("• Status: ✅ No members above threshold in this clan.")

            for d, nm, tg, level, icon in stale[:100]:
                lines.append(f"• {icon} {nm} `{tg}` — {d}d ({level})")
            if untracked_count > 0:
                lines.append(f"• Tracking warm-up: {untracked_count} member(s) need more tracking data")

        if not lines:
            return await ctx.send("No data available for inactivity report.")

        lines.insert(0, f"Summary: **{total_flagged}** flagged across **{len(clans_to_check)}** clan(s), **{total_members}** total current members.")
        lines.insert(1, f"Recommendation: {_inactive_action_hint(total_flagged, total_members, threshold)}")

        pages = build_paginated_embeds(
            title=f"🕒 Inactivity Report ({threshold}+ days)",
            lines=lines,
            color=discord.Color.red(),
            per_page=16,
            footer_prefix="CC2 Clash Bot • Inactivity",
        )
        await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /clearcache
    # ═══════════════════════════════════
    @app_commands.command(name="clearcache", description="Clear in-memory API caches (admin)")
    @app_commands.describe(confirm="Set true to actually clear")
    async def clearcache(self, interaction: discord.Interaction, confirm: bool = False):
        if not await self._ensure_leadership_interaction(interaction):
            return

        stats = api_cache.get_stats()
        pending = len(getattr(request_deduplicator, "_pending", {}))

        if not confirm:
            return await interaction.response.send_message(
                f"⚠️ Dry run:\nAPI cache keys: **{stats.get('total_keys', 0)}**\n"
                f"Pending entries: **{pending}**\n\nRe-run with `confirm=true`.",
                ephemeral=True,
            )
        await interaction.response.send_message("🧹 Clearing…", ephemeral=False)
        await api_cache.clear()
        dd = await request_deduplicator.clear()
        await interaction.edit_original_response(
            content=f"✅ Cleared. Keys removed: **{stats.get('total_keys', 0)}**. "
                    f"Dedup: total={dd.get('total', 0)}, removed={dd.get('removed_done', 0)}"
        )
        await audit_log(
            self.bot,
            action="clearcache",
            actor=interaction.user,
            details=f"Cleared cache keys={stats.get('total_keys', 0)} dedup_removed={dd.get('removed_done', 0)}.",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /cleanup
    # ═══════════════════════════════════
    @app_commands.command(name="cleanup", description="Find/remove __pycache__ and compiled files (admin)")
    @app_commands.describe(force="Set true to delete; default = dry-run")
    async def cleanup(self, interaction: discord.Interaction, force: bool = False):
        if not await self._ensure_leadership_interaction(interaction):
            return

        root = Path(__file__).resolve().parent.parent
        await interaction.response.send_message("🔎 Scanning…", ephemeral=True)

        def _scan(root_path):
            pycache_dirs, compiled_files, total_size = [], [], 0
            for dirpath, _, filenames in os.walk(root_path):
                if ".git" in dirpath:
                    continue
                if "__pycache__" in dirpath:
                    pycache_dirs.append(dirpath)
                for fn in filenames:
                    if fn.endswith((".pyc", ".pyo")) or fn in ("Thumbs.db",):
                        fp = os.path.join(dirpath, fn)
                        try:
                            sz = os.path.getsize(fp)
                        except Exception:
                            sz = 0
                        compiled_files.append((fp, sz))
                        total_size += sz
            return pycache_dirs, compiled_files, total_size

        py_dirs, comp_files, total_size = await asyncio.to_thread(_scan, root)
        if not py_dirs and not comp_files:
            return await interaction.edit_original_response(content="✅ Nothing to clean.")

        if not force:
            lines = [f"Found {len(py_dirs)} __pycache__ dirs + {len(comp_files)} compiled files (~{total_size / 1024:.1f} KB)"]
            lines += py_dirs[:10] + [f"{p} ({s}B)" for p, s in comp_files[:10]]
            lines.append("\nUse `/cleanup force:true` to delete.")
            return await interaction.edit_original_response(content="\n".join(lines))

        def _delete(dirs, files):
            d, f_ = 0, 0
            for fp, _ in files:
                try:
                    os.remove(fp); d += 1
                except Exception:
                    pass
            for dp in dirs:
                try:
                    shutil.rmtree(dp); d += 1
                except Exception:
                    pass
            return d
        deleted = await asyncio.to_thread(_delete, py_dirs, comp_files)
        await interaction.edit_original_response(content=f"✅ Deleted {deleted} items (~{total_size / 1024:.1f} KB freed).")
        await audit_log(
            self.bot,
            action="cleanup",
            actor=interaction.user,
            details=f"Deleted {deleted} cleanup items. Freed about {total_size / 1024:.1f} KB.",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /syncroles
    # ═══════════════════════════════════
    @app_commands.command(name="syncroles", description="Sync TH roles (TH1–TH18) for linked players")
    @app_commands.checks.cooldown(1, 60.0)
    @app_commands.describe(clan_tag="Choose a clan or ALL")
    @app_commands.autocomplete(clan_tag=clan_autocomplete)
    async def syncroles(self, interaction: discord.Interaction, clan_tag: str):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Use in a server.", ephemeral=True)
        if not await self._ensure_leadership_interaction(interaction):
            return
        await interaction.response.send_message("🔄 Syncing TH roles…", ephemeral=True)
        guild = interaction.guild
        links = load_links()
        scoped_clans = self.bot.get_scoped_clans(interaction.guild.id if interaction.guild else None)
        target_tags = [c["tag"] for c in scoped_clans] if clan_tag == "ALL" else [clan_tag]

        updated, created = 0, 0
        for ctag in target_tags:
            members = await self.bot.get_clan_member_list(ctag)
            for m in members or []:
                clash_tag = _normalize_tag(m.get("tag", ""))
                th = m.get("townHallLevel")
                if not clash_tag or not th:
                    continue
                did = links.get(clash_tag)
                if not did:
                    continue
                member = guild.get_member(int(did))
                if not member:
                    continue
                role_name = f"TH{th}"
                role = discord.utils.get(guild.roles, name=role_name)
                if not role:
                    color = TH_COLORS.get(th, 0x95A5A6)
                    try:
                        role = await guild.create_role(name=role_name, color=discord.Color(color), reason="CC2 TH Sync")
                        created += 1
                    except Exception:
                        continue
                if role not in member.roles:
                    try:
                        await member.add_roles(role, reason="TH Sync")
                        updated += 1
                    except Exception:
                        pass
        await interaction.edit_original_response(
            content=f"🏰 **TH Role Sync**\n👤 Updated: **{updated}**\n🆕 Created: **{created}** roles"
        )
        await audit_log(
            self.bot,
            action="syncroles",
            actor=interaction.user,
            details=f"Synced roles for clan={clan_tag}; updated={updated}, created={created}.",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /addclan  /removeclan
    # ═══════════════════════════════════
    @app_commands.command(name="addclan", description="Add a new clan to the monitored list")
    @app_commands.describe(name="Clan name", tag="Clan tag (e.g. #PQUCURCQ)")
    async def addclan(self, interaction: discord.Interaction, name: str, tag: str):
        if not await self._ensure_leadership_interaction(interaction):
            return
        await interaction.response.send_message("➕ Adding…", ephemeral=True)
        tag_norm = _normalize_tag(tag)
        if not _is_valid_clan_tag(tag_norm):
            return await interaction.edit_original_response(content="❌ Invalid clan tag format. Use format like #PQUCURCQ.")
        gid = interaction.guild.id if interaction.guild else None
        if _get_clan_by_tag(self.bot, tag_norm, guild_id=gid):
            return await interaction.edit_original_response(content=f"❌ `{tag_norm}` already monitored.")
        import urllib.parse
        data = await self.bot.coc_get(f"/clans/{urllib.parse.quote(tag_norm)}")
        if not data:
            return await interaction.edit_original_response(content=f"❌ Could not validate `{tag_norm}` via API.")
        display = name.strip() or data.get("name") or "Unnamed"
        new_clan = {"name": display, "tag": tag_norm}
        if gid is None:
            self.bot.clans.append(new_clan)
            save_clans(self.bot.clans)
        else:
            scoped = load_guild_clans(gid)
            scoped.append(new_clan)
            save_guild_clans(gid, scoped)
        self.bot.strict_join_cache[tag_norm] = load_strict_cache(tag_norm)
        self._start_all_tasks(new_clan)
        scope_txt = "global" if gid is None else f"guild:{gid}"
        await interaction.edit_original_response(content=f"✅ Added **{display}** (`{tag_norm}`) to {scope_txt}.")
        await audit_log(
            self.bot,
            action="addclan",
            actor=interaction.user,
            details=f"Added clan {display} ({tag_norm}) scope={scope_txt}.",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    @app_commands.command(name="removeclan", description="Remove a clan from the monitored list")
    @app_commands.describe(clan="Clan to remove")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def removeclan(self, interaction: discord.Interaction, clan: str):
        if not await self._ensure_leadership_interaction(interaction):
            return
        await interaction.response.send_message("➖ Removing…", ephemeral=True)
        gid = interaction.guild.id if interaction.guild else None
        c_obj = _get_clan_by_tag(self.bot, clan, guild_id=gid)
        if not c_obj:
            return await interaction.edit_original_response(content="❌ Not found.")
        tag_norm = c_obj["tag"]
        name = c_obj["name"]
        if gid is None:
            self.bot.clans = [c for c in self.bot.clans if c["tag"].upper() != tag_norm.upper()]
            save_clans(self.bot.clans)
            self._stop_all_tasks(tag_norm)
            scope_txt = "global"
        else:
            scoped = [c for c in load_guild_clans(gid) if c.get("tag", "").upper() != tag_norm.upper()]
            save_guild_clans(gid, scoped)
            if not self.bot.is_clan_monitored_anywhere(tag_norm):
                self._stop_all_tasks(tag_norm)
            scope_txt = f"guild:{gid}"
        await interaction.edit_original_response(content=f"✅ Removed **{name}** (`{tag_norm}`) from {scope_txt}.")
        await audit_log(
            self.bot,
            action="removeclan",
            actor=interaction.user,
            details=f"Removed clan {name} ({tag_norm}) scope={scope_txt}.",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    def _start_all_tasks(self, clan: Dict[str, str]):
        for cog_name in ("Membership", "War", "Upgrades"):
            cog = self.bot.get_cog(cog_name)
            if cog and hasattr(cog, "start_tracking"):
                cog.start_tracking(clan)

    def _stop_all_tasks(self, clan_tag: str):
        for cog_name in ("Membership", "War", "Upgrades"):
            cog = self.bot.get_cog(cog_name)
            if cog and hasattr(cog, "stop_tracking"):
                cog.stop_tracking(clan_tag)

    # ═══════════════════════════════════
    # /setbase  /getbase  /basebook
    # ═══════════════════════════════════
    @commands.hybrid_command(name="addbase", aliases=["ab"], description="Save a base link for your account")
    @app_commands.describe(
        base_type="Type of base (war / legend / anti2 / blizzard)",
        link="Clash of Clans base link",
        name="Label for this base",
        tag="Player tag (optional, defaults to linked)",
        town_hall="Optional TH override, e.g. 16 or 17",
        style="Optional style code, e.g. HV",
        custom_section="When type=custom, choose section name (e.g. anti3 ring)",
    )
    @app_commands.choices(base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
        app_commands.Choice(name="Custom", value="custom"),
    ])
    async def addbase(
        self,
        ctx: commands.Context,
        base_type: str,
        link: str,
        name: str,
        tag: Optional[str] = None,
        town_hall: Optional[int] = None,
        style: Optional[str] = None,
        custom_section: Optional[str] = None,
    ):
        if ctx.guild:
            allowed = self._resolve_base_channel_ids(ctx.guild.id)
            if allowed and (ctx.channel is None or getattr(ctx.channel, "id", 0) not in allowed):
                return await ctx.send(
                    f"❌ Use this command in base channels: {self._format_channel_mentions(allowed)}"
                )

        base_type_norm = (base_type or "").strip().lower()
        try:
            section_key, section_label = self._resolve_base_section(base_type_norm, custom_section)
        except ValueError as ex:
            choices = ", ".join(list(BASE_TYPES) + ["custom"])
            return await ctx.send(f"❌ {ex} Choose one of: `{choices}`")

        link_norm = self._extract_base_layout_link(link)
        if not link_norm:
            return await ctx.send("❌ Invalid base link. Provide a Clash layout link with `action=OpenLayout`.")

        meta = self._extract_layout_meta_from_link(link_norm)
        th_val = town_hall if isinstance(town_hall, int) and town_hall > 0 else (
            meta.get("townHall") if isinstance(meta.get("townHall"), int) else None
        )
        if not isinstance(th_val, int):
            return await ctx.send("❌ Could not detect Town Hall from link. Please provide `town_hall`.")

        owner_key = self._resolve_base_owner_key(th_val)
        style_val = self._normalize_layout_style(style) or self._normalize_layout_style(meta.get("layoutStyle"))

        await ctx.send(
            (
                f"⚠️ Base will be saved in **TH{th_val if th_val else '?'}** section "
                f"(style **{style_val or 'N/A'}**, type **{section_label}**) for `{owner_key}`.\n"
                "Reply with `y` to confirm or `n` to cancel within 30s."
            )
        )

        def _check(msg: discord.Message) -> bool:
            if msg.author.id != ctx.author.id:
                return False
            if msg.channel.id != ctx.channel.id:
                return False
            return (msg.content or "").strip().lower() in {"y", "yes", "n", "no"}

        try:
            reply = await self.bot.wait_for("message", check=_check, timeout=30.0)
        except asyncio.TimeoutError:
            return await ctx.send("⏱️ Base save cancelled (no confirmation received).")

        if (reply.content or "").strip().lower() not in {"y", "yes"}:
            return await ctx.send("❌ Base save cancelled.")

        created, total = self._save_base_entry(
            owner_key=owner_key,
            base_type=section_key,
            link=link_norm,
            name=name.strip(),
            added_by_id=ctx.author.id,
            town_hall=th_val,
            layout_style=style_val,
        )

        if not created:
            return await ctx.send(
                f"ℹ️ This base link is already saved for {owner_key} under {section_label}."
            )

        emb = discord.Embed(title="✅ Base Saved", color=0x2ECC71, timestamp=datetime.now(timezone.utc))
        emb.add_field(name="Town Hall", value=owner_key, inline=True)
        emb.add_field(name="Type", value=section_label, inline=True)
        emb.add_field(name="Name", value=name.strip(), inline=False)
        emb.add_field(name="Link", value=link_norm, inline=False)
        emb.set_footer(text=f"Total {section_label} bases: {total}")
        await ctx.send(embed=emb)

    @app_commands.command(name="setbase", description="Save a base link for your account")
    @app_commands.describe(
        base_type="Type of base (war / legend / anti2 / blizzard)",
        link="Clash of Clans base link",
        name="Label for this base",
        tag="Player tag (optional, defaults to linked)",
    )
    @app_commands.choices(base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
    ])
    async def setbase(self, interaction: discord.Interaction, base_type: app_commands.Choice[str],
                      link: str, name: str, tag: Optional[str] = None):
        await interaction.response.send_message("📥 Saving…", ephemeral=True)
        link_norm = self._extract_base_layout_link(link)
        if not link_norm:
            return await interaction.edit_original_response(
                content="❌ Invalid base link. Provide a Clash layout link with `action=OpenLayout`."
            )

        meta = self._extract_layout_meta_from_link(link_norm)
        th_val = meta.get("townHall") if isinstance(meta.get("townHall"), int) else None
        if not isinstance(th_val, int):
            return await interaction.edit_original_response(
                content="❌ Could not detect Town Hall from link. Use `addbase` with `town_hall` override."
            )
        owner_key = self._resolve_base_owner_key(th_val)

        created, total = self._save_base_entry(
            owner_key=owner_key,
            base_type=base_type.value,
            link=link_norm,
            name=name.strip(),
            added_by_id=interaction.user.id,
            town_hall=th_val,
        )

        if not created:
            return await interaction.edit_original_response(
                content=f"ℹ️ This base link is already saved for {owner_key} under {base_type.value}."
            )

        emb = discord.Embed(title="✅ Base Saved", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
        emb.add_field(name="Town Hall", value=owner_key, inline=True)
        emb.add_field(name="Type", value=base_type.value, inline=True)
        emb.add_field(name="Name", value=name.strip(), inline=False)
        emb.add_field(name="Link", value=link_norm, inline=False)
        emb.set_footer(text=f"Total {base_type.value} bases: {total}")
        await interaction.edit_original_response(content="✅", embed=emb)

    @commands.hybrid_command(name="fetchbase", aliases=["fb"], description="Fetch saved base links by TH/style/type")
    @app_commands.describe(
        town_hall="Filter by town hall, e.g. 16",
        style="Filter by style code, e.g. HV",
        base_type="Filter by base type",
        tag="Player tag (optional, defaults to linked)",
    )
    @app_commands.choices(base_type=[
        app_commands.Choice(name="Any", value="any"),
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
        app_commands.Choice(name="Custom", value="custom"),
    ])
    async def fetchbase(
        self,
        ctx: commands.Context,
        town_hall: Optional[int] = None,
        style: Optional[str] = None,
        base_type: Optional[str] = "any",
        tag: Optional[str] = None,
    ):
        if ctx.guild:
            allowed = self._resolve_base_channel_ids(ctx.guild.id)
            if allowed and (ctx.channel is None or getattr(ctx.channel, "id", 0) not in allowed):
                return await ctx.send(
                    f"❌ Use this command in base channels: {self._format_channel_mentions(allowed)}"
                )

        if town_hall is not None and town_hall <= 0:
            return await ctx.send("❌ Town hall must be a positive number.")

        style_filter = self._normalize_layout_style(style)
        base_type_filter = (base_type or "any").strip().lower()
        if base_type_filter != "any" and base_type_filter not in set(BASE_TYPES) and base_type_filter != "custom":
            return await ctx.send(f"❌ Invalid base type filter: `{base_type_filter}`")

        bases = load_bases()
        if not isinstance(bases, dict) or not bases:
            return await ctx.send("⚠️ No bases found.")

        owner_keys: List[str] = []
        if town_hall is not None:
            owner_keys = [self._resolve_base_owner_key(town_hall)]
        elif tag:
            owner_keys = [_normalize_tag(tag)]
        else:
            owner_keys = [k for k in bases.keys() if str(k).upper().startswith("TH")]
            if not owner_keys:
                linked = get_linked_tag_for_user(ctx.author.id)
                if linked:
                    owner_keys = [linked]

        results: List[tuple[str, Dict[str, Any], str]] = []
        for owner in owner_keys:
            owner_bases = bases.get(owner, {}) if isinstance(bases, dict) else {}
            if not isinstance(owner_bases, dict):
                continue
            for bt, entries in owner_bases.items():
                if base_type_filter != "any":
                    if base_type_filter == "custom":
                        if not str(bt).startswith("custom:"):
                            continue
                    elif bt != base_type_filter:
                        continue
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    link_val = str(entry.get("link", "") or "").strip()
                    if not link_val:
                        continue
                    meta = self._extract_layout_meta_from_link(link_val)
                    entry_th = entry.get("townHall")
                    if not isinstance(entry_th, int):
                        entry_th = meta.get("townHall") if isinstance(meta.get("townHall"), int) else None

                    entry_style = self._normalize_layout_style(entry.get("layoutStyle"))
                    if not entry_style:
                        entry_style = self._normalize_layout_style(meta.get("layoutStyle"))

                    if town_hall is not None and entry_th != town_hall:
                        continue
                    if style_filter and entry_style != style_filter:
                        continue

                    enriched = dict(entry)
                    enriched["townHall"] = entry_th
                    enriched["layoutStyle"] = entry_style
                    results.append((bt, enriched, owner))

        if not results:
            filters = []
            if town_hall is not None:
                filters.append(f"TH{town_hall}")
            if style_filter:
                filters.append(style_filter)
            if base_type_filter != "any":
                filters.append(base_type_filter)
            filter_text = ", ".join(filters) if filters else "current filters"
            return await ctx.send(f"⚠️ No base links found with {filter_text}.")

        lines: List[str] = []
        for bt, e, owner in results[:50]:
            nm = str(e.get("name", "Unnamed") or "Unnamed")
            th_txt = f"TH{e.get('townHall')}" if isinstance(e.get("townHall"), int) else "TH?"
            style_txt = str(e.get("layoutStyle") or "N/A")
            lines.append(
                f"• **{nm}** | `{bt}` | `{th_txt}` | `{style_txt}` | `{owner}`\n{e.get('link', '(missing)')}"
            )

        pages = build_paginated_embeds(
            title="🏰 Base Links",
            lines=lines,
            color=discord.Color.blue(),
            per_page=8,
            footer_prefix="CC2 Clash Bot • Fetch Base",
        )
        await send_paginated_embeds(ctx, pages)

    @commands.hybrid_command(name="addattack", aliases=["aatk"], description="Save an attack strategy")
    @app_commands.describe(
        town_hall="Town Hall level, e.g. 16",
        style="Style code, e.g. HV, QC, SMASH",
        title="Short title",
        strategy="Attack strategy notes",
        link="Optional reference link (video/replay/guide)",
    )
    async def addattack(
        self,
        ctx: commands.Context,
        town_hall: int,
        style: str,
        title: str,
        strategy: str,
        link: Optional[str] = None,
    ):
        if town_hall < 1 or town_hall > 18:
            return await ctx.send("❌ Town hall must be between 1 and 18.")

        style_norm = self._normalize_layout_style(style)
        if not style_norm:
            return await ctx.send("❌ Please provide a valid style code.")

        strategy_text = (strategy or "").strip()
        if len(strategy_text) < 8:
            return await ctx.send("❌ Strategy text is too short.")

        title_text = (title or "").strip() or f"TH{town_hall} {style_norm}"
        ref_link = (link or "").strip() or None

        created, total = self._save_attack_strategy_entry(
            town_hall=town_hall,
            style=style_norm,
            title=title_text,
            strategy_text=strategy_text,
            link=ref_link,
            added_by_id=ctx.author.id,
        )

        if not created:
            return await ctx.send(
                f"ℹ️ This attack strategy already exists for `TH{town_hall}` `{style_norm}`."
            )

        emb = discord.Embed(
            title="✅ Attack Strategy Saved",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Town Hall", value=f"TH{town_hall}", inline=True)
        emb.add_field(name="Style", value=style_norm, inline=True)
        emb.add_field(name="Title", value=title_text, inline=False)
        emb.add_field(name="Strategy", value=strategy_text[:900], inline=False)
        if ref_link:
            emb.add_field(name="Link", value=ref_link, inline=False)
        emb.set_footer(text=f"Total TH{town_hall} {style_norm} strategies: {total}")
        await ctx.send(embed=emb)

    @commands.hybrid_command(name="fetchattack", aliases=["fatk"], description="Fetch attack strategies by TH/style")
    @app_commands.describe(
        town_hall="Filter by TH",
        style="Filter by style",
        query="Optional keyword in title/strategy",
        limit="Max entries (1-50)",
    )
    async def fetchattack(
        self,
        ctx: commands.Context,
        town_hall: Optional[int] = None,
        style: Optional[str] = None,
        query: Optional[str] = None,
        limit: Optional[int] = 20,
    ):
        if town_hall is not None and (town_hall < 1 or town_hall > 18):
            return await ctx.send("❌ Town hall must be between 1 and 18.")

        style_filter = self._normalize_layout_style(style)
        query_text = (query or "").strip().lower()
        max_rows = max(1, min(50, int(limit or 20)))

        rows = load_attack_strategies()
        if not isinstance(rows, list) or not rows:
            return await ctx.send("⚠️ No attack strategies saved yet.")

        matches: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_th = int(row.get("townHall", 0) or 0)
            row_style = self._normalize_layout_style(row.get("style")) or "GEN"
            if town_hall is not None and row_th != town_hall:
                continue
            if style_filter and row_style != style_filter:
                continue

            blob = f"{row.get('title', '')} {row.get('strategy', '')}".lower()
            if query_text and query_text not in blob:
                continue
            matches.append(row)

        if not matches:
            return await ctx.send("⚠️ No strategies match the selected filters.")

        matches.sort(key=lambda r: str(r.get("addedAt", "")), reverse=True)
        lines: List[str] = []
        for row in matches[:max_rows]:
            row_th = int(row.get("townHall", 0) or 0)
            row_style = self._normalize_layout_style(row.get("style")) or "GEN"
            title_text = str(row.get("title", "Strategy") or "Strategy")
            strat_text = str(row.get("strategy", "") or "")
            short = strat_text if len(strat_text) <= 280 else strat_text[:277] + "..."
            line = f"• **{title_text}** | `TH{row_th}` | `{row_style}`\n{short}"
            if row.get("link"):
                line += f"\n{row.get('link')}"
            lines.append(line)

        pages = build_paginated_embeds(
            title="⚔️ Attack Strategies",
            lines=lines,
            color=discord.Color.orange(),
            per_page=6,
            footer_prefix="CC2 Clash Bot • Fetch Attack",
        )
        await send_paginated_embeds(ctx, pages)

    @app_commands.command(name="getbase", description="Get a saved base link")
    @app_commands.describe(base_type="Type of base", town_hall="Town Hall, e.g. 17", tag="Legacy player tag (optional)")
    @app_commands.choices(base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
    ])
    async def getbase(
        self,
        interaction: discord.Interaction,
        base_type: app_commands.Choice[str],
        town_hall: Optional[int] = None,
        tag: Optional[str] = None,
    ):
        await interaction.response.send_message("📤 Fetching…", ephemeral=True)

        owner_key = self._resolve_base_owner_key(town_hall) if isinstance(town_hall, int) and town_hall > 0 else None
        if not owner_key:
            if tag:
                owner_key = _normalize_tag(tag)
            else:
                linked = get_linked_tag_for_user(interaction.user.id)
                owner_key = linked if linked else None
        if not owner_key:
            return await interaction.edit_original_response(content="❌ Provide `town_hall` (recommended) or a legacy tag.")

        bases = load_bases()
        entries = bases.get(owner_key, {}).get(base_type.value, [])
        if not entries:
            return await interaction.edit_original_response(content=f"⚠️ No `{base_type.value}` bases for `{owner_key}`.")
        entry = entries[-1]
        emb = discord.Embed(
            title=f"🏰 {base_type.value.capitalize()} Base — {entry.get('name', 'Unnamed')}",
            color=0x3498db, timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Owner", value=owner_key, inline=True)
        emb.add_field(name="Name", value=entry.get("name", "Unnamed"), inline=False)
        emb.add_field(name="Link", value=entry.get("link", "(missing)"), inline=False)
        emb.set_footer(text=f"{len(entries)} bases saved; showing latest.")
        await interaction.edit_original_response(content="✅", embed=emb)

    @app_commands.command(name="basebook", description="Show all saved bases for your account")
    @app_commands.describe(town_hall="Town Hall, e.g. 17", tag="Legacy player tag (optional)")
    async def basebook(self, interaction: discord.Interaction, town_hall: Optional[int] = None, tag: Optional[str] = None):
        await interaction.response.send_message("📚 Building…", ephemeral=True)

        owner_key = self._resolve_base_owner_key(town_hall) if isinstance(town_hall, int) and town_hall > 0 else None
        if not owner_key:
            if tag:
                owner_key = _normalize_tag(tag)
            else:
                linked = get_linked_tag_for_user(interaction.user.id)
                owner_key = linked if linked else None
        if not owner_key:
            return await interaction.edit_original_response(content="❌ Provide `town_hall` (recommended) or a legacy tag.")

        bases = load_bases()
        owner_bases = bases.get(owner_key, {})
        if not owner_bases:
            return await interaction.edit_original_response(content=f"⚠️ No bases for `{owner_key}`.")
        lines = []
        for t, entries in owner_bases.items():
            if not isinstance(entries, list) or not entries:
                continue
            lines.append(f"**{t.capitalize()} Bases:**")
            for e in entries[:10]:
                lines.append(f"• **{e.get('name', 'Unnamed')}** → {e.get('link', '(missing)')}")
            lines.append("")
        await interaction.edit_original_response(content=f"📚 **Base Book for `{owner_key}`**\n\n" + "\n".join(lines))

    # ═══════════════════════════════════
    # cc2 botstats / /botstats
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="botstats", aliases=["bs"],
        description="Show bot uptime, cache stats, and command usage.",
    )
    async def botstats(self, ctx: commands.Context):
        """Display bot health / diagnostics at a glance."""
        from cache import api_cache
        now = datetime.now(timezone.utc)
        uptime_delta = now - self.bot.start_time
        hours, rem = divmod(int(uptime_delta.total_seconds()), 3600)
        minutes, seconds = divmod(rem, 60)
        days, hours = divmod(hours, 24)

        uptime_parts = []
        if days:
            uptime_parts.append(f"{days}d")
        uptime_parts.append(f"{hours}h {minutes}m {seconds}s")
        uptime_str = " ".join(uptime_parts)

        cache_size = len(api_cache._cache) if hasattr(api_cache, "_cache") else "N/A"

        # Project line stats (Python source only)
        project_root = Path(__file__).resolve().parent.parent
        excluded_dirs = {
            ".git", ".venv", "venv", "__pycache__", "node_modules", "build",
            "dist", ".mypy_cache", ".pytest_cache",
        }
        py_files = 0
        code_lines = 0
        try:
            for root, dirs, files in os.walk(project_root):
                dirs[:] = [d for d in dirs if d not in excluded_dirs and not d.startswith(".")]
                for fn in files:
                    if not fn.endswith(".py"):
                        continue
                    py_files += 1
                    fp = Path(root) / fn
                    try:
                        with open(fp, "r", encoding="utf-8") as f:
                            for line in f:
                                s = line.strip()
                                if s and not s.startswith("#"):
                                    code_lines += 1
                    except Exception:
                        continue
        except Exception:
            py_files = 0
            code_lines = 0

        embed = discord.Embed(
            title="📊 Bot Statistics",
            color=0x2ECC71,
            timestamp=now,
        )
        embed.add_field(name="⏱️ Uptime", value=uptime_str, inline=True)
        embed.add_field(name="📡 Latency", value=f"{round(self.bot.latency * 1000)}ms", inline=True)
        embed.add_field(name="🏰 Tracked Clans", value=str(len(self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None))), inline=True)
        embed.add_field(name="💾 Cache Entries", value=str(cache_size), inline=True)
        embed.add_field(name="🌐 Guilds", value=str(len(self.bot.guilds)), inline=True)
        embed.add_field(name="📄 Python Files", value=str(py_files), inline=True)
        embed.add_field(name="🧠 Code Lines", value=f"{code_lines:,}", inline=True)
        embed.set_footer(text="CC2 Clash Bot")
        await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /findplayer
    # ═══════════════════════════════════
    @commands.hybrid_command(name="findplayer", aliases=["fp"], description="Find a player by partial name across all monitored clans")
    @app_commands.checks.cooldown(1, 10.0)
    @app_commands.describe(name="Full or partial player name", scope="guild or family")
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    async def findplayer(self, ctx: commands.Context, *, name: str, scope: str = "guild"):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        needle = (name or "").strip().lower()
        if not needle:
            return await ctx.send("❌ Please provide a player name to search.")

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.")

        matches: List[str] = []
        for c in _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val):
            members = await self.bot.get_clan_member_list(c["tag"])
            for m in members or []:
                nm = str(m.get("name", ""))
                if needle in nm.lower():
                    matches.append(
                        f"• **{nm}** `{m.get('tag', '')}` — {c['name']} (TH{m.get('townHallLevel', '?')})"
                    )

        if not matches:
            return await ctx.send("No matching players found for the selected scope.")

        pages = build_paginated_embeds(
            title=f"🔎 Find Player: {name} ({scope_val})",
            lines=matches,
            color=discord.Color.blue(),
            per_page=14,
            footer_prefix="CC2 Clash Bot • Find Player",
        )
        await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /familyreport
    # ═══════════════════════════════════
    @commands.hybrid_command(name="familyreport", aliases=["fr"], description="Show CC2 family-wide health summary")
    @app_commands.checks.cooldown(1, 20.0)
    @app_commands.describe(scope="guild or family")
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    async def familyreport(self, ctx: commands.Context, scope: str = "guild"):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.")

        family_totals = {
            "members": 0,
            "donations": 0,
            "rush_sum": 0.0,
            "rush_count": 0,
            "th_sum": 0,
            "th_count": 0,
        }

        embed = discord.Embed(
            title=f"🏰 CC2 Family Report ({scope_val})",
            color=discord.Color.gold(),
            timestamp=datetime.now(timezone.utc),
        )

        for c in _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val):
            members = await self.bot.get_clan_member_list(c["tag"])
            members = members or []
            tags = [m.get("tag") for m in members if m.get("tag")]
            players = await self.bot.fetch_players(tags)

            clan_members = len(members)
            clan_donations = 0
            clan_rush_sum = 0.0
            clan_rush_count = 0
            clan_th_sum = 0
            clan_th_count = 0

            for m in members:
                tag = m.get("tag")
                player = players.get(tag) if tag else None
                if not player:
                    continue
                clan_donations += int(player.get("donations", 0) or 0)
                th = int(player.get("townHallLevel", 0) or 0)
                if th > 0:
                    clan_th_sum += th
                    clan_th_count += 1
                rush = calculate_weighted_rush_score(player)
                if rush:
                    clan_rush_sum += float(rush.get("score", 0.0))
                    clan_rush_count += 1

            avg_th = (clan_th_sum / clan_th_count) if clan_th_count else 0.0
            avg_rush = (clan_rush_sum / clan_rush_count) if clan_rush_count else 0.0
            player_rows = [players[t] for t in tags if t in players and isinstance(players[t], dict)]
            war_win = self._recent_war_win_rate(c["tag"])
            raid_completion = self._recent_raid_completion_rate(c["tag"])
            health = calculate_clan_health_score(player_rows, war_win, raid_completion)

            embed.add_field(
                name=c["name"],
                value=(
                    f"👥 Members: **{clan_members}/50**\n"
                    f"🏰 Avg TH: **{avg_th:.2f}**\n"
                    f"💝 Season Donations: **{clan_donations:,}**\n"
                    f"⚡ Avg Rush Score: **{avg_rush:.2f}%**\n"
                    f"🩺 Health: **{health.get('score', 0):.1f}/100** ({health.get('tier', 'N/A')})"
                ),
                inline=False,
            )

            family_totals["members"] += clan_members
            family_totals["donations"] += clan_donations
            family_totals["rush_sum"] += clan_rush_sum
            family_totals["rush_count"] += clan_rush_count
            family_totals["th_sum"] += clan_th_sum
            family_totals["th_count"] += clan_th_count

            family_totals.setdefault("health_sum", 0.0)
            family_totals.setdefault("health_count", 0)
            family_totals["health_sum"] += float(health.get("score", 0.0) or 0.0)
            family_totals["health_count"] += 1

        fam_avg_th = (family_totals["th_sum"] / family_totals["th_count"]) if family_totals["th_count"] else 0.0
        fam_avg_rush = (family_totals["rush_sum"] / family_totals["rush_count"]) if family_totals["rush_count"] else 0.0
        fam_avg_health = (family_totals.get("health_sum", 0.0) / family_totals.get("health_count", 1)) if family_totals.get("health_count", 0) else 0.0

        embed.add_field(
            name="Family Totals",
            value=(
                f"👥 Total Members: **{family_totals['members']}**\n"
                f"🏰 Family Avg TH: **{fam_avg_th:.2f}**\n"
                f"💝 Combined Donations: **{family_totals['donations']:,}**\n"
                f"⚡ Family Avg Rush Score: **{fam_avg_rush:.2f}%**\n"
                f"🩺 Family Health: **{fam_avg_health:.1f}/100**"
            ),
            inline=False,
        )
        embed.set_footer(text="CC2 Clash Bot • Family Report")
        await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /clanhealth
    # ═══════════════════════════════════
    @commands.hybrid_command(name="clanhealth", aliases=["chl"], description="Show whole-clan aggregate health snapshot")
    @app_commands.checks.cooldown(1, 12.0)
    @app_commands.describe(clan="(Optional) specific clan tag", scope="guild or family")
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def clanhealth(self, ctx: commands.Context, clan: Optional[str] = None, scope: str = "guild"):
        await ctx.defer()

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.")

        if clan:
            clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
            if clans_to_check is None:
                return await ctx.send("❌ Clan not found.")
        else:
            clans_to_check = _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)
            if not clans_to_check:
                return await ctx.send("❌ No monitored clans available for this scope.")

        total_members = 0
        total_th_sum = 0
        total_th_count = 0
        total_rushed = 0
        total_war_eligible = 0
        total_donations = 0
        total_hero_sum = 0
        total_hero_count = 0
        total_health = 0.0
        total_health_count = 0

        lines: List[str] = []
        for c in clans_to_check:
            members = await self.bot.get_clan_member_list(c["tag"])
            members = members or []
            tags = [m.get("tag") for m in members if m.get("tag")]
            players = await self.bot.fetch_players(tags)

            member_count = len(members)
            th_sum = 0
            th_count = 0
            rushed = 0
            war_eligible = 0
            donations = 0
            hero_sum = 0
            hero_count = 0

            player_rows: List[Dict[str, Any]] = []
            for t in tags:
                p = players.get(t)
                if not isinstance(p, dict):
                    continue
                player_rows.append(p)

                th = int(p.get("townHallLevel", 0) or 0)
                if th > 0:
                    th_sum += th
                    th_count += 1
                    if th >= 10:
                        war_eligible += 1

                donations += int(p.get("donations", 0) or 0)

                heroes = extract_hero_levels(p)
                hsum = int(heroes.get("BK", 0) or 0) + int(heroes.get("AQ", 0) or 0) + int(heroes.get("GW", 0) or 0) + int(heroes.get("RC", 0) or 0)
                if hsum > 0:
                    hero_sum += hsum
                    hero_count += 1

                rush = calculate_weighted_rush_score(p)
                if isinstance(rush, dict) and bool(rush.get("is_rushed")):
                    rushed += 1

            avg_th = (th_sum / th_count) if th_count else 0.0
            avg_hero_sum = (hero_sum / hero_count) if hero_count else 0.0
            rushed_pct = (rushed / member_count * 100.0) if member_count else 0.0
            eligible_pct = (war_eligible / member_count * 100.0) if member_count else 0.0

            war_win = self._recent_war_win_rate(c["tag"])
            raid_completion = self._recent_raid_completion_rate(c["tag"])
            health = calculate_clan_health_score(player_rows, war_win, raid_completion)
            health_score = float(health.get("score", 0.0) or 0.0)
            health_tier = str(health.get("tier", "N/A") or "N/A")

            lines.append(f"**{c['name']}** `{c['tag']}`")
            lines.append(
                f"Members: **{member_count}/50** • Avg TH: **{avg_th:.2f}** • Avg Hero Sum: **{avg_hero_sum:.1f}**"
            )
            lines.append(
                f"Rushed: **{rushed}/{member_count} ({rushed_pct:.1f}%)** • War-eligible (TH10+): **{war_eligible}/{member_count} ({eligible_pct:.1f}%)**"
            )
            lines.append(
                f"Season Donations: **{donations:,}** • Health: **{health_score:.1f}/100** ({health_tier})"
            )
            lines.append("")

            total_members += member_count
            total_th_sum += th_sum
            total_th_count += th_count
            total_rushed += rushed
            total_war_eligible += war_eligible
            total_donations += donations
            total_hero_sum += hero_sum
            total_hero_count += hero_count
            total_health += health_score
            total_health_count += 1

        fam_avg_th = (total_th_sum / total_th_count) if total_th_count else 0.0
        fam_avg_hero = (total_hero_sum / total_hero_count) if total_hero_count else 0.0
        fam_rushed_pct = (total_rushed / total_members * 100.0) if total_members else 0.0
        fam_eligible_pct = (total_war_eligible / total_members * 100.0) if total_members else 0.0
        fam_health = (total_health / total_health_count) if total_health_count else 0.0

        lines.extend(
            [
                "**Family Totals**",
                (
                    f"Members: **{total_members}** • Avg TH: **{fam_avg_th:.2f}** • "
                    f"Avg Hero Sum: **{fam_avg_hero:.1f}**"
                ),
                (
                    f"Rushed: **{total_rushed}/{total_members} ({fam_rushed_pct:.1f}%)** • "
                    f"War-eligible (TH10+): **{total_war_eligible}/{total_members} ({fam_eligible_pct:.1f}%)**"
                ),
                f"Season Donations: **{total_donations:,}** • Family Health: **{fam_health:.1f}/100**",
            ]
        )

        pages = build_paginated_embeds(
            title=f"🩺 Clan Health Snapshot ({scope_val})",
            lines=lines,
            color=discord.Color.green(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • Clan Health",
        )
        await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /transferlog
    # ═══════════════════════════════════
    @commands.hybrid_command(name="transferlog", aliases=["tlog"], description="Show recent cross-clan transfer timeline")
    @app_commands.checks.cooldown(1, 10.0)
    @app_commands.describe(limit="How many events to show (default 10, max 30)", scope="guild or family")
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    async def transferlog(self, ctx: commands.Context, limit: int = 10, scope: str = "guild"):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer(ephemeral=True)

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send(
                embed=build_error_embed(
                    "E-TRANSFER-SCOPE",
                    "Invalid scope value.",
                    "Use `guild` or `family`.",
                    context=f"scope={scope}",
                ),
                ephemeral=True,
            )

        lim = max(1, min(limit, 30))
        data = load_transfers_data()
        events = data.get("events", []) if isinstance(data, dict) else []
        if not isinstance(events, list) or not events:
            return await ctx.send(
                embed=build_error_embed(
                    "E-TRANSFER-NODATA",
                    "No transfer events recorded yet.",
                    "Wait for transfer events to be logged, then rerun this command.",
                ),
                ephemeral=True,
            )

        allowed_tags = {str(c.get("tag", "")).upper() for c in _resolve_scope_clans(self.bot, (ctx.guild.id if ctx.guild else None), scope_val)}
        filtered: List[Dict[str, Any]] = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            from_tag = str((ev.get("from") or {}).get("tag", "")).upper()
            to_tag = str((ev.get("to") or {}).get("tag", "")).upper()
            if scope_val == "family" or from_tag in allowed_tags or to_tag in allowed_tags:
                filtered.append(ev)

        if not filtered:
            return await ctx.send(
                embed=build_error_embed(
                    "E-TRANSFER-SCOPEEMPTY",
                    "No transfer events found for the selected scope.",
                    "Try `scope=family` or increase the limit.",
                    context=f"scope={scope_val}",
                ),
                ephemeral=True,
            )

        filtered.sort(key=lambda e: str(e.get("timestamp", "")), reverse=True)
        rows = filtered[:lim]

        lines: List[str] = []
        for ev in rows:
            ts = str(ev.get("timestamp") or "")
            ts_short = ts.replace("T", " ")[:16] if ts else "unknown"
            tag = str(ev.get("player_tag") or "")
            frm = ev.get("from") or {}
            to = ev.get("to") or {}
            from_name = str(frm.get("name") or "Unknown")
            from_tag = str(frm.get("tag") or "")
            to_name = str(to.get("name") or "Unknown")
            to_tag = str(to.get("tag") or "")
            lines.append(
                f"• `{ts_short}` **{tag}** — **{from_name}** `{from_tag}` ➜ **{to_name}** `{to_tag}`"
            )

        pages = build_paginated_embeds(
            title=f"🔁 Transfer Log ({scope_val})",
            lines=lines,
            color=discord.Color.blurple(),
            per_page=12,
            footer_prefix="CC2 Clash Bot • Transfer Log",
        )
        await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /promotionsuggestions
    # ═══════════════════════════════════
    @commands.hybrid_command(name="promotionsuggestions", aliases=["ps"], description="Suggest members ready for promotion")
    @app_commands.describe(clan="Clan to analyze; default = all")
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def promotionsuggestions(self, ctx: commands.Context, clan: Optional[str] = None):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        if not clan:
            scoped = self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for promotion suggestions",
                    include_all=True,
                )
                await ctx.send("Select a clan for promotion suggestions:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = resolve_clans(self.bot, clan, guild_id=(ctx.guild.id if ctx.guild else None))
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        lines: List[str] = []
        for c in clans_to_check:
            members = await self.bot.get_clan_member_list(c["tag"])
            if not members:
                continue
            tags = [m.get("tag") for m in members if m.get("tag")]
            players = await self.bot.fetch_players(tags)

            scored = []
            for m in members:
                tag = m.get("tag")
                if not tag:
                    continue
                p = players.get(tag)
                if not p:
                    continue
                sugg = suggest_promotion(p)
                if float(sugg.get("readiness", 0.0)) >= 55.0:
                    scored.append((sugg, p))

            scored.sort(key=lambda x: x[0].get("readiness", 0.0), reverse=True)
            bucket_counts = {
                "Promote Now": 0,
                "Review Soon": 0,
                "Coach First": 0,
            }
            for sugg, _ in scored:
                label, _ = _promotion_confidence(float(sugg.get("readiness", 0.0) or 0.0))
                if label in bucket_counts:
                    bucket_counts[label] = int(bucket_counts.get(label, 0)) + 1

            lines.append(f"**{c['name']}** — {len(scored)} candidate(s) from {len(members)} member(s)")
            lines.append(
                "• Buckets: "
                f"🟢 Promote Now {bucket_counts['Promote Now']} | "
                f"🟡 Review Soon {bucket_counts['Review Soon']} | "
                f"🟠 Coach First {bucket_counts['Coach First']}"
            )
            for sugg, p in scored[:25]:
                reasons = sugg.get("reasons", []) if isinstance(sugg.get("reasons", []), list) else []
                blockers = sugg.get("blockers", []) if isinstance(sugg.get("blockers", []), list) else []
                reason_text = " | ".join(reasons[:3]) if reasons else "No reason data"
                blocker_text = (" • Blockers: " + ", ".join(blockers[:2])) if blockers else ""
                conf_label, conf_icon = _promotion_confidence(float(sugg.get("readiness", 0.0) or 0.0))
                action_text = _promotion_action_hint(float(sugg.get("readiness", 0.0) or 0.0), blockers)
                lines.append(
                    f"• **{p.get('name', 'Unknown')}** `{p.get('tag', '')}` — "
                    f"Readiness **{sugg.get('readiness', 0):.1f}/100** ({sugg.get('tier')})\n"
                    f"  {conf_icon} **{conf_label}** • {reason_text}{blocker_text}\n"
                    f"  🎯 {action_text}"
                )

            if not scored:
                lines.append("• No candidates above coaching threshold (55+ readiness).")

        if not lines:
            return await ctx.send("No promotion candidates found.")

        pages = build_paginated_embeds(
            title="⬆️ Promotion Suggestions",
            lines=lines,
            color=discord.Color.green(),
            per_page=14,
            footer_prefix="CC2 Clash Bot • Promotions",
        )
        await send_paginated_embeds(ctx, pages)

    # ═══════════════════════════════════
    # /poll
    # ═══════════════════════════════════
    @commands.hybrid_command(name="poll", aliases=["pl"], description="Create a reaction-based poll")
    @app_commands.describe(
        question="Poll question",
        options="Up to 5 options separated by semicolon ';'",
        duration="Poll duration like 30m, 2h, or 1d (default 1h)",
        mode="Voting mode: single or multi",
    )
    @app_commands.choices(
        mode=[
            app_commands.Choice(name="Single Choice", value="single"),
            app_commands.Choice(name="Multi Choice", value="multi"),
        ]
    )
    async def poll(self, ctx: commands.Context, question: str, options: str, duration: str = "1h", mode: str = "single"):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        choices = [o.strip() for o in (options or "").split(";") if o.strip()]
        if len(choices) < 2:
            return await ctx.send("❌ Provide at least 2 options separated by ';'.")
        if len(choices) > 5:
            return await ctx.send("❌ Maximum 5 options allowed.")

        mode_val = (mode or "single").strip().lower()
        if mode_val not in {"single", "multi"}:
            return await ctx.send("❌ Mode must be `single` or `multi`.")

        duration_seconds = _parse_poll_duration_seconds(duration)
        if duration_seconds is None:
            return await ctx.send("❌ Invalid duration. Use formats like `30m`, `2h`, or `1d`.")
        duration_seconds = max(60, min(int(duration_seconds), 7 * 24 * 3600))
        duration_label = _format_poll_duration_label(duration_seconds)
        minimum_votes = max(2, len(choices))
        emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
        created_at = datetime.now(timezone.utc)
        close_at = created_at + timedelta(seconds=duration_seconds)
        creator_name = getattr(ctx.author, "display_name", None) or str(ctx.author)

        async def _collect_counts(target_msg: discord.Message) -> List[int]:
            counts: List[int] = []
            if mode_val == "single":
                seen_users = set()
                for i in range(len(choices)):
                    vote_count = 0
                    emoji = emojis[i]
                    react_obj = next((r for r in target_msg.reactions if str(r.emoji) == emoji), None)
                    if react_obj is None:
                        counts.append(0)
                        continue
                    async for user in react_obj.users():
                        if user.bot:
                            continue
                        if user.id in seen_users:
                            continue
                        seen_users.add(user.id)
                        vote_count += 1
                    counts.append(vote_count)
                return counts

            for i in range(len(choices)):
                count = 0
                emoji = emojis[i]
                for r in target_msg.reactions:
                    if str(r.emoji) == emoji:
                        count = max(0, int(r.count) - 1)
                        break
                counts.append(count)
            return counts

        emb = _build_poll_embed(
            question=question,
            choices=choices,
            emojis=emojis,
            mode_val=mode_val,
            creator_name=creator_name,
            close_at=close_at,
            counts=[0 for _ in choices],
            is_closed=False,
            minimum_votes=minimum_votes,
        )

        msg = await ctx.send(embed=emb)
        for i in range(len(choices)):
            await msg.add_reaction(emojis[i])

        async def _poll_live_updater():
            last_signature: Optional[str] = None
            while True:
                remaining = (close_at - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    return
                try:
                    refreshed = await msg.channel.fetch_message(msg.id)
                except Exception:
                    return

                counts = await _collect_counts(refreshed)
                remaining_text = _format_poll_remaining(close_at)
                signature = f"{counts}|{remaining_text}"
                if signature != last_signature:
                    last_signature = signature
                    live_embed = _build_poll_embed(
                        question=question,
                        choices=choices,
                        emojis=emojis,
                        mode_val=mode_val,
                        creator_name=creator_name,
                        close_at=close_at,
                        counts=counts,
                        is_closed=False,
                        minimum_votes=minimum_votes,
                    )
                    try:
                        await msg.edit(embed=live_embed, view=None)
                    except Exception:
                        return
                await asyncio.sleep(60)

        async def _close_poll():
            await asyncio.sleep(max(1, int((close_at - datetime.now(timezone.utc)).total_seconds())))
            try:
                final_msg = await msg.channel.fetch_message(msg.id)
            except Exception:
                return

            counts = await _collect_counts(final_msg)
            total_votes = int(sum(counts)) if counts else 0
            is_inconclusive = total_votes < minimum_votes

            best = max(counts) if counts else 0
            winners = [choices[i] for i, c in enumerate(counts) if c == best and total_votes > 0]
            tie = len(winners) > 1

            closed_embed = _build_poll_embed(
                question=question,
                choices=choices,
                emojis=emojis,
                mode_val=mode_val,
                creator_name=creator_name,
                close_at=close_at,
                counts=counts,
                is_closed=True,
                minimum_votes=minimum_votes,
                winners=winners,
            )
            try:
                await msg.edit(embed=closed_embed, view=None)
            except Exception:
                pass

            result = discord.Embed(
                title="📊 Poll Result",
                description=f"**{question}**",
                color=(discord.Color.gold() if is_inconclusive else discord.Color.red()),
                timestamp=datetime.now(timezone.utc),
            )
            result.add_field(name="Mode", value=("Single Choice" if mode_val == "single" else "Multi Choice"), inline=True)
            result.add_field(name="Duration", value=duration_label, inline=True)
            result_lines = []
            for i, ch in enumerate(choices):
                result_lines.append(f"{emojis[i]} {ch} — **{counts[i]}** vote(s)")
            result.add_field(name="Results", value="\n".join(result_lines), inline=False)
            if is_inconclusive:
                result.add_field(
                    name="Outcome",
                    value=f"🟡 Inconclusive (minimum votes {minimum_votes}, got {total_votes})",
                    inline=False,
                )
            elif tie:
                result.add_field(name="Winner", value="Tie: " + ", ".join(f"**{w}**" for w in winners), inline=False)
            else:
                result.add_field(name="Winner", value=f"**{winners[0]}** ({best} vote(s))", inline=False)
            result.set_footer(text="CC2 Clash Bot • Poll")
            await msg.channel.send(embed=result)

        asyncio.create_task(_poll_live_updater())
        asyncio.create_task(_close_poll())

        await audit_log(
            self.bot,
            action="poll",
            actor=ctx.author,
            details=f"Question='{question}' options={len(choices)} duration={duration_label} mode={mode_val}",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /createevent
    # ═══════════════════════════════════
    @commands.hybrid_command(name="createevent", aliases=["ce"], description="Create a scheduled Discord event (admin only)")
    @app_commands.describe(
        title="Event title",
        start="UTC start time (YYYY-MM-DD HH:MM) or relative (in 2h)",
        duration_hours="Event duration in hours (1-24)",
        description="Optional event description",
        location="Optional location text (external event)",
    )
    async def createevent(
        self,
        ctx: commands.Context,
        title: str,
        start: str,
        duration_hours: int = 2,
        description: Optional[str] = None,
        location: Optional[str] = None,
    ):
        if not await self._ensure_admin_ctx(ctx):
            return
        if not ctx.guild:
            return await ctx.send("❌ This command can only be used in a server.")

        await ctx.defer()

        title_clean = (title or "").strip()
        if not title_clean:
            return await ctx.send("❌ Event title is required.")

        start_at = self._parse_event_start_utc(start)
        if start_at is None:
            return await ctx.send(
                "❌ Invalid start time. Use `YYYY-MM-DD HH:MM` (UTC) or relative format like `in 2h`."
            )

        now = datetime.now(timezone.utc)
        if start_at <= now + timedelta(minutes=2):
            return await ctx.send("❌ Start time must be at least 2 minutes in the future.")

        duration_hours = max(1, min(int(duration_hours or 2), 24))
        end_at = start_at + timedelta(hours=duration_hours)

        try:
            event = await ctx.guild.create_scheduled_event(
                name=title_clean[:100],
                start_time=start_at,
                end_time=end_at,
                entity_type=discord.EntityType.external,
                privacy_level=discord.PrivacyLevel.guild_only,
                location=(location or "CC2 Family Event")[:100],
                description=((description or "").strip() or None),
                reason=f"Created by {ctx.author} via createevent command",
            )
        except Exception as e:
            return await ctx.send(f"❌ Failed to create scheduled event: {e}")

        event_url = f"https://discord.com/events/{ctx.guild.id}/{event.id}"
        emb = discord.Embed(
            title="📅 Event Created",
            description=f"**{event.name}**",
            color=discord.Color.green(),
            timestamp=now,
        )
        emb.add_field(name="Starts (UTC)", value=f"`{start_at.strftime('%Y-%m-%d %H:%M')}`", inline=True)
        emb.add_field(name="Duration", value=f"`{duration_hours}h`", inline=True)
        emb.add_field(name="Link", value=event_url, inline=False)
        if event.description:
            emb.add_field(name="Description", value=event.description[:1024], inline=False)
        emb.set_footer(text="CC2 Clash Bot • Event")
        await ctx.send(embed=emb)

        await audit_log(
            self.bot,
            action="createevent",
            actor=ctx.author,
            details=(
                f"name='{event.name}' start_utc={start_at.isoformat()} "
                f"duration_hours={duration_hours} guild={ctx.guild.id}"
            ),
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /welcome  +  cc2 welcome
    # ═══════════════════════════════════
    @commands.hybrid_command(name="welcome", aliases=["wel"], description="Manually re-send the join embed for a player tag")
    @app_commands.describe(tag="Player tag (example: #2PQUE2J)")
    async def welcome(self, ctx: commands.Context, *, tag: str):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        from cogs.membership import JoinEmbedView

        tag_norm = _normalize_tag(tag)
        player = await self.bot.get_player(tag_norm)
        if not player:
            return await ctx.send(f"❌ Could not fetch player `{tag_norm}`.")

        clan_obj = player.get("clan") or {}
        clan_name = clan_obj.get("name") or "Unknown Clan"

        member_count = None
        clan_tag = _normalize_tag(clan_obj.get("tag") or "") if clan_obj.get("tag") else None
        if clan_tag:
            for tracked in self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None):
                if _normalize_tag(tracked.get("tag", "")) == clan_tag:
                    members = await self.bot.get_clan_member_list(clan_tag)
                    if isinstance(members, list):
                        member_count = len(members)
                    clan_name = tracked.get("name") or clan_name
                    break

        try:
            embed = build_join_embed(
                player,
                tag_norm,
                clan_name=clan_name,
                member_count=member_count,
            )
            view = JoinEmbedView()
            await ctx.send(embed=embed, view=view)
        except Exception as exc:
            return await ctx.send(f"❌ Failed to build welcome embed: {exc}")

        await audit_log(
            self.bot,
            action="welcome",
            actor=ctx.author,
            details=f"Re-fired join embed for tag={tag_norm} clan={clan_name}",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # /onboardingdm  +  cc2 onboardingdm
    # ═══════════════════════════════════
    @commands.hybrid_command(name="onboardingdm", aliases=["odm"], description="Send onboarding DM template to a member")
    @app_commands.describe(
        member="Server member to receive onboarding DM",
        force="Send even when onboarding_dm_enabled is off for this guild",
    )
    async def onboardingdm(self, ctx: commands.Context, member: discord.Member, force: bool = False):
        if not await self._ensure_leadership_ctx(ctx):
            return
        await ctx.defer()

        if getattr(member, "bot", False):
            return await ctx.send(
                embed=build_error_embed(
                    "E-ONBOARD-TARGET",
                    "Cannot send onboarding DM to a bot account.",
                    "Select a human guild member and run the command again.",
                    context=f"target={member.id}",
                )
            )

        membership_cog = self.bot.get_cog("Membership")
        if membership_cog is None or not hasattr(membership_cog, "send_onboarding_dm"):
            return await ctx.send(
                embed=build_error_embed(
                    "E-ONBOARD-COG",
                    "Membership module is not available.",
                    "Reload the bot so the Membership cog is loaded, then retry.",
                )
            )

        delivered = await membership_cog.send_onboarding_dm(member, force=bool(force))
        if not delivered:
            return await ctx.send(
                embed=build_error_embed(
                    "E-ONBOARD-DM",
                    "Onboarding DM could not be delivered.",
                    "Ask the member to enable DMs from server members, or retry with `force: true`.",
                    context=f"target={member.id} force={bool(force)}",
                )
            )

        await ctx.send(f"✅ Onboarding DM sent to {member.mention}.")
        await audit_log(
            self.bot,
            action="onboardingdm",
            actor=ctx.author,
            details=f"target={member.id} force={bool(force)} guild={ctx.guild.id if ctx.guild else 'DM'}",
            audit_channel_id=AUDIT_CHANNEL_ID,
        )

    # ═══════════════════════════════════
    # cc2 test-join (text-only)
    # ═══════════════════════════════════
    @commands.command(name="test-join", aliases=["testjoin", "tj"])
    async def test_join(self, ctx: commands.Context):
        """Preview the join embed using a real player from a tracked clan."""
        from cogs.membership import JoinEmbedView

        # Grab a real player from the first tracked clan so buttons work
        player = None
        clan_name = "Test Clan"
        tag = None
        for clan in self.bot.get_scoped_clans(ctx.guild.id if ctx.guild else None):
            members = await self.bot.get_clan_member_list(clan["tag"])
            if members:
                tag = members[0].get("tag")
                clan_name = clan.get("name", "Test Clan")
                if tag:
                    player = await self.bot.get_player(tag)
                    if player:
                        break

        if not player or not tag:
            return await ctx.send("❌ Could not fetch a player from any tracked clan.")

        try:
            embed = build_join_embed(player, tag, clan_name)
            view = JoinEmbedView()
            await ctx.send(embed=embed, view=view)
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")


async def setup(bot):
    await bot.add_cog(AdminCog(bot))
