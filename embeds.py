"""Discord embed builders for player/clan information."""
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import discord

from calculations import (
    extract_hero_levels,
    calculate_hero_rush,
    calculate_lab_rush,
    calculate_base_rush,
    calculate_weighted_rush_score,
    estimate_progression_speed,
)
from storage import get_linked_user_for_tag


def build_join_embed(
    player_json: Dict[str, Any],
    tag: str,
    clan_name: Optional[str] = None,
    member_count: Optional[int] = None,
    member_cap: int = 50,
    layout: str = "compact",
) -> discord.Embed:
    """Build embed for player join announcement.

    Args:
        player_json: Player data dictionary from the API.
        tag: Player tag string.
        clan_name: Name of the clan the player joined.
        layout: "compact" (default) or "detailed".
    """
    if layout.lower() == "detailed":
        return _build_join_embed_detailed(player_json, tag, clan_name, member_count=member_count, member_cap=member_cap)
    return _build_join_embed_compact(player_json, tag, clan_name, member_count=member_count, member_cap=member_cap)


# ───────────────────────────────────────────────────────────────────
# Join embed — COMPACT layout (default)
# ───────────────────────────────────────────────────────────────────

def _build_join_embed_compact(
    player_json: Dict[str, Any],
    tag: str,
    clan_name: Optional[str] = None,
    member_count: Optional[int] = None,
    member_cap: int = 50,
) -> discord.Embed:
    """Compact join embed — key info at a glance."""
    name = player_json.get("name", "Unknown")
    role = player_json.get("role", "Member")
    th = player_json.get("townHallLevel")
    xp = player_json.get("expLevel")
    trophies = player_json.get("trophies")
    war_stars = player_json.get("warStars")

    donations = player_json.get("donations", 0)
    received = player_json.get("donationsReceived", 0)
    attack_wins = player_json.get("attackWins", 0)
    defense_wins = player_json.get("defenseWins", 0)

    hero_levels = extract_hero_levels(player_json)

    troops = player_json.get("troops", []) or []
    spells = player_json.get("spells", []) or []
    pets = player_json.get("pets", []) or []

    troop_count = len(troops)
    spell_count = len(spells)
    pet_count = len(pets)

    maxed = sum(
        1 for t in troops
        if t.get("maxLevel") and t.get("level") and t["maxLevel"] == t["level"]
    )

    # Rush/color logic — weighted rush score
    rush = calculate_weighted_rush_score(player_json)
    is_rushed = rush["is_rushed"] if rush else False
    embed_color = discord.Color.red() if is_rushed else discord.Color.green()

    tag_display = tag if tag.startswith("#") else f"#{tag}"
    embed = discord.Embed(
        title=f"🟢 PLAYER JOINED — {name} ({tag_display})",
        description=(
            f"TH **{format_value(th)}** · XP **{format_value(xp)}** · 🏆 **{format_value(trophies)}** · ⚔️ **{format_value(war_stars)}** wars"
        ),
        color=embed_color,
        timestamp=datetime.now(timezone.utc)
    )

    # ───────── CLAN ─────────
    clan_lines = [f"Clan: **{clan_name or 'Unknown Clan'}**", f"Role: **{role}**"]
    if member_count is not None:
        clan_lines.append(f"Members: **{format_value(member_count)}/{format_value(member_cap)}**")
    embed.add_field(name="🏰 CLAN", value="\n".join(clan_lines), inline=False)

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── STATS ─────────
    embed.add_field(
        name="📊 STATS",
        value=(
            f"Donations: **{format_value(donations)}** | Received: **{format_value(received)}**\n"
            f"Attacks: **{format_value(attack_wins)}** | Defense: **{format_value(defense_wins)}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── HEROES ─────────
    hero_block = (
        f"👑 BK **{format_value(hero_levels.get('BK', 0))}** | 🏹 AQ **{format_value(hero_levels.get('AQ', 0))}**\n"
        f"🧙 GW **{format_value(hero_levels.get('GW', 0))}** | 🛡️ RC **{format_value(hero_levels.get('RC', 0))}**"
    )
    embed.add_field(name="🦸 HEROES", value=hero_block, inline=False)

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── ARMY ─────────
    embed.add_field(
        name="🧩 ARMY",
        value=(
            f"⚔️ Troops: **{format_value(troop_count)}** | 🔝 Maxed: **{format_value(maxed)}**\n"
            f"✨ Spells: **{format_value(spell_count)}** | 🐾 Pets: **{format_value(pet_count)}**"
        ),
        inline=False
    )

    embed.set_footer(text=f"CC2 Clash Bot — Player Joined • {tag_display}")
    return embed


# ───────────────────────────────────────────────────────────────────
# Join embed — DETAILED layout
# ───────────────────────────────────────────────────────────────────

def _build_join_embed_detailed(
    player_json: Dict[str, Any],
    tag: str,
    clan_name: Optional[str] = None,
    member_count: Optional[int] = None,
    member_cap: int = 50,
) -> discord.Embed:
    """Detailed join embed — full breakdown for leaders/co-leaders."""
    name = player_json.get("name", "Unknown")
    role = player_json.get("role", "Member")
    th = player_json.get("townHallLevel")
    xp = player_json.get("expLevel")
    trophies = player_json.get("trophies")
    best_trophies = player_json.get("bestTrophies", 0)
    war_stars = player_json.get("warStars")

    donations = player_json.get("donations", 0)
    received = player_json.get("donationsReceived", 0)
    attack_wins = player_json.get("attackWins", 0)
    defense_wins = player_json.get("defenseWins", 0)

    hero_levels = extract_hero_levels(player_json)

    troops = player_json.get("troops", []) or []
    spells = player_json.get("spells", []) or []
    pets = player_json.get("pets", []) or []

    troop_count = len(troops)
    spell_count = len(spells)
    pet_count = len(pets)

    maxed = sum(
        1 for t in troops
        if t.get("maxLevel") and t.get("level") and t["maxLevel"] == t["level"]
    )

    # Achievements
    achievements = player_json.get("achievements", []) or []
    attacks_won_lt = extract_achievement_value(achievements, "Conqueror") or 0
    defenses_won_lt = extract_achievement_value(achievements, "Unbreakable") or 0
    cwl_stars = extract_achievement_value(achievements, "War League Legend")
    clan_games_pts = extract_achievement_value(achievements, "Games Champion")
    capital_looted = extract_achievement_value(achievements, "Aggressive Capitalism")
    capital_contrib = extract_achievement_value(achievements, "Most Valuable Clanmate")
    gold_looted = extract_achievement_value(achievements, "Gold Grab")
    elixir_looted = extract_achievement_value(achievements, "Elixir Escapade")
    dark_elixir_looted = extract_achievement_value(achievements, "Heroic Heist")

    # Minion Prince / Battle Machine
    mp_level = None
    bm_level = 0
    if isinstance(player_json.get("heroes"), list):
        for h in player_json.get("heroes", []):
            nm = (h.get("name") or "").lower()
            if "minion prince" in nm:
                try:
                    mp_level = int(h.get("level") or 0)
                except Exception:
                    mp_level = h.get("level")
            if "battle machine" in nm:
                try:
                    bm_level = int(h.get("level") or 0)
                except Exception:
                    bm_level = h.get("level")

    # Rush analysis — weighted rush score
    rush = calculate_weighted_rush_score(player_json)
    is_rushed = rush["is_rushed"] if rush else False
    embed_color = discord.Color.red() if is_rushed else discord.Color.green()

    tag_display = tag if tag.startswith("#") else f"#{tag}"
    embed = discord.Embed(
        title=f"🟢 PLAYER JOINED — {name} ({tag_display})",
        description=(
            f"TH **{format_value(th)}** · XP **{format_value(xp)}** · "
            f"🏆 **{format_value(trophies)}** · 🥇 **{format_value(best_trophies)}** · "
            f"⚔️ **{format_value(war_stars)}** wars"
        ),
        color=embed_color,
        timestamp=datetime.now(timezone.utc)
    )

    # ───────── CLAN ─────────
    clan_lines = [f"Clan: **{clan_name or 'Unknown Clan'}**", f"Role: **{role}**"]
    if member_count is not None:
        clan_lines.append(f"Members: **{format_value(member_count)}/{format_value(member_cap)}**")
    embed.add_field(name="🏰 CLAN", value="\n".join(clan_lines), inline=False)

    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── SEASON STATS ─────────
    embed.add_field(
        name="📊 SEASON STATS",
        value=(
            f"🎁 Donated: **{format_value(donations)}** | 📥 Received: **{format_value(received)}**\n"
            f"⚔️ Attacks Won: **{format_value(attack_wins)}** | 🛡️ Defenses Won: **{format_value(defense_wins)}**"
        ),
        inline=False
    )

    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── LIFETIME STATS ─────────
    embed.add_field(
        name="🏆 LIFETIME STATS",
        value=(
            f"⚔️ Attacks Won: **{format_value(attacks_won_lt)}**\n"
            f"🛡️ Defenses Won: **{format_value(defenses_won_lt)}**\n"
            f"⭐ CWL Stars: **{format_value(cwl_stars)}**\n"
            f"🎯 Clan Games: **{format_value(clan_games_pts)}**\n"
            f"💰 Capital Looted: **{format_value(capital_looted)}**\n"
            f"💎 Capital Contributed: **{format_value(capital_contrib)}**"
        ),
        inline=False
    )

    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── TOTAL LOOT ─────────
    embed.add_field(
        name="💰 TOTAL LOOT (LIFETIME)",
        value=(
            f"🪙 Gold: **{format_loot(gold_looted)}**\n"
            f"🧪 Elixir: **{format_loot(elixir_looted)}**\n"
            f"🟣 Dark Elixir: **{format_loot(dark_elixir_looted)}**"
        ),
        inline=False
    )

    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── HEROES ─────────
    hero_block = (
        f"👑 Barbarian King: **{format_value(hero_levels.get('BK', 0))}**\n"
        f"🏹 Archer Queen: **{format_value(hero_levels.get('AQ', 0))}**\n"
        f"🧙 Grand Warden: **{format_value(hero_levels.get('GW', 0))}**\n"
        f"🛡️ Royal Champion: **{format_value(hero_levels.get('RC', 0))}**\n"
    )
    if mp_level is not None:
        hero_block += f"🐉 Minion Prince: **{format_value(mp_level)}**\n"
    if bm_level:
        hero_block += f"🤖 Battle Machine: **{format_value(bm_level)}**\n"
    embed.add_field(name="🦸 HEROES", value=hero_block.rstrip(), inline=False)

    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── ARMY ─────────
    embed.add_field(
        name="🧩 ARMY",
        value=(
            f"⚔️ Troops: **{format_value(troop_count)}** | 🔝 Maxed: **{format_value(maxed)}**\n"
            f"✨ Spells: **{format_value(spell_count)}** | 🐾 Pets: **{format_value(pet_count)}**"
        ),
        inline=False
    )

    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── RUSH STATUS ─────────
    if rush:
        rush_label = "🔴 RUSHED" if rush["is_rushed"] else "🟢 OK"
        # Safe formatting — pillar values may be "N/A"
        hero_val = f"**{rush['hero_gap']}%**" if rush['hero_gap'] != "N/A" else "**N/A**"
        lab_val = f"**{rush['lab_gap']}%**" if rush['lab_gap'] != "N/A" else "**N/A**"
        if rush.get("lab_estimated"):
            lab_val += " (est)"
        pet_val = f"**{rush['pet_gap']}%**" if rush['pet_gap'] != "N/A" else "**N/A**"
        wall_val = f"**{rush['wall_gap']}%**" if rush['wall_gap'] != "N/A" else "**N/A**"
        rush_lines = (
            f"Overall: **{rush['score']}%** ({rush_label})\n"
            f"⚡ Heroes: {hero_val} · Lab: {lab_val}\n"
            f"⚡ Pets: {pet_val}\n"
            f"⚡ Walls: {wall_val}"
        )
    else:
        rush_lines = "Rush data not available for this TH level."

    embed.add_field(
        name="⚡ RUSH STATUS",
        value=rush_lines,
        inline=False
    )

    embed.set_footer(text=f"CC2 Clash Bot — Player Joined • {tag_display}")
    return embed


def extract_achievement_value(achievements: List[Dict[str, Any]], achievement_name: str) -> int:
    """Extract value from achievements by name (case-insensitive)."""
    if not isinstance(achievements, list):
        return 0
    
    for ach in achievements:
        name = ach.get("name", "")
        if achievement_name.lower() in name.lower():
            try:
                return int(ach.get("value", 0))
            except (ValueError, TypeError):
                pass
    return 0


# --- Image helpers ---
IMAGE_CACHE: Dict[str, str] = {}
TOWNHALL_ICON_BASE = os.getenv("TOWNHALL_ICON_BASE", "https://raw.githubusercontent.com/cc2-assets/coc-icons/main/townhalls")


def _cache_image(key: str, url: str) -> str:
    IMAGE_CACHE[key] = url
    return url


def get_townhall_icon(th_level: int) -> Optional[str]:
    try:
        key = f"th_{th_level}"
        if key in IMAGE_CACHE:
            return IMAGE_CACHE[key]
        url = f"{TOWNHALL_ICON_BASE}/th{th_level}.png"
        return _cache_image(key, url)
    except Exception:
        return None


def get_league_icon(player: Dict[str, Any]) -> Optional[str]:
    # Prefer league icon from API response if available
    try:
        league = player.get("league") or {}
        urls = league.get("iconUrls") or {}
        url = urls.get("small") or urls.get("tiny") or urls.get("medium")
        if url:
            return url
    except Exception:
        pass
    return None


# --- Visual header helpers ---
BOLD_CAPS_START = 0x1D400

def format_value(value: Any, is_percentage: bool = False) -> str:
    """Format numeric values consistently across embeds.
    
    Rules:
    - >= 1,000,000 → X.XXM (e.g., 2,519,850 → 2.52M)
    - >= 1,000 → X.XXK (e.g., 110,200 → 110.2K)
    - < 1,000 → raw number
    - Percentages: Keep 2 decimal places (e.g., 69.12%)
    - None/missing → "N/A"
    - Zero → "0"
    - Negative → clamped to 0
    
    Args:
        value: Numeric value to format (int, float, or None)
        is_percentage: If True, format as percentage with 2 decimals
    
    Returns:
        Formatted string
    """
    # Handle None or missing values
    if value is None:
        return "N/A"
    
    try:
        # Convert to float for percentage calculations, int otherwise
        if is_percentage:
            val = float(value)
            # Clamp negative percentages to 0
            if val < 0:
                val = 0
            return f"{val:.2f}"
        else:
            val = int(value)
            # Clamp negative values to 0
            if val < 0:
                val = 0
            # Handle zero explicitly
            if val == 0:
                return "0"
            # Apply K/M formatting
            if val >= 1_000_000:
                return f"{val / 1_000_000:.2f}M"
            elif val >= 1_000:
                return f"{val / 1_000:.1f}K"
            else:
                return str(val)
    except (ValueError, TypeError):
        return "N/A"


def format_loot(value: int) -> str:
    """Legacy wrapper for format_value. Format loot values with appropriate units (M, K, etc)."""
    return format_value(value, is_percentage=False)


def _bold_upper(text: str) -> str:
    """Convert ASCII letters to mathematical bold uppercase for a headline effect.

    Only letters A-Z are transformed; digits and symbols are left unchanged.
    This simulates a stronger, larger headline within Discord's font constraints.
    """
    out = []
    for ch in (text or "").upper():
        if 'A' <= ch <= 'Z':
            out.append(chr(BOLD_CAPS_START + (ord(ch) - ord('A'))))
        else:
            out.append(ch)
    return ''.join(out)


def build_profile_embed_compact(player: Dict[str, Any], tag: str) -> discord.Embed:
    """Build compact profile embed for mobile/narrow screens."""
    name = player.get("name", "Unknown")
    tag_display = tag if tag.startswith("#") else f"#{tag}"
    
    xp = player.get("expLevel", "?")
    th = player.get("townHallLevel", "?")
    trophies = player.get("trophies", 0)
    best_trophies = player.get("bestTrophies", 0)
    
    season_donated = player.get("donations", 0)
    season_received = player.get("donationsReceived", 0)
    
    clan = player.get("clan", {}) or {}
    clan_name = clan.get("name", "No Clan")
    clan_role = player.get("role", "Member")
    clan_tag = clan.get("tag", "")
    
    achievements = player.get("achievements", []) or []
    attacks_won_lifetime = extract_achievement_value(achievements, "Conqueror") or 0
    defenses_won_lifetime = extract_achievement_value(achievements, "Unbreakable") or 0
    
    gold_looted = extract_achievement_value(achievements, "Gold Grab")
    elixir_looted = extract_achievement_value(achievements, "Elixir Escapade")
    dark_elixir_looted = extract_achievement_value(achievements, "Heroic Heist")
    
    hero_levels = extract_hero_levels(player)
    
    # Calculate rush status using weighted score
    rush = calculate_weighted_rush_score(player)
    is_rushed = rush["is_rushed"] if rush else False
    embed_color = discord.Color.red() if is_rushed else discord.Color.green()

    embed = discord.Embed(
        title=f"{name}  {tag_display}",
        description=(
            f"TH **{format_value(th)}** · XP **{format_value(xp)}** · 🏆 **{format_value(trophies)}**"
        ),
        color=embed_color
    )

    # ───────── ESSENTIALS ─────────
    embed.add_field(
        name="📊 ESSENTIALS",
        value=(
            f"🏰 TH: **{format_value(th)}** | 🎖️ Role: **{clan_role}**\n"
            f"🏰 Clan: **{clan_name}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── STATS ─────────
    embed.add_field(
        name="⚔️ STATS",
        value=(
            f"Donated: **{format_value(season_donated)}** | Received: **{format_value(season_received)}**\n"
            f"Attacks Won: **{format_value(attacks_won_lifetime)}** | Defenses Won: **{format_value(defenses_won_lifetime)}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── HEROES ─────────
    hero_block = (
        f"👑 BK **{format_value(hero_levels.get('BK', 0))}** | 🏹 AQ **{format_value(hero_levels.get('AQ', 0))}**\n"
        f"🧙 GW **{format_value(hero_levels.get('GW', 0))}** | 🛡️ RC **{format_value(hero_levels.get('RC', 0))}**"
    )
    embed.add_field(name="🦸 HEROES", value=hero_block, inline=False)

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── RUSH STATUS ─────────
    if rush:
        rush_label = "🔴 RUSHED" if rush["is_rushed"] else "🟢 OK"
        hero_v = f"**{rush['hero_gap']}%**" if rush['hero_gap'] != "N/A" else "**N/A**"
        lab_v = f"**{rush['lab_gap']}%**" if rush['lab_gap'] != "N/A" else "**N/A**"
        if rush.get("lab_estimated"):
            lab_v += " (est)"
        pet_v = f"**{rush['pet_gap']}%**" if rush['pet_gap'] != "N/A" else "**N/A**"
        rush_val = (
            f"Overall: **{rush['score']}%** ({rush_label})\n"
            f"Heroes {hero_v} · Lab {lab_v} · "
            f"Pets {pet_v}"
        )
    else:
        rush_val = "N/A"
    
    embed.add_field(
        name="⚡ RUSH STATUS",
        value=rush_val,
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── LINKED DISCORD ─────────
    linked_user_id = get_linked_user_for_tag(tag)
    linked_user = f"<@{linked_user_id}>" if linked_user_id else "Not linked"
    
    embed.add_field(
        name="🎮 DISCORD",
        value=linked_user,
        inline=False
    )

    embed.set_footer(text=f"CC2 Clash Bot • Profile (Compact) • {tag_display}")
    return embed


def build_profile_embed_detailed(player: Dict[str, Any], tag: str, streaks: Optional[Dict[str, Any]] = None) -> discord.Embed:
    """Build profile embed with simplified layout."""
    name = player.get("name", "Unknown")
    tag_display = tag if tag.startswith("#") else f"#{tag}"
    
    xp = player.get("expLevel", "?")
    th = player.get("townHallLevel", "?")
    trophies = player.get("trophies", 0)
    best_trophies = player.get("bestTrophies", 0)
    wars_played = player.get("warStars", 0)
    
    season_donated = player.get("donations", 0)
    season_received = player.get("donationsReceived", 0)
    season_attacks = player.get("attackWins", 0)
    season_defenses = player.get("defenseWins", 0)
    
    clan = player.get("clan", {}) or {}
    clan_name = clan.get("name", "No Clan")
    clan_role = player.get("role", "Member")
    clan_tag = clan.get("tag", "")
    
    achievements = player.get("achievements", []) or []
    attacks_won_lifetime = extract_achievement_value(achievements, "Conqueror") or 0
    defenses_won_lifetime = extract_achievement_value(achievements, "Unbreakable") or 0
    cwl_stars = extract_achievement_value(achievements, "War League Legend")
    clan_games_pts = extract_achievement_value(achievements, "Games Champion")
    capital_looted = extract_achievement_value(achievements, "Aggressive Capitalism")
    capital_contrib = extract_achievement_value(achievements, "Most Valuable Clanmate")
    
    gold_looted = extract_achievement_value(achievements, "Gold Grab")
    elixir_looted = extract_achievement_value(achievements, "Elixir Escapade")
    dark_elixir_looted = extract_achievement_value(achievements, "Heroic Heist")
    
    hero_levels = extract_hero_levels(player)
    mp_level = None
    bm_level = 0
    if isinstance(player.get("heroes"), list):
        for h in player.get("heroes", []):
            nm = (h.get("name") or "").lower()
            if "minion prince" in nm:
                try:
                    mp_level = int(h.get("level") or 0)
                except Exception:
                    mp_level = h.get("level")
            if "battle machine" in nm:
                try:
                    bm_level = int(h.get("level") or 0)
                except Exception:
                    bm_level = h.get("level")

    # Calculate color based on weighted rush score
    rush = calculate_weighted_rush_score(player)
    is_rushed = rush["is_rushed"] if rush else False
    embed_color = discord.Color.red() if is_rushed else discord.Color.green()

    embed = discord.Embed(
        title=f"{name}  {tag_display}",
        description=(
            f"XP **{format_value(xp)}** · "
            f"TH **{format_value(th)}** · "
            f"🏆 **{format_value(trophies)}** · "
            f"🥇 **{format_value(best_trophies)}** · "
            f"⚔️ Wars **{format_value(wars_played)}**"
        ),
        color=embed_color
    )

    # ───────── CORE STATS ─────────
    embed.add_field(
        name="🏆 CORE STATS",
        value=(
            f"🏰 Town Hall: **{format_value(th)}**\n"
            f"📈 XP Level: **{format_value(xp)}**\n"
            f"🏆 Current Trophies: **{format_value(trophies)}**\n"
            f"🥇 Best Trophies: **{format_value(best_trophies)}**\n"
            f"⚔️ Wars Played: **{format_value(wars_played)}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── SEASON STATS ─────────
    embed.add_field(
        name="📊 SEASON STATS",
        value=(
            f"🎁 Donated: **{format_value(season_donated)}**\n"
            f"📥 Received: **{format_value(season_received)}**\n"
            f"⚔️ Attacks Won: **{format_value(season_attacks)}**\n"
            f"🛡️ Defenses Won: **{format_value(season_defenses)}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── CLAN INFO ─────────
    embed.add_field(
        name="🏰 CLAN INFO",
        value=(
            f"🏷️ Clan: **{clan_name}**\n"
            f"🎖️ Role: **{clan_role}**\n"
            f"👥 Clan Tag: **#{clan_tag}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── LIFETIME STATS ─────────
    embed.add_field(
        name="🏆 LIFETIME STATS",
        value=(
            f"⚔️ Attacks Won: **{format_value(attacks_won_lifetime)}**\n"
            f"🛡️ Defenses Won: **{format_value(defenses_won_lifetime)}**\n"
            f"⭐ CWL Stars: **{cwl_stars}**\n"
            f"🎯 Clan Games: **{format_value(clan_games_pts)}**\n"
            f"💰 Capital Gold Looted: **{format_value(capital_looted)}**\n"
            f"💎 Capital Gold Contributed: **{format_value(capital_contrib)}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── TOTAL LOOT ─────────
    embed.add_field(
        name="💰 TOTAL LOOT (LIFETIME)",
        value=(
            f"🪙 Gold: **{format_loot(gold_looted)}**\n"
            f"🧪 Elixir: **{format_loot(elixir_looted)}**\n"
            f"🟣 Dark Elixir: **{format_loot(dark_elixir_looted)}**"
        ),
        inline=False
    )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── HEROES ─────────
    hero_block = (
        f"👑 Barbarian King: **{format_value(hero_levels.get('BK', 0))}**\n"
        f"🏹 Archer Queen: **{format_value(hero_levels.get('AQ', 0))}**\n"
        f"🧙 Grand Warden: **{format_value(hero_levels.get('GW', 0))}**\n"
        f"🛡️ Royal Champion: **{format_value(hero_levels.get('RC', 0))}**\n"
    )
    if mp_level is not None:
        hero_block += f"🐉 Minion Prince: **{format_value(mp_level)}**\n"
    if bm_level:
        hero_block += f"🤖 Battle Machine: **{format_value(bm_level)}**\n"
    
    embed.add_field(name="🦸 HEROES", value=hero_block.rstrip(), inline=False)

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── RUSH STATUS (SINGLE SECTION) ─────────
    if rush:
        rush_label = "🔴 RUSHED" if rush["is_rushed"] else "🟢 OK"
        hero_v = f"**{rush['hero_gap']}%**" if rush['hero_gap'] != "N/A" else "**N/A**"
        lab_v = f"**{rush['lab_gap']}%**" if rush['lab_gap'] != "N/A" else "**N/A**"
        if rush.get("lab_estimated"):
            lab_v += " (est)"
        pet_v = f"**{rush['pet_gap']}%**" if rush['pet_gap'] != "N/A" else "**N/A**"
        wall_v = f"**{rush['wall_gap']}%**" if rush['wall_gap'] != "N/A" else "**N/A**"
        rush_val = (
            f"Overall: **{rush['score']}%** ({rush_label})\n"
            f"⚡ Heroes: {hero_v} · Lab: {lab_v}\n"
            f"⚡ Pets: {pet_v}\n"
            f"⚡ Walls: {wall_v}"
        )
    else:
        rush_val = "Rush data not available for this TH level."
    
    embed.add_field(
        name="⚡ RUSH STATUS",
        value=rush_val,
        inline=False
    )

    offenders = (rush or {}).get("equipment_offenders", []) if rush else []
    if offenders:
        offender_lines: List[str] = []
        for row in offenders[:3]:
            bar_len = 10
            progress = max(0.0, min(100.0, float(row.get("progress_pct", 0.0) or 0.0)))
            filled = int((progress / 100.0) * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)
            offender_lines.append(
                f"• **{row.get('name', 'Unknown')}** "
                f"{row.get('level', 0)}/{row.get('max_level', 0)} "
                f"(gap {row.get('gap', 0)}) [{bar}]"
            )

        embed.add_field(
            name="🧰 EQUIPMENT OFFENDERS",
            value="\n".join(offender_lines),
            inline=False,
        )

    if isinstance(streaks, dict):
        war_streak = int(streaks.get("war_participation_streak", 0) or 0)
        raid_streak = int(streaks.get("raid_full_streak", 0) or 0)
        if war_streak > 0 or raid_streak > 0:
            embed.add_field(
                name="🔥 STREAKS",
                value=(
                    f"⚔️ War Participation: **{format_value(war_streak)}** consecutive wars\n"
                    f"🏰 Full Raid Completion: **{format_value(raid_streak)}** consecutive weekends"
                ),
                inline=False,
            )

    progression = estimate_progression_speed(player)
    if progression.get("available") and progression.get("timeline_lines"):
        lines = progression.get("timeline_lines", [])
        summary = (
            f"Estimated total progression: **~{format_value(progression.get('estimated_total_months', 0))} months**\n"
            f"Average pace: **{progression.get('pace', 'Balanced')}** ({progression.get('avg_months_per_th', 0):.2f} mo/TH)"
        )
        embed.add_field(
            name="⏳ TH PROGRESSION (EST.)",
            value=summary + "\n\nRecent TH timeline:\n" + "\n".join(f"• {row}" for row in lines),
            inline=False,
        )

    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)

    # ───────── LINKED DISCORD ─────────
    linked_user_id = get_linked_user_for_tag(tag)
    linked_user = f"<@{linked_user_id}>" if linked_user_id else "Not linked"
    
    embed.add_field(
        name="🎮 DISCORD",
        value=linked_user,
        inline=False
    )

    embed.set_footer(text=f"CC2 Clash Bot • Profile (Detailed) • {tag_display}")

    return embed


def build_info_embed(
    player: Dict[str, Any],
    tag: str,
    layout: str = "compact",
    streaks: Optional[Dict[str, Any]] = None,
) -> discord.Embed:
    """Wrapper to choose between compact and detailed layouts.
    
    Args:
        player: Player data dictionary
        tag: Player tag
        layout: "compact" (default) or "detailed"
    """
    if layout.lower() == "detailed":
        return build_profile_embed_detailed(player, tag, streaks=streaks)
    else:
        return build_profile_embed_compact(player, tag)


def build_leave_embed(tag: str, name: Optional[str] = None, member_count: Optional[int] = None, member_cap: int = 50) -> discord.Embed:
    """Build embed for player leave announcement."""
    tag_display = tag if tag.startswith("#") else f"#{tag}"
    embed = discord.Embed(
        title=f"{name or 'Player'}  {tag_display}",
        description="Player has left the clan.",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="🆔 Tag", value=f"`{tag_display}`", inline=True)
    if member_count is not None:
        embed.add_field(name="👥 Members", value=f"**{format_value(member_count)}/{format_value(member_cap)}**", inline=True)
    embed.set_footer(text="CC2 Clash Bot • Player Left")
    return embed


def build_donation_embed(player: Dict[str, Any], tag: str, lifetime: Dict[str, int], seasonal: int, received: int) -> discord.Embed:
    """Build donation stats embed matching /info style.
    
    Args:
        player: Player data dictionary
        tag: Player tag
        lifetime: Lifetime donations dict with keys: troops_donated, spells_donated, siege_donated, total_donated
        seasonal: Current season donations sent
        received: Current season donations received
    """
    name = player.get("name", "Unknown")
    tag_display = tag if tag.startswith("#") else f"#{tag}"
    
    embed = discord.Embed(
        title=f"{name}  {tag_display}",
        description=f"💝 **{format_value(seasonal)}** sent · **{format_value(received)}** received (this season)",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc)
    )
    
    # ───────── SEASONAL ─────────
    embed.add_field(
        name="📅 CURRENT SEASON",
        value=(
            f"Sent: **{format_value(seasonal)}**\n"
            f"Received: **{format_value(received)}**"
        ),
        inline=False
    )
    
    # Separator
    embed.add_field(name="`─────────────────`", value="", inline=False)
    
    # ───────── LIFETIME ─────────
    embed.add_field(
        name="🏆 LIFETIME DONATIONS",
        value=(
            f"Troops: **{format_value(lifetime.get('troops_donated', 0))}**\n"
            f"Spells: **{format_value(lifetime.get('spells_donated', 0))}**\n"
            f"Siege: **{format_value(lifetime.get('siege_donated', 0))}**\n"
            f"**Total: {format_value(lifetime.get('total_donated', 0))}**"
        ),
        inline=False
    )
    
    embed.set_footer(text="CC2 Clash Bot • Donation Stats")
    return embed


def build_compare_embed(
    player_a: Dict[str, Any],
    tag_a: str,
    player_b: Dict[str, Any],
    tag_b: str,
) -> discord.Embed:
    """Build side-by-side comparison embed for two players.

    Focuses on decision value by showing:
    - core progression metrics
    - activity and donation quality
    - clear per-metric winners + overall summary
    """
    from calculations import calculate_weighted_rush_score, calculate_activity_score

    def val(p: Dict[str, Any], key: str, default: int = 0) -> int:
        try:
            return int(p.get(key, default) or default)
        except (ValueError, TypeError):
            return default

    a_name = player_a.get("name", tag_a)
    b_name = player_b.get("name", tag_b)

    a_th = val(player_a, "townHallLevel")
    b_th = val(player_b, "townHallLevel")
    a_wars = val(player_a, "warStars")
    b_wars = val(player_b, "warStars")
    a_don = val(player_a, "donations")
    b_don = val(player_b, "donations")
    a_trophies = val(player_a, "trophies")
    b_trophies = val(player_b, "trophies")
    a_received = val(player_a, "donationsReceived")
    b_received = val(player_b, "donationsReceived")

    a_heroes = sum(extract_hero_levels(player_a).values())
    b_heroes = sum(extract_hero_levels(player_b).values())

    a_activity = float((calculate_activity_score(player_a) or {}).get("score", 0.0) or 0.0)
    b_activity = float((calculate_activity_score(player_b) or {}).get("score", 0.0) or 0.0)

    a_donation_ratio = float(a_don) / float(max(1, a_received))
    b_donation_ratio = float(b_don) / float(max(1, b_received))

    a_rush = calculate_weighted_rush_score(player_a)
    b_rush = calculate_weighted_rush_score(player_b)
    a_rush_score = float(a_rush.get("score", 100.0) if a_rush else 100.0)
    b_rush_score = float(b_rush.get("score", 100.0) if b_rush else 100.0)

    def mark_higher(a: float, b: float) -> tuple[str, str]:
        if a > b:
            return "✅", "❌"
        if b > a:
            return "❌", "✅"
        return "➖", "➖"

    def mark_lower(a: float, b: float) -> tuple[str, str]:
        if a < b:
            return "✅", "❌"
        if b < a:
            return "❌", "✅"
        return "➖", "➖"

    th_m_a, th_m_b = mark_higher(a_th, b_th)
    ws_m_a, ws_m_b = mark_higher(a_wars, b_wars)
    dn_m_a, dn_m_b = mark_higher(a_don, b_don)
    tr_m_a, tr_m_b = mark_higher(a_trophies, b_trophies)
    hr_m_a, hr_m_b = mark_higher(a_heroes, b_heroes)
    ac_m_a, ac_m_b = mark_higher(a_activity, b_activity)
    dr_m_a, dr_m_b = mark_higher(a_donation_ratio, b_donation_ratio)
    rs_m_a, rs_m_b = mark_lower(a_rush_score, b_rush_score)

    a_points = 0
    b_points = 0
    for left, right in [
        (th_m_a, th_m_b),
        (ws_m_a, ws_m_b),
        (dn_m_a, dn_m_b),
        (tr_m_a, tr_m_b),
        (hr_m_a, hr_m_b),
        (ac_m_a, ac_m_b),
        (dr_m_a, dr_m_b),
        (rs_m_a, rs_m_b),
    ]:
        if left == "✅":
            a_points += 1
        if right == "✅":
            b_points += 1

    if a_points > b_points:
        overall = f"🏆 **{a_name}** leads overall (**{a_points}-{b_points}** metric wins)"
    elif b_points > a_points:
        overall = f"🏆 **{b_name}** leads overall (**{b_points}-{a_points}** metric wins)"
    else:
        overall = f"🤝 Overall tie (**{a_points}-{b_points}** metric wins)"

    deciding_factors = [
        ("TH", abs(a_th - b_th), "higher"),
        ("War Stars", abs(a_wars - b_wars), "higher"),
        ("Donations", abs(a_don - b_don), "higher"),
        ("Trophies", abs(a_trophies - b_trophies), "higher"),
        ("Hero Levels", abs(a_heroes - b_heroes), "higher"),
        ("Activity", abs(a_activity - b_activity), "higher"),
        ("Donation Ratio", abs(a_donation_ratio - b_donation_ratio), "higher"),
        ("Rush Score", abs(a_rush_score - b_rush_score), "lower"),
    ]
    deciding_factors.sort(key=lambda row: row[1], reverse=True)

    def _winner_name(metric: str, preference: str) -> str:
        if metric == "TH":
            if a_th == b_th:
                return "Tie"
            return a_name if a_th > b_th else b_name
        if metric == "War Stars":
            if a_wars == b_wars:
                return "Tie"
            return a_name if a_wars > b_wars else b_name
        if metric == "Donations":
            if a_don == b_don:
                return "Tie"
            return a_name if a_don > b_don else b_name
        if metric == "Trophies":
            if a_trophies == b_trophies:
                return "Tie"
            return a_name if a_trophies > b_trophies else b_name
        if metric == "Hero Levels":
            if a_heroes == b_heroes:
                return "Tie"
            return a_name if a_heroes > b_heroes else b_name
        if metric == "Activity":
            if abs(a_activity - b_activity) < 1e-9:
                return "Tie"
            return a_name if a_activity > b_activity else b_name
        if metric == "Donation Ratio":
            if abs(a_donation_ratio - b_donation_ratio) < 1e-9:
                return "Tie"
            return a_name if a_donation_ratio > b_donation_ratio else b_name
        if metric == "Rush Score":
            if abs(a_rush_score - b_rush_score) < 1e-9:
                return "Tie"
            return a_name if a_rush_score < b_rush_score else b_name
        return "Tie"

    decision_lines: List[str] = []
    for metric, gap, preference in deciding_factors[:3]:
        winner = _winner_name(metric, preference)
        if winner == "Tie" or gap <= 0:
            decision_lines.append(f"• {metric}: **Tie**")
        else:
            suffix = "(lower is better)" if metric == "Rush Score" else ""
            decision_lines.append(f"• {metric}: **{winner}** leads by `{gap:,.2f}` {suffix}".strip())

    war_score_a = (
        (a_th * 1.5)
        + (a_heroes * 0.8)
        + (a_wars * 0.1)
        + (a_activity * 5.0)
        - (a_rush_score * 1.2)
    )
    war_score_b = (
        (b_th * 1.5)
        + (b_heroes * 0.8)
        + (b_wars * 0.1)
        + (b_activity * 5.0)
        - (b_rush_score * 1.2)
    )
    support_score_a = (a_don * 0.2) + (a_donation_ratio * 15.0) + (a_activity * 6.0)
    support_score_b = (b_don * 0.2) + (b_donation_ratio * 15.0) + (b_activity * 6.0)
    push_score_a = (a_trophies * 0.05) + (a_activity * 6.0) - (a_rush_score * 0.8)
    push_score_b = (b_trophies * 0.05) + (b_activity * 6.0) - (b_rush_score * 0.8)

    def _winner(a_val: float, b_val: float) -> str:
        if abs(a_val - b_val) < 1e-9:
            return "Tie"
        return a_name if a_val > b_val else b_name

    fit_lines = [
        f"⚔️ War attacker fit: **{_winner(war_score_a, war_score_b)}**",
        f"🎁 Support donor fit: **{_winner(support_score_a, support_score_b)}**",
        f"🏆 Trophy push fit: **{_winner(push_score_a, push_score_b)}**",
    ]

    delta_lines = [
        f"TH: `{a_th - b_th:+}`",
        f"War Stars: `{a_wars - b_wars:+,}`",
        f"Donations: `{a_don - b_don:+,}`",
        f"Trophies: `{a_trophies - b_trophies:+,}`",
        f"Hero Levels: `{a_heroes - b_heroes:+}`",
        f"Activity: `{a_activity - b_activity:+.2f}`",
        f"Donation Ratio: `{a_donation_ratio - b_donation_ratio:+.2f}`",
        f"Rush Score: `{a_rush_score - b_rush_score:+.2f}%` (lower is better)",
    ]

    embed = discord.Embed(
        title="⚖️ Player Comparison",
        color=discord.Color.blurple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.description = overall
    embed.add_field(
        name=f"{a_name} `{tag_a}`",
        value=(
            f"TH: **{a_th}** {th_m_a}\n"
            f"War Stars: **{a_wars:,}** {ws_m_a}\n"
            f"Donations: **{a_don:,}** {dn_m_a}\n"
            f"Trophies: **{a_trophies:,}** {tr_m_a}\n"
            f"Hero Levels: **{a_heroes:,}** {hr_m_a}\n"
            f"Activity: **{a_activity:.2f}** {ac_m_a}\n"
            f"Donation Ratio: **{a_donation_ratio:.2f}x** {dr_m_a}\n"
            f"Rush Score: **{a_rush_score:.2f}%** {rs_m_a}"
        ),
        inline=True,
    )
    embed.add_field(
        name=f"{b_name} `{tag_b}`",
        value=(
            f"TH: **{b_th}** {th_m_b}\n"
            f"War Stars: **{b_wars:,}** {ws_m_b}\n"
            f"Donations: **{b_don:,}** {dn_m_b}\n"
            f"Trophies: **{b_trophies:,}** {tr_m_b}\n"
            f"Hero Levels: **{b_heroes:,}** {hr_m_b}\n"
            f"Activity: **{b_activity:.2f}** {ac_m_b}\n"
            f"Donation Ratio: **{b_donation_ratio:.2f}x** {dr_m_b}\n"
            f"Rush Score: **{b_rush_score:.2f}%** {rs_m_b}"
        ),
        inline=True,
    )
    embed.add_field(name="Δ (A - B)", value="\n".join(delta_lines), inline=False)
    embed.add_field(name="🧭 Decision Summary", value="\n".join(decision_lines), inline=False)
    embed.add_field(name="🎯 Best Fit Recommendations", value="\n".join(fit_lines), inline=False)
    embed.set_footer(text="CC2 Clash Bot • Compare")
    return embed


