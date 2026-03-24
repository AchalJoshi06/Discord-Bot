"""Discord embed builders for player/clan information."""
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import discord

from calculations import extract_hero_levels


def build_join_embed(
    player_json: Dict[str, Any],
    tag: str,
    clan_name: Optional[str] = None
) -> discord.Embed:
    """Build embed for player join announcement."""
    name = player_json.get("name", "Unknown")
    role = player_json.get("role", "Member")
    th = player_json.get("townHallLevel", "?")
    xp = player_json.get("expLevel", "?")
    trophies = player_json.get("trophies", "?")
    war_stars = player_json.get("warStars", "?")
    
    donations = player_json.get("donations", 0)
    received = player_json.get("donationsReceived", 0)
    attack_wins = player_json.get("attackWins", "?")
    defense_wins = player_json.get("defenseWins", "?")
    
    hero_levels = extract_hero_levels(player_json)
    hero_summary = (
        f"👑 BK {hero_levels.get('BK', 0)}   "
        f"👸 AQ {hero_levels.get('AQ', 0)}   "
        f"🧙 GW {hero_levels.get('GW', 0)}   "
        f"🛡 RC {hero_levels.get('RC', 0)}"
    )
    
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
    
    troop_sum = f"⚔️ {troop_count} troops • 🔝 {maxed} maxed"
    spell_sum = f"✨ {spell_count} spells"
    pet_sum = f"🐾 {pet_count} pets" if pet_count else "🐾 None"
    
    embed = discord.Embed(
        title=f"🟢 PLAYER JOINED — {name}",
        color=0x00b894,
        timestamp=datetime.now(timezone.utc)
    )
    
    embed.add_field(name="🏰 Clan", value=f"**{clan_name or 'Unknown Clan'}** ({role})", inline=False)
    embed.add_field(name="🆔 Tag", value=f"`{tag}`", inline=True)
    embed.add_field(name="🏛 Town Hall", value=str(th), inline=True)
    embed.add_field(name="🎖 XP", value=str(xp), inline=True)
    
    embed.add_field(name="🏆 Trophies", value=str(trophies), inline=True)
    embed.add_field(name="⭐ War Stars", value=str(war_stars), inline=True)
    
    embed.add_field(
        name="📤 Donations (Season)",
        value=f"{donations} sent / {received} received",
        inline=False
    )
    
    embed.add_field(
        name="⚔️ War Record",
        value=f"Attacks: {attack_wins} • Defense: {defense_wins}",
        inline=False
    )
    
    embed.add_field(name="🦸 Heroes", value=hero_summary, inline=False)
    embed.add_field(
        name="🧩 Troops / Spells / Pets",
        value=f"{troop_sum}\n{spell_sum}\n{pet_sum}",
        inline=False
    )
    
    embed.set_footer(text="CC2 Clash Bot — Welcome! • Auto-generated")
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



def build_info_embed(player: Dict[str, Any], tag: str) -> discord.Embed:
    """Build comprehensive embed for player info command (like in-game profile).

    This version uses a compact, ClashPerk-style visual layout while preserving
    all existing data sources and calculations. It **adds** Minion Prince
    display (detected from `player["heroes"]`) without changing any existing
    calculations or command behavior.
    """
    name = player.get("name", "Unknown")
    th = player.get("townHallLevel", "?")
    xp = player.get("expLevel", "?")
    trophies = player.get("trophies", "?")
    best_trophies = player.get("bestTrophies", "?")
    war_stars = player.get("warStars", "?")
    role = player.get("role", "Member")

    # Clan info
    clan = player.get("clan", {})
    clan_name = clan.get("name", "No Clan")
    clan_tag = clan.get("tag", "")

    # Season stats
    donations = player.get("donations", 0)
    received = player.get("donationsReceived", 0)
    attack_wins = player.get("attackWins", 0)
    defense_wins = player.get("defenseWins", 0)

    # Extract lifetime stats from achievements
    achievements = player.get("achievements", []) or []

    # Lifetime donations
    troops_donated_lifetime = extract_achievement_value(achievements, "Friend in Need")
    spells_donated_lifetime = extract_achievement_value(achievements, "Sharing is Caring")
    siege_donated_lifetime = extract_achievement_value(achievements, "Siege Sharer")

    # Lifetime attacks/defense
    attacks_won_lifetime = extract_achievement_value(achievements, "Conqueror") or attack_wins
    defense_won_lifetime = extract_achievement_value(achievements, "Unbreakable") or defense_wins

    # CWL War Stars
    cwl_stars = extract_achievement_value(achievements, "War League Legend")

    # Clan Games
    clan_games = extract_achievement_value(achievements, "Games Champion")

    # Capital Gold
    capital_looted = extract_achievement_value(achievements, "Aggressive Capitalism")
    capital_contributed = extract_achievement_value(achievements, "Most Valuable Clanmate")

    # Total Loot (from achievements)
    gold_looted = extract_achievement_value(achievements, "Gold Grab")
    elixir_looted = extract_achievement_value(achievements, "Elixir Escapade")
    dark_elixir_looted = extract_achievement_value(achievements, "Heroic Heist")

    # Heroes (preserve existing extract_hero_levels for core heroes)
    hero_levels = extract_hero_levels(player)

    # Detect Minion Prince (name-based detection in heroes array)
    minion_prince_level: Optional[int] = None
    if isinstance(player.get("heroes"), list):
        for h in player.get("heroes", []):
            if "minion prince" in (h.get("name") or "").lower():
                try:
                    minion_prince_level = int(h.get("level", 0) or 0)
                except Exception:
                    minion_prince_level = h.get("level", 0)
                break

    # Battle Machine (Builder Base hero)
    battle_machine_level = 0
    if isinstance(player.get("heroes"), list):
        for h in player.get("heroes", []):
            if "battle machine" in (h.get("name") or "").lower():
                try:
                    battle_machine_level = int(h.get("level", 0) or 0)
                except Exception:
                    battle_machine_level = h.get("level", 0)
                break

    # Visual assets
    th_icon = None
    try:
        if isinstance(th, int):
            th_icon = get_townhall_icon(th)
    except Exception:
        th_icon = None

    league_icon = get_league_icon(player)

    # Create compact embed (ClashPerk-style)
    embed = discord.Embed(
        title=_bold_upper(name),
        description=f"`{tag}`",
        color=0x3498db,
        timestamp=datetime.now(timezone.utc)
    )

    # Use league badge as author icon and show tag there for quick glance
    if league_icon:
        embed.set_author(name=f"`{tag}`", icon_url=league_icon)
    else:
        embed.set_author(name=f"`{tag}`")

    if th_icon:
        embed.set_thumbnail(url=th_icon)

    # Header / Core stats (compact) — bold uppercase header with emoji
    core_line = f"XP: {xp} • TH: {th} • Trophies: {trophies:,} • Best: {best_trophies:,} • Wars: {war_stars:,}"
    embed.add_field(name=f"📊 {_bold_upper('CORE')}", value=core_line, inline=False)

    # spacer
    embed.add_field(name="\u200b", value="\u200b", inline=False)

    # Season & Other Stats (compact single line)
    season_line = f"Donated: {donations:,} • Received: {received:,} • Attacks: {attack_wins:,} • Defense: {defense_wins:,}"
    embed.add_field(name=f"📅 {_bold_upper('SEASON')}", value=season_line, inline=False)

    # spacer
    embed.add_field(name="\u200b", value="\u200b", inline=False)

    # Clan / Role / Last seen (single line)
    last_seen = player.get("lastSeen", None) or player.get("lastSeenTime", None) or "Unknown"
    clan_compact = f"{clan_name if clan_name != 'No Clan' else 'No Clan'}{(' `'+clan_tag+'`') if clan_tag else ''} • Role: {role} • Last Seen: {last_seen}"
    embed.add_field(name=f"🏰 {_bold_upper('CLAN')}", value=clan_compact, inline=False)

    # spacer
    embed.add_field(name="\u200b", value="\u200b", inline=False)

    # Achievement / Lifetime Stats — grouped lines (Donations / War / Capital)
    total_lifetime_donations = troops_donated_lifetime + spells_donated_lifetime + siege_donated_lifetime
    lifetime_groups = []
    lifetime_groups.append(f"Donations: {troops_donated_lifetime:,} • Spells: {spells_donated_lifetime:,} • Siege: {siege_donated_lifetime:,}")
    lifetime_groups.append(f"War: Attacks {attacks_won_lifetime:,} • Defense {defense_won_lifetime:,} • CWL {cwl_stars:,}")
    lifetime_groups.append(f"Capital: Looted {capital_looted:,} • Contributed {capital_contributed:,}")

    # Lifetime must be exactly 4 lines: Donations / War / Capital / Clan Games
    # Ensure clan_games line is always present (even if zero)
    clan_games_line = f"Clan Games: {clan_games:,}"
    lifetime_lines_fixed = [
        f"Donations: Troops {troops_donated_lifetime:,} • Spells {spells_donated_lifetime:,} • Siege {siege_donated_lifetime:,}",
        f"War: Attacks {attacks_won_lifetime:,} • Defense {defense_won_lifetime:,} • CWL {cwl_stars:,}",
        f"Capital: Looted {capital_looted:,} • Contributed {capital_contributed:,}",
        clan_games_line,
    ]
    embed.add_field(name=f"🏆 {_bold_upper('LIFETIME')}", value="\n".join(lifetime_lines_fixed), inline=False)

    # spacer between Lifetime and Total Loot
    embed.add_field(name="\u200b", value="\u200b", inline=False)

    # Total Loot (exactly 3 lines, no emojis inside lines)
    gold_b = gold_looted / 1_000_000_000 if gold_looted >= 1_000_000_000 else gold_looted / 1_000_000
    elixir_b = elixir_looted / 1_000_000_000 if elixir_looted >= 1_000_000_000 else elixir_looted / 1_000_000
    dark_m = dark_elixir_looted / 1_000_000 if dark_elixir_looted >= 1_000_000 else dark_elixir_looted / 1_000

    gold_unit = "B" if gold_looted >= 1_000_000_000 else "M"
    elixir_unit = "B" if elixir_looted >= 1_000_000_000 else "M"
    dark_unit = "M" if dark_elixir_looted >= 1_000_000 else "K"

    embed.add_field(
        name=f"💰 {_bold_upper('TOTAL LOOT (LIFETIME)')}",
        value=(
            f"Gold: {gold_b:.2f}{gold_unit}\n"
            f"Elixir: {elixir_b:.2f}{elixir_unit}\n"
            f"Dark Elixir: {dark_m:.2f}{dark_unit}"
        ),
        inline=False
    )

    # spacer
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    # Heroes: display icons (when available) inline with levels
    hero_entries: List[str] = []
    # Core heroes
    hero_entries.append(f"👑 BK {hero_levels.get('BK', 0)}")
    hero_entries.append(f"👸 AQ {hero_levels.get('AQ', 0)}")
    hero_entries.append(f"🧙 GW {hero_levels.get('GW', 0)}")
    hero_entries.append(f"🛡 RC {hero_levels.get('RC', 0)}")

    # Minion Prince (show only if present)
    if minion_prince_level is not None:
        hero_entries.append(f"🤴 MP {minion_prince_level}")

    # Battle Machine
    if battle_machine_level > 0:
        hero_entries.append(f"🤖 BM {battle_machine_level}")

    hero_display = " • ".join(hero_entries)
    embed.add_field(name=f"🦸 {_bold_upper('HEROES')}", value=hero_display, inline=False)

    embed.set_footer(text="CC2 Clash Bot • Comprehensive Profile")
    return embed


def build_leave_embed(tag: str, name: Optional[str] = None) -> discord.Embed:
    """Build embed for player leave announcement."""
    title = f"🔴 LEAVE — {name or tag}"
    embed = discord.Embed(
        title=title,
        color=0xe74c3c,
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
    return embed


