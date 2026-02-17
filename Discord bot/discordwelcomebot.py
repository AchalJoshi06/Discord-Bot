# Tokens and API keys are loaded from `config.py` / environment variables."

# --- imports ---
import discord
from discord import app_commands
import aiohttp
import asyncio
import urllib.parse
import re
import json
import os
import io
import csv
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import time
from cache import api_cache, request_deduplicator
from config import (
    DISCORD_TOKEN,
    MONTHLY_SNAPSHOT_DAY,
    PLAYER_CACHE_TTL,
    CLAN_CACHE_TTL,
    WAR_CACHE_TTL,
    COC_API_BASE_URL,
    COC_API_KEYS,
    COC_IP_DETECT_URL,
    COC_IP_CACHE_TTL,
    LEAVE_DEBOUNCE_COUNT,
    SKIP_EMPTY_MEMBER_LIST,
)
from donations import (
    extract_lifetime_donations,
    create_donation_snapshot,
    save_monthly_snapshot,
    get_donation_history,
    get_player_donation_stats,
    get_current_month_key,
)
# Reuse shared embed builder
from embeds import build_info_embed, _bold_upper

# ============================
# CONFIG / INTERVALS / FILES
# ============================
CHANNEL_ID = 1439346726048633053
LOG_CHANNEL_ID = CHANNEL_ID
ANNOUNCE_CHANNEL_ID = CHANNEL_ID

# intervals (seconds)
CHECK_INTERVAL = 5
WAR_POLL_INTERVAL = 6
REMINDER_INTERVAL = 2 * 60 * 60   # 2 hours
UPGRADE_CHECK_INTERVAL = 15 * 60  # 15 minutes
UPGRADE_ALERT_CHECK = 5 * 60      # 5 minutes

COC_CONCURRENCY = 6
COC_TIMEOUT = 12

STARTUP_BULK_LIMIT = 100

# file names
LINKS_FILE = "links.json"
NAME_CACHE_FILE = "names.json"
MEMBERS_PREFIX = "members_"
WAR_PREFIX = "war_"
CLANS_FILE = "clans.json"
BASES_FILE = "bases.json"  # store base links: tag -> {type -> [bases]}
TH_COLORS = {
    18: 0x2C2F33,  # TH18 – dark metallic steel / high-tech
    17: 0x003F5D,  # TH17 – deep Prussian blue / justice
    16: 0xD4A017,  # TH16 – golden-yellow nature theme
    15: 0x3C2F7E,  # TH15 – enchanted indigo / magic
    14: 0x1ABB6D,  # TH14 – jungle green
    13: 0x009AA9,  # TH13 – frost turquoise / ice
    12: 0x1E62D0,  # TH12 – electric blue
    11: 0xDCDDDA,  # TH11 – white stone / sacred
    10: 0xA30419,  # TH10 – fire red / lava
    9:  0x1C1C1C,  # TH9 – black fortress
    8:  0x5B5B5B,  # TH8 – dark grey stone
    7:  0x6E6E6E,  # TH7 – castle stone
    6:  0xA78C5A,  # TH6 – golden pillars
    5:  0x9E7A5C,  # TH5 – muted stone
    4:  0xB56C24,  # TH4 – tan/orange stone
    3:  0xC77A2E,  # TH3 – darker orange tiles
    2:  0xD98C36,  # TH2 – orange brick
    1:  0xE39B4A,  # TH1 – light orange wood
}

# ============================
# FILE HELPERS
# ============================
def load_json(path: str):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def save_json(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("[FILE] save error:", e)

def members_filename(tag: str) -> str:
    return f"{MEMBERS_PREFIX}{tag.replace('#','')}.json"

def war_filename(tag: str) -> str:
    return f"{WAR_PREFIX}{tag.replace('#','')}.json"
# ============================
# BASE STORAGE HELPERS
# ============================

def load_bases():
    data = load_json(BASES_FILE)
    return data if isinstance(data, dict) else {}

def save_bases(data):
    if not isinstance(data, dict):
        return
    save_json(BASES_FILE, data)

def get_linked_tag_for_user(user_id: int) -> str | None:
    """
    Reverse-lookup: from discord user id -> player tag
    Uses LINKS_FILE: {tag: discord_id}
    """
    links = load_json(LINKS_FILE) or {}
    for tag, did in links.items():
        if str(did) == str(user_id):
            return tag
    return None

# ============================
# DYNAMIC CLANS (clans.json)
# ============================
def load_clans() -> List[Dict[str, str]]:
    data = load_json(CLANS_FILE)
    if isinstance(data, list) and data:
        # sanitize
        out = []
        for c in data:
            name = str(c.get("name", "Unnamed"))
            tag = str(c.get("tag", "")).upper()
            if not tag.startswith("#"):
                tag = "#" + tag
            out.append({"name": name, "tag": tag})
        return out

    # fallback defaults (your current 2 clans)
    default = [
        {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
        {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
    ]
    save_json(CLANS_FILE, default)
    return default

def save_clans(clans: List[Dict[str, str]]):
    save_json(CLANS_FILE, clans)

# global clan list (will be modified by /addclan, /removeclan)
CLANS: List[Dict[str, str]] = load_clans()

def get_clan_by_tag(tag: str) -> Optional[Dict[str, str]]:
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm
    for c in CLANS:
        if c["tag"].upper() == tag_norm:
            return c
    return None

# ============================
# STRICT JOIN CACHE (persistent)
# ============================
strict_join_cache: Dict[str, set] = {}  # clan_tag -> set(tags)
# Track which clans have had their baseline initialized during this runtime to avoid repeated spam
initialized_baseline: set = set()
# Track consecutive missing counts to debounce false leaves: clan_tag -> {tag -> count}
missing_counts: Dict[str, Dict[str, int]] = {}

def load_strict_cache(clan_tag: str) -> set:
    data = load_json(members_filename(clan_tag))
    return set(data) if isinstance(data, list) else set()

def save_strict_cache(clan_tag: str, tags: set):
    save_json(members_filename(clan_tag), list(tags))

# ============================
# DISCORD CLIENT
# ============================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

class MyBot(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.coc_semaphore = asyncio.Semaphore(COC_CONCURRENCY)

client = MyBot(intents=intents)

# ============================
# COC API HELPERS (aiohttp)
# ============================
# Key selection cache and helpers for multi-key support
_selected_coc_key: Optional[str] = None
_selected_coc_key_ts = 0.0
_coc_key_lock = asyncio.Lock()
_cached_ip: Optional[str] = None
_cached_ip_ts = 0.0

async def get_egress_ip():
    """Detect and cache the current egress IP using the configured service."""
    global _cached_ip, _cached_ip_ts
    now = time.time()
    if _cached_ip and (now - _cached_ip_ts) < COC_IP_CACHE_TTL:
        return _cached_ip
    if client.http_session is None:
        return None
    try:
        async with client.http_session.get(COC_IP_DETECT_URL, timeout=5) as resp:
            if resp.status == 200:
                ip = (await resp.text()).strip()
                _cached_ip = ip
                _cached_ip_ts = now
                return ip
    except Exception:
        return None
    return None

async def select_coc_key():
    """Select a COC API key based on detected IP or previous successful key."""
    global _selected_coc_key, _selected_coc_key_ts
    now = time.time()
    if _selected_coc_key and (now - _selected_coc_key_ts) < COC_IP_CACHE_TTL:
        return _selected_coc_key
    async with _coc_key_lock:
        now = time.time()
        if _selected_coc_key and (now - _selected_coc_key_ts) < COC_IP_CACHE_TTL:
            return _selected_coc_key
        ip = await get_egress_ip()
        if ip and ip in COC_API_KEYS:
            _selected_coc_key = COC_API_KEYS[ip]
            _selected_coc_key_ts = now
            return _selected_coc_key
        if "*" in COC_API_KEYS:
            _selected_coc_key = COC_API_KEYS["*"]
            _selected_coc_key_ts = now
            return _selected_coc_key
        for k in COC_API_KEYS.values():
            _selected_coc_key = k
            _selected_coc_key_ts = now
            return _selected_coc_key
        return None

async def _try_key_and_fetch(url: str, headers: Dict[str, str], timeout: int):
    try:
        async with client.coc_semaphore:
            async with client.http_session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status != 200:
                    return resp.status, None
                data = await resp.json()
                return resp.status, data
    except asyncio.TimeoutError:
        return None, None
    except aiohttp.ClientError:
        return None, None

async def coc_get(path: str):
    """Fetch from COC API with caching and request deduplication.

    Uses `api_cache` (short TTLs) and `request_deduplicator` to avoid duplicate
    concurrent requests and reduce API rate usage. Supports multiple API keys
    and will rotate keys on auth failures.
    """
    if client.http_session is None:
        return None

    # Normalize path and build cache key
    path_norm = path if path.startswith("/") else "/" + path
    cache_key = f"coc:{path_norm}"

    # Determine TTL based on endpoint
    ttl = CLAN_CACHE_TTL
    if "/players/" in path_norm:
        ttl = PLAYER_CACHE_TTL
    elif path_norm.endswith("/currentwar") or "/currentwar" in path_norm:
        ttl = WAR_CACHE_TTL

    # Check cache
    cached = await api_cache.get(cache_key, ttl)
    if cached is not None:
        return cached

    # Use request deduplicator to avoid duplicate concurrent requests
    async def _fetch():
        # try selected key first
        key = await select_coc_key()
        if not key:
            return None
        url = f"{COC_API_BASE_URL}{path_norm}"
        headers = {"Authorization": f"Bearer {key}"}

        status, data = await _try_key_and_fetch(url, headers, COC_TIMEOUT)
        if status == 200 and data is not None:
            await api_cache.set(cache_key, data)
            return data

        # on auth failure or other non-200, try other keys
        global _selected_coc_key, _selected_coc_key_ts
        async with _coc_key_lock:
            for k in set(COC_API_KEYS.values()):
                if k == key:
                    continue
                headers = {"Authorization": f"Bearer {k}"}
                status, data = await _try_key_and_fetch(url, headers, COC_TIMEOUT)
                if status == 200 and data is not None:
                    _selected_coc_key = k
                    _selected_coc_key_ts = time.time()
                    await api_cache.set(cache_key, data)
                    return data
        return None

    result = await request_deduplicator.get_or_create(cache_key, _fetch)
    return result

async def get_clan_member_list(clan_tag: str):
    data = await coc_get(f"/clans/{urllib.parse.quote(clan_tag)}")
    if not data:
        return []
    return data.get("memberList", [])

async def get_player(tag: str):
    return await coc_get(f"/players/{urllib.parse.quote(tag)}")

async def get_current_war(clan_tag: str):
    return await coc_get(f"/clans/{urllib.parse.quote(clan_tag)}/currentwar")

async def fetch_players(tags: List[str], concurrency: int = COC_CONCURRENCY) -> Dict[str, Optional[Dict[str, Any]]]:
    """Fetch many players in parallel and return a mapping tag -> player JSON or None."""
    results: Dict[str, Optional[Dict[str, Any]]] = {}
    if not tags:
        return results
    sem = asyncio.Semaphore(concurrency)

    async def _fetch(tag: str):
        async with sem:
            try:
                player = await get_player(tag)
            except Exception:
                player = None
            results[tag] = player

    tasks = [asyncio.create_task(_fetch(t)) for t in tags]
    await asyncio.gather(*tasks)
    return results

# ============================
# LOG helper
# ============================
async def log(msg: str):
    print(msg)
    if LOG_CHANNEL_ID:
        try:
            ch = client.get_channel(LOG_CHANNEL_ID) or await client.fetch_channel(LOG_CHANNEL_ID)
            await ch.send(f"[LOG {datetime.now().isoformat()}] {msg}")
        except Exception:
            pass

# ============================
# HERO EXTRACTION (used in embeds)
# ============================
def extract_hero_levels(player_json: Dict[str, Any]) -> Dict[str, int]:
    hero_levels = {"BK": 0, "AQ": 0, "GW": 0, "RC": 0}
    if isinstance(player_json.get("heroes"), list):
        for h in player_json.get("heroes", []):
            name = (h.get("name") or "").lower()
            lvl = h.get("level") or 0
            try:
                lvl = int(lvl)
            except Exception:
                lvl = 0
            if "barbarian king" in name:
                hero_levels["BK"] = lvl
            elif "archer queen" in name:
                hero_levels["AQ"] = lvl
            elif "grand warden" in name:
                hero_levels["GW"] = lvl
            elif "royal champion" in name:
                hero_levels["RC"] = lvl
    # fallback keys (if present)
    mapping = {
        "barbarianKingLevel": "BK",
        "archerQueenLevel": "AQ",
        "grandWardenLevel": "GW",
        "royalChampionLevel": "RC",
    }
    for k, code in mapping.items():
        if k in player_json and player_json[k] is not None:
            try:
                hero_levels[code] = int(player_json[k])
            except Exception:
                pass
    return hero_levels

# ============================
# EMBEDS (join/info/leave)
# ============================
def build_join_embed(player_json: Dict[str, Any], tag: str, clan_name: Optional[str] = None) -> discord.Embed:
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
        f"👑 BK {hero_levels.get('BK',0)}   "
        f"👸 AQ {hero_levels.get('AQ',0)}   "
        f"🧙 GW {hero_levels.get('GW',0)}   "
        f"🛡 RC {hero_levels.get('RC',0)}"
    )

    troops = player_json.get("troops", []) or []
    spells = player_json.get("spells", []) or []
    pets = player_json.get("pets", []) or []

    # Treat Minion Prince as hero if present in heroes; fallback to pets (back-compat)
    mp_level = None
    for h in player_json.get('heroes', []) or []:
        if 'minion prince' in (h.get('name') or '').lower():
            try:
                mp_level = int(h.get('level') or 0)
            except Exception:
                mp_level = h.get('level') or '?'
            break
    if mp_level is None:
        for p in list(pets):
            nm = (p.get("name") or "").lower()
            if "minion prince" in nm:
                try:
                    mp_level = int(p.get("level") or 0)
                except Exception:
                    mp_level = p.get("level") or "?"
                pets.remove(p)
                break

    # Append MP to hero summary if present
    if mp_level is not None:
        hero_summary = hero_summary + f"   🤴 MP {mp_level}"

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

def _get_league_icon(player: Dict[str, Any]) -> Optional[str]:
    league = player.get("league") or {}
    icon_urls = league.get("iconUrls") if isinstance(league, dict) else None
    if icon_urls and isinstance(icon_urls, dict):
        # prefer medium, then small, then tiny
        return icon_urls.get("small") or icon_urls.get("tiny") or icon_urls.get("medium")
    return None


# -----------------------
# Hero icon helpers (Minion Prince)
# -----------------------


def _hero_slug(name: str) -> str:
    s = (name or "").lower()
    # keep only alphanumeric and spaces, then replace spaces with hyphens
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = s.strip().replace(" ", "-")
    return s


def _construct_hero_icon_url(hero_name: str) -> str:
    """Construct a best-effort CDN URL for a hero icon (used for Minion Prince)."""
    slug = _hero_slug(hero_name)
    # Best-effort path - cached and non-blocking. If this doesn't exist, it's harmless.
    return f"https://api-assets.clashofclans.com/hero-icons/{slug}.png"


async def _cache_hero_icon_url(hero_name: str) -> None:
    key = f"heroicon:{_hero_slug(hero_name)}"
    await api_cache.set(key, _construct_hero_icon_url(hero_name))


def _ensure_cached_hero_icon_url(hero_name: str) -> str:
    """Return a hero icon URL immediately and schedule caching in the background.

    This keeps the embed-builder synchronous while ensuring the cache is populated.
    """
    url = _construct_hero_icon_url(hero_name)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_cache_hero_icon_url(hero_name))
    except RuntimeError:
        # not running in an event loop, skip caching for now
        pass
    return url


def _exclude_minion_prince(player: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy of player with Minion Prince removed from 'heroes' list.

    This ensures rush calculations keep legacy behavior unchanged.
    """
    p = dict(player)
    if isinstance(p.get('heroes'), list):
        p['heroes'] = [h for h in p['heroes'] if 'minion prince' not in (h.get('name') or '').lower()]
    return p


class PlayerProfileView(discord.ui.View):
    """Quick action buttons shown alongside player profile embeds."""
    def __init__(self, tag: str, clan_tag: Optional[str] = None):
        super().__init__(timeout=None)
        self.tag = tag
        self.clan_tag = clan_tag



def build_leave_embed(tag: str, name: Optional[str] = None) -> discord.Embed:
    title = f"🔴 LEAVE — {name or tag}"
    embed = discord.Embed(title=title, color=0xe74c3c, timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
    return embed
# ============================================
# HERO / LAB / BASE caps & calculations
# ============================================
HERO_CAPS = {
    8:  {"BK": 10,  "AQ": 0,   "GW": 0,   "RC": 0},
    9:  {"BK": 20,  "AQ": 0,   "GW": 0,   "RC": 0},
    10: {"BK": 30,  "AQ": 30,  "GW": 0,   "RC": 0},
    11: {"BK": 40,  "AQ": 40,  "GW": 0,   "RC": 0},
    12: {"BK": 50,  "AQ": 50,  "GW": 20,  "RC": 0},
    13: {"BK": 65,  "AQ": 65,  "GW": 40,  "RC": 0},
    14: {"BK": 75,  "AQ": 75,  "GW": 50,  "RC": 25},
    15: {"BK": 85,  "AQ": 85,  "GW": 60,  "RC": 35},
    16: {"BK": 90,  "AQ": 90,  "GW": 65,  "RC": 40},
    17: {"BK": 95,  "AQ": 95,  "GW": 70,  "RC": 45},
    18: {"BK": 100, "AQ": 100, "GW": 75,  "RC": 50},
}
HERO_RUSH_THRESHOLD = 5.0

LAB_CAPS = {
    1:  {"total": 0}, 2:  {"total": 0}, 3:  {"total": 10}, 4:  {"total": 40},
    5:  {"total": 90}, 6:  {"total":160}, 7:  {"total":260}, 8:  {"total":390},
    9:  {"total":560}, 10: {"total":760}, 11: {"total":950}, 12: {"total":1100},
    13: {"total":1300}, 14: {"total":1500}, 15: {"total":1700}, 16: {"total":1900},
    17: {"total":2100}, 18: {"total":2300},
}
LAB_RUSH_THRESHOLD = 25.0

BASE_CAPS = {
    1:  {"total": 0}, 2:  {"total":15}, 3:  {"total":55}, 4:  {"total":140},
    5:  {"total":260}, 6:  {"total":430}, 7:  {"total":650}, 8:  {"total":920},
    9:  {"total":1250}, 10: {"total":1650}, 11: {"total":2000}, 12: {"total":2350},
    13: {"total":2700}, 14: {"total":3100}, 15: {"total":3500}, 16: {"total":3900},
    17: {"total":4300}, 18: {"total":4700},
}
BASE_RUSH_THRESHOLD = 5.0

def calculate_hero_rush(player_json: Dict[str, Any]):
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    prev_th = int(th) - 1
    caps = HERO_CAPS.get(prev_th)
    if not caps:
        return None
    hero_levels = extract_hero_levels(player_json)
    required_total = sum(caps.values())
    current_total = sum(hero_levels.values())
    missing_total = max(0, required_total - current_total)
    rush_percent = (missing_total / required_total) * 100 if required_total > 0 else 0.0
    counted = rush_percent >= HERO_RUSH_THRESHOLD
    return {"percent": round(rush_percent,2), "counted": counted, "hero_levels": hero_levels}

def extract_lab_total(player_json: Dict[str, Any]) -> int:
    total = 0
    for key in ("troops","spells","pets"):
        if isinstance(player_json.get(key), list):
            for item in player_json.get(key):
                lvl = item.get("level") or 0
                try:
                    total += int(lvl)
                except Exception:
                    pass
    return total

def calculate_lab_rush(player_json: Dict[str, Any]):
    th = player_json.get("townHallLevel")
    if th is None:
        return None
    prev_th = int(th) - 1
    caps = LAB_CAPS.get(prev_th)
    if not caps:
        return None
    required = caps.get("total",0)
    current = extract_lab_total(player_json)
    missing = max(0, required - current)
    percent = (missing / required) * 100 if required > 0 else 0.0
    counted = percent >= LAB_RUSH_THRESHOLD
    return {"percent": round(percent,2), "counted": counted, "required": required, "current": current}

def calculate_base_rush(player_json: Dict[str, Any]):
    th = player_json.get("townHallLevel")
    if th is None:
        return {"status":"N/A"}
    prev_th = int(th) - 1
    caps = BASE_CAPS.get(prev_th)
    if not caps:
        return {"status":"N/A"}
    return {"status":"N/A", "required": caps.get("total",0)}

# ============================================
# TRACKER: join/leave (strict no-duplicate)
# ============================================
async def track_clan(clan: Dict[str,str]):
    await client.wait_until_ready()
    clan_name = clan["name"]
    clan_tag = clan["tag"]
    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)

    # load persistent strict cache for this clan
    strict_join_cache[clan_tag] = load_strict_cache(clan_tag)

    print(f"[TRACK] Started tracker for {clan_name} ({clan_tag})")

    while not client.is_closed():
        await asyncio.sleep(CHECK_INTERVAL)
        member_list = await get_clan_member_list(clan_tag)
        if member_list is None:
            continue

        # If fetch returned an empty list, likely transient API problem; skip to avoid false mass-leaves
        if not member_list and SKIP_EMPTY_MEMBER_LIST:
            await log(f"[TRACK] empty member list for {clan_name} ({clan_tag}), skipping this poll")
            continue

        current_tags = {m["tag"]: m.get("name") for m in member_list if m.get("tag")}
        prev_tags = strict_join_cache.get(clan_tag, set())

        # If we have no persisted baseline (first run), set current members as baseline and skip announcing.
        # Be conservative: only initialize from a non-empty current fetch and only log once per runtime.
        if not prev_tags:
            # if we did fetch members, use that as baseline; otherwise keep empty set
            if current_tags:
                strict_join_cache[clan_tag] = set(current_tags.keys())
            else:
                strict_join_cache[clan_tag] = set()

            try:
                save_strict_cache(clan_tag, strict_join_cache[clan_tag])
            except Exception:
                pass

            # Avoid spamming the logs if the cache remains empty or saving repeatedly fails.
            if clan_tag not in initialized_baseline:
                await log(f"[TRACK] Baseline initialized for {clan_name} ({clan_tag}) — skipping initial join announcements")
                initialized_baseline.add(clan_tag)
            continue

        # joins: only tags not in persistent cache
        joins = [tag for tag in current_tags if tag not in prev_tags]
        for tag in joins:
            player = await get_player(tag)
            if player:
                emb = build_join_embed(player, tag, clan_name)
                try:
                    await channel.send(embed=emb)
                    await asyncio.sleep(0.15)
                except Exception as e:
                    await log(f"[TRACK] join send failed: {e}")
            else:
                name = current_tags.get(tag, tag)
                emb = discord.Embed(
                    title=f"🟢 PLAYER JOINED — {name}",
                    description=f"`{tag}` joined **{clan_name}**",
                    color=0x2ecc71,
                    timestamp=datetime.now(timezone.utc)
                )
                emb.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
                try:
                    await channel.send(embed=emb)
                    await asyncio.sleep(0.15)
                except Exception as e:
                    await log(f"[TRACK] fallback join send failed: {e}")

            strict_join_cache[clan_tag].add(tag)
            # reset any pending missing counter for this tag
            missing_counts.setdefault(clan_tag, {}).pop(tag, None)

        if joins:
            save_strict_cache(clan_tag, strict_join_cache[clan_tag])

        # leaves: debounce to avoid false mass-leaves due to transient API failures
        # increment missing counters for tags not present in current fetch
        missing = missing_counts.setdefault(clan_tag, {})
        current_tag_set = set(current_tags.keys())
        # reset counters for present tags
        for t in list(current_tag_set):
            if t in missing:
                missing.pop(t, None)

        candidate_leaves = [tag for tag in list(prev_tags) if tag not in current_tag_set]
        for tag in candidate_leaves:
            cnt = missing.get(tag, 0) + 1
            missing[tag] = cnt
            if cnt < LEAVE_DEBOUNCE_COUNT:
                # wait for more consecutive misses
                continue
            # confirmed leave
            name = current_tags.get(tag) or tag
            emb = build_leave_embed(tag, name)
            try:
                await channel.send(embed=emb)
                await asyncio.sleep(0.15)
            except Exception as e:
                await log(f"[TRACK] leave send failed: {e}")

            if tag in strict_join_cache[clan_tag]:
                strict_join_cache[clan_tag].remove(tag)
                save_strict_cache(clan_tag, strict_join_cache[clan_tag])
            missing.pop(tag, None)

# ============================================
# WAR TRACKER
# ============================================
war_baselines: Dict[str, Dict[str, Any]] = {}

async def war_tracker(clan: Dict[str,str]):
    await client.wait_until_ready()
    clan_name = clan["name"]
    clan_tag = clan["tag"]
    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
    war_baselines[clan_tag] = load_json(war_filename(clan_tag)) or {}
    print(f"[WAR] Started war tracker for {clan_name} ({clan_tag})")

    while not client.is_closed():
        war = await get_current_war(clan_tag)
        if not war or war.get("state") != "inWar":
            await asyncio.sleep(WAR_POLL_INTERVAL)
            continue

        clan_data = war.get("clan") or {}
        members = clan_data.get("members") or []
        current_map: Dict[str, list] = {}
        for member in members:
            if not isinstance(member, dict):
                continue
            tag = member.get("tag")
            if not tag:
                continue
            attacks = member.get("attacks", []) or []
            current_map[tag] = attacks

        prev_map = war_baselines.get(clan_tag, {})

        for tag, attacks in current_map.items():
            prev_attacks = prev_map.get(tag, [])
            if len(attacks) > len(prev_attacks):
                name = next((m.get("name") for m in members if m.get("tag") == tag), tag)
                new_attacks = attacks[len(prev_attacks):]
                for atk in new_attacks:
                    stars = atk.get("stars", "?")
                    desc = atk.get("destructionPercentage", atk.get("destructionPercent", "?"))
                    try:
                        await channel.send(f"⚔️ **WAR HIT:** {name} ({tag}) — {stars}★ • {desc}%")
                        await asyncio.sleep(0.12)
                    except Exception:
                        pass

        war_baselines[clan_tag] = current_map
        save_json(war_filename(clan_tag), war_baselines[clan_tag])
        await asyncio.sleep(WAR_POLL_INTERVAL)

# ============================================
# REMINDERS (war attacks)
# ============================================
from datetime import datetime
async def fixed_time_reminder_loop():
    await client.wait_until_ready()
    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
    print("[REMINDER] Fixed-time (every even hour) reminder loop started")

    sent_today_hours = set()

    while not client.is_closed():
        now = datetime.now()
        hour = now.hour
        minute = now.minute

        if hour == 0 and minute == 0:
            sent_today_hours.clear()

        if minute == 0 and hour % 2 == 0 and hour not in sent_today_hours:
            sent_today_hours.add(hour)

            out_lines = []
            pending_total = 0

            for clan in CLANS:
                war = await get_current_war(clan["tag"])
                if not war or war.get("state") != "inWar":
                    continue

                members = (war.get("clan") or {}).get("members") or []
                pending = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]

                if pending:
                    pending_total += len(pending)
                    out_lines.append(f"**{clan['name']}** — {len(pending)} pending")
                    out_lines += [f"• {p.get('name')} `{p.get('tag')}`" for p in pending[:40]]

                    # DM sending
                    links = load_json(LINKS_FILE) or {}
                    dm_sent = 0
                    dm_failed = 0

                    for p in pending:
                        tag_norm = (p.get("tag") or "").upper()
                        discord_id = links.get(tag_norm)
                        if discord_id:
                            try:
                                user = await client.fetch_user(int(discord_id))
                                await user.send(
                                    f"⚠️ **WAR REMINDER**\nYou have **0 attacks used** in war for **{clan['name']}**.\nPlease attack ASAP! 💥"
                                )
                                dm_sent += 1
                                await asyncio.sleep(0.25)
                            except Exception as e:
                                dm_failed += 1
                                await log(f"[DM FAIL] {tag_norm} ({discord_id}) → {e}")

            if out_lines:
                try:
                    msg = "⏰ **WAR REMINDER — Every 2 Hours (Even Hours)**\n" + "\n".join(out_lines)
                    await channel.send(msg + f"\n\n📨 **DM sent:** {dm_sent} | ❌ **Failed:** {dm_failed}")
                except Exception as e:
                    await log(f"[REMINDER FIXED] send failed: {e}")

        await asyncio.sleep(30)

async def monthly_donation_snapshot_loop():
    """Take monthly donation snapshots (runs once/day check)."""
    await client.wait_until_ready()
    print("[SNAPSHOT] Monthly donation snapshot loop started")

    last_snapshot_month = {}

    while not client.is_closed():
        try:
            now = datetime.now(timezone.utc)
            current_day = now.day
            current_month_key = get_current_month_key()

            # Check if it's the configured day of month
            if current_day == MONTHLY_SNAPSHOT_DAY:
                for clan in CLANS:
                    clan_tag = clan["tag"]
                    clan_name = clan["name"]

                    # Skip if we already took a snapshot this month
                    if last_snapshot_month.get(clan_tag) == current_month_key:
                        continue

                    try:
                        print(f"[SNAPSHOT] Taking snapshot for {clan_name} ({clan_tag})")

                        # Fetch all members
                        members = await get_clan_member_list(clan_tag)
                        if not members:
                            continue

                        # Fetch player data for all members
                        player_cache = {}
                        for member in members:
                            tag = member.get("tag")
                            if tag:
                                player = await get_player(tag)
                                if player:
                                    player_cache[tag] = player
                                await asyncio.sleep(0.1)

                        if player_cache:
                            # Create and save snapshot
                            snapshot = create_donation_snapshot(clan_tag, members, player_cache)
                            success = save_monthly_snapshot(clan_tag, snapshot)

                            if success:
                                last_snapshot_month[clan_tag] = current_month_key
                                member_count = len(snapshot.get("members", {}))
                                print(f"[SNAPSHOT] Successfully saved snapshot for {clan_name}: {member_count} members")

                                # Send notification
                                try:
                                    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
                                    if channel:
                                        embed = discord.Embed(
                                            title=f"📸 Monthly Donation Snapshot — {clan_name}",
                                            color=0x3498db,
                                            timestamp=datetime.now(timezone.utc)
                                        )
                                        embed.add_field(name="Month", value=current_month_key, inline=True)
                                        embed.add_field(name="Members", value=str(member_count), inline=True)
                                        embed.description = "Donation snapshot taken successfully. Use `/donationhistory` to view."
                                        await channel.send(embed=embed)
                                except Exception as e:
                                    await log(f"[SNAPSHOT] Failed to send notification: {e}")
                            else:
                                print(f"[SNAPSHOT] Failed to save snapshot for {clan_name}")

                    except Exception as e:
                        print(f"[SNAPSHOT] Error taking snapshot for {clan_name}: {e}")

            # Wait 1 hour before checking again
            await asyncio.sleep(3600)

        except Exception as e:
            print(f"[SNAPSHOT] Error in snapshot loop: {e}")
            await asyncio.sleep(3600)

# ============================================
# HERO UPGRADE ALERTS (3+ heroes)
# ============================================
async def check_hero_upgrades(clan: Dict[str,str]):
    clan_tag = clan["tag"]
    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
    members = await get_clan_member_list(clan_tag)
    if not members:
        return
    for m in members:
        tag = m.get("tag")
        if not tag:
            continue
        player = await get_player(tag)
        if not player:
            continue
        upgrading = []
        if isinstance(player.get("heroes"), list):
            for h in player["heroes"]:
                ut = h.get("upgradeTimeLeft")
                if ut is not None and ut not in (0, "0", ""):
                    upgrading.append(h.get("name") or "Unknown Hero")
        if len(upgrading) >= 3:
            embed = discord.Embed(
                title="⚠️ Hero Upgrade Alert",
                description=f"**{player.get('name')}** (`{tag}`) is upgrading **{len(upgrading)} heroes**!",
                color=0xe67e22,
                timestamp=datetime.now(timezone.utc)
            )
            embed.add_field(name="Heroes", value="\n".join(upgrading) if upgrading else "—")
            try:
                await channel.send(embed=embed)
            except Exception:
                pass

async def hero_upgrade_loop(clan: Dict[str,str]):
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            await check_hero_upgrades(clan)
        except Exception as e:
            await log(f"[UPGRADE] hero check failed: {e}")
        await asyncio.sleep(UPGRADE_CHECK_INTERVAL)

# ============================================
# GENERAL UPGRADE ALERT LOOP (troops/pets/spells)
# ============================================
last_upgrade_cache: Dict[str, List[str]] = {}

async def upgrade_alert_loop(clan: Dict[str,str]):
    await client.wait_until_ready()
    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
    while not client.is_closed():
        members = await get_clan_member_list(clan["tag"])
        if not members:
            await asyncio.sleep(300)
            continue
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            player = await get_player(tag)
            if not player:
                continue
            upgrading: List[str] = []
            for u in player.get("heroes", []):
                ut = u.get("upgradeTimeLeft")
                if ut is not None and ut not in (0, "0", ""):
                    upgrading.append(f"Hero: {u.get('name')} → L{(u.get('level') or 0) + 1}")
            for u in player.get("pets", []):
                ut = u.get("upgradeTimeLeft")
                if ut is not None and ut not in (0, "0", ""):
                    upgrading.append(f"Pet: {u.get('name')} → L{(u.get('level') or 0) + 1}")
            for u in (player.get("troops") or []) + (player.get("spells") or []):
                ut = u.get("upgradeTimeLeft")
                if ut is not None and ut not in (0, "0", ""):
                    upgrading.append(f"Troop/Spell: {u.get('name')} → L{(u.get('level') or 0) + 1}")
            old = last_upgrade_cache.get(tag, [])
            new_upgrades = [x for x in upgrading if x not in old]
            if new_upgrades:
                embed = discord.Embed(
                    title=f"⬆️ Upgrade Started — {player.get('name')}",
                    color=0x00aaff,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(name="New Upgrades", value="\n".join(new_upgrades) if new_upgrades else "—")
                embed.set_footer(text=tag)
                try:
                    await channel.send(embed=embed)
                except Exception:
                    pass
            last_upgrade_cache[tag] = upgrading

        await asyncio.sleep(UPGRADE_ALERT_CHECK)

# ============================================
# CLAN AUTOCOMPLETE (for slash commands)
# ============================================
async def clan_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    # dynamic choices based on current CLANS list — include ALL CLANS option
    current_lower = (current or "").lower()
    options: List[app_commands.Choice[str]] = []

    # Always offer ALL CLANS as first choice when the user types 'all' or empty
    if not current_lower or "all".startswith(current_lower):
        options.append(app_commands.Choice(name="ALL CLANS", value="ALL"))

    for c in CLANS:
        label = f"{c['name']} ({c['tag']})"
        if current_lower in label.lower():
            options.append(app_commands.Choice(name=label, value=c["tag"]))
    return options[:25]
# ============================================
# SLASH COMMANDS
# ============================================

@client.tree.command(name="link", description="Link your Discord account to a Clash player tag (for war reminders).")
@app_commands.describe(tag="Your player tag (example: #2PQUE2J)")
async def link(interaction: discord.Interaction, tag: str):
    await interaction.response.send_message("🔗 Linking your tag...", ephemeral=True)
    tag = tag.strip().upper()
    if not tag.startswith("#"):
        tag = "#" + tag
    links = load_json(LINKS_FILE) or {}
    links[tag] = str(interaction.user.id)
    save_json(LINKS_FILE, links)
    emb = discord.Embed(title="Account Linked ✅", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
    emb.add_field(name="Discord User", value=f"{interaction.user.mention}", inline=True)
    emb.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
    emb.set_footer(text="Use /info tag:#PLAYER to get player info anytime.")
    await interaction.edit_original_response(content="🔗 Linked! Check your DMs (ephemeral summary follows).")
    await interaction.followup.send(embed=emb, ephemeral=True)

@client.tree.command(name="info", description="Get detailed player info + rush + war stars")
@app_commands.describe(tag="Player tag (example: #2PQUE2J) — optional; if omitted uses your linked tag")
async def info(interaction: discord.Interaction, tag: Optional[str] = None):
    await interaction.response.send_message("🔎 Fetching player info...", ephemeral=True)
    # If no tag provided, use linked tag for this Discord user
    if not tag:
        linked = get_linked_tag_for_user(interaction.user.id)
        if not linked:
            await interaction.edit_original_response(content="❌ No tag provided and no linked account found. Provide a tag or link your account with `/link`.")
            return
        tag_norm = linked
    else:
        tag_norm = tag.strip().upper()
        if not tag_norm.startswith("#"):
            tag_norm = "#" + tag_norm
    player = await get_player(tag_norm)
    if not player:
        await interaction.edit_original_response(content=(f"❌ Could not fetch player `{tag_norm}`. Check tag or API."))
        return

    # Detect Minion Prince: prefer heroes list, fallback to pets (preserve previous behavior)
    mp_level = None
    # heroes first
    for h in player.get('heroes', []) or []:
        if 'minion prince' in (h.get('name') or '').lower():
            try:
                mp_level = int(h.get('level') or 0)
            except Exception:
                mp_level = h.get('level') or '?'
            break
    # fallback to pets (older accounts may report MP there)
    if mp_level is None:
        pets_list = player.get('pets', []) or []
        for p in list(pets_list):
            if 'minion prince' in (p.get('name') or '').lower():
                try:
                    mp_level = int(p.get('level') or 0)
                except Exception:
                    mp_level = p.get('level') or '?'
                pets_list.remove(p)
                # update player dict so pet listing excludes MP
                player['pets'] = pets_list
                break

    embed = build_info_embed(player, tag_norm)

    # Exclude Minion Prince from rush calculations to keep legacy metrics unchanged
    player_for_rush = dict(player)
    if isinstance(player_for_rush.get('heroes'), list):
        player_for_rush['heroes'] = [h for h in player_for_rush['heroes'] if 'minion prince' not in (h.get('name') or '').lower()]
    hero_res = calculate_hero_rush(player_for_rush)
    lab_res = calculate_lab_rush(player)

    # Compact Rush Status (no hero levels here; heroes are shown once in the embed)
    rush_info = []
    if hero_res:
        status = "Rushed" if hero_res['counted'] else "OK"
        rush_info.append(f"Hero Rush: {hero_res['percent']:.2f}% ({status})")
    if lab_res:
        status = "Rushed" if lab_res['counted'] else "OK"
        rush_info.append(f"Lab Rush: {lab_res['percent']:.2f}% ({status})")
    if rush_info:
        embed.add_field(name="\u200b", value="\u200b", inline=False)
        embed.add_field(name=f"⚡ {_bold_upper('RUSH STATUS')}", value="\n".join(rush_info), inline=False)

    # Pets details (show up to 10 pets with levels)
    pets = player.get('pets', []) or []
    if pets:
        pet_lines = [f"{p.get('name')} L{p.get('level', '?')}" for p in pets[:10]]
        embed.add_field(name="🐾 Pets", value="\n".join(pet_lines), inline=False)

    await interaction.edit_original_response(content="✅ Done — player info below.")
    await interaction.followup.send(embed=embed, view=PlayerProfileView(tag_norm, player.get('clan', {}).get('tag')))

@client.tree.command(name="roster", description="Export clan roster CSV (members) for a clan")
@app_commands.describe(clan="Clan to export (select ALL CLANS to export all members)")
@app_commands.autocomplete(clan=clan_autocomplete)
async def roster(interaction: discord.Interaction, clan: str):
    await interaction.response.send_message("📤 Building roster...", ephemeral=False)

    # Support ALL CLANS by combining into a single CSV with a clan column
    if clan == "ALL":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["clan","name","tag","townHall","expLevel","trophies","role"])
        total_rows = 0
        for c in CLANS:
            members = await get_clan_member_list(c["tag"])
            if not members:
                continue
            for m in members:
                writer.writerow([
                    c.get("name"),
                    m.get("name"),
                    m.get("tag"),
                    m.get("townHallLevel"),
                    m.get("expLevel"),
                    m.get("trophies"),
                    m.get("role")
                ])
                total_rows += 1
        if total_rows == 0:
            await interaction.edit_original_response(content="❌ Could not fetch members for any clans.")
            return
        output.seek(0)
        bio = io.BytesIO(output.getvalue().encode())
        bio.name = f"roster_ALL_clans.csv"
        await interaction.edit_original_response(content="✅ Combined roster ready — check attachment.")
        await interaction.followup.send(file=discord.File(bio, filename=bio.name), ephemeral=False)
        return

    clan_obj = get_clan_by_tag(clan)
    if not clan_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return
    members = await get_clan_member_list(clan_obj["tag"])
    if not members:
        await interaction.edit_original_response(content="❌ Could not fetch clan or clan is empty.")
        return
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["name","tag","townHall","expLevel","trophies","role"])
    for m in members:
        writer.writerow([
            m.get("name"),
            m.get("tag"),
            m.get("townHallLevel"),
            m.get("expLevel"),
            m.get("trophies"),
            m.get("role")
        ])
    output.seek(0)
    bio = io.BytesIO(output.getvalue().encode())
    bio.name = f"roster_{clan_obj['tag'].replace('#','')}.csv"
    await interaction.edit_original_response(content="✅ Roster ready — check attachment.")
    await interaction.followup.send(file=discord.File(bio, filename=bio.name), ephemeral=False)

@client.tree.command(name="status", description="Show bot status and basic stats")
async def status(interaction: discord.Interaction):
    await interaction.response.send_message("⏳ Gathering status...", ephemeral=True)
    u = client.user
    guilds = len(client.guilds)
    now = datetime.now(timezone.utc).isoformat()
    text = (
        f"**Bot:** {u}\n"
        f"**Guilds:** {guilds}\n"
        f"**Time:** {now}\n"
        f"**COC concurrency:** {COC_CONCURRENCY}\n"
        f"**Monitored clans:** {', '.join([c['name'] for c in CLANS])}"
    )
    await interaction.edit_original_response(content=text)

@client.tree.command(name="clearbot", description="Delete recent bot messages in this channel (fast & safe).")
@app_commands.describe(limit="How many recent messages to check (max 500)")
async def clearbot(interaction: discord.Interaction, limit: int = 200):
    await interaction.response.send_message("🧹 Cleaning bot messages…", ephemeral=True)

    if limit > 500:
        limit = 500
    channel = interaction.channel
    deleted = 0

    try:
        # Try bulk purge first for speed
        def _is_bot(m: discord.Message):
            return m.author.id == client.user.id
        try:
            deleted_messages = await channel.purge(limit=limit, check=_is_bot)
            deleted = len(deleted_messages)
        except Exception:
            # Fallback to per-message deletion
            async for msg in channel.history(limit=limit):
                if msg.author.id == client.user.id:
                    try:
                        await msg.delete()
                        deleted += 1
                    except Exception:
                        await asyncio.sleep(0.05)

        await interaction.edit_original_response(
            content=f"🧹 Deleted **{deleted}** bot messages in <#{channel.id}>."
        )
    except Exception as e:
        try:
            await interaction.edit_original_response(
                content=f"❌ Error while deleting messages:\n```\n{e}\n```"
            )
        except Exception:
            pass

@client.tree.command(name="whohavenotattacked", description="Show players who haven't attacked in current war.")
@app_commands.describe(clan="(Optional) Select a clan; if empty, checks all.")
@app_commands.autocomplete(clan=clan_autocomplete)
async def whohavenotattacked(interaction: discord.Interaction, clan: Optional[str] = None):
    await interaction.response.send_message("🔎 Checking war status...", ephemeral=False)
    out_lines: List[str] = []

    clans_to_check = CLANS
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return

    for c in clans_to_check:
        war = await get_current_war(c["tag"])
        if not war or war.get("state") != "inWar":
            continue
        members = (war.get("clan") or {}).get("members") or []
        pending = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]
        if pending:
            out_lines.append(f"**{c['name']}** — {len(pending)} pending")
            out_lines += [f"• {p.get('name')} `{p.get('tag')}`" for p in pending[:50]]

    if not out_lines:
        await interaction.edit_original_response(content="No ongoing war or everyone attacked.")
    else:
        text = "\n".join(out_lines)
        await interaction.edit_original_response(content="📋 Results ready.")
        await interaction.followup.send(text, ephemeral=False)

@client.tree.command(name="kicksuggestions", description="Show players who might be candidates for kicking (rushed / missed war hits).")
@app_commands.describe(clan="(Optional) clan to check; default = all")
@app_commands.autocomplete(clan=clan_autocomplete)
async def kicksuggestions(interaction: discord.Interaction, clan: Optional[str] = None):
    await interaction.response.send_message("🔎 Building kick suggestions...", ephemeral=False)
    output: List[str] = []

    clans_to_check = CLANS
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return

    for c in clans_to_check:
        clan_name = c["name"]
        clan_tag = c["tag"]
        ml = await get_clan_member_list(clan_tag)
        if not ml:
            continue

        # Bulk-fetch player profiles for speed
        tags = [m.get("tag") for m in ml if m.get("tag")]
        player_cache = await fetch_players(tags)

        # Fetch war once per clan
        war = await get_current_war(clan_tag)
        cw_members = (war.get("clan") or {}).get("members") if war and war.get("state") == "inWar" else []

        bad: List[str] = []
        for m in ml:
            tag = m.get("tag")
            if not tag:
                continue
            player = player_cache.get(tag)
            if not player:
                continue
            # Exclude Minion Prince to preserve legacy rush calculation behavior
            hero = calculate_hero_rush(_exclude_minion_prince(player))
            rushed = hero and hero["counted"]
            missed_attack = False
            if cw_members:
                found = next((x for x in cw_members if x.get("tag") == tag), None)
                if found and len(found.get("attacks", [])) == 0:
                    missed_attack = True
            if rushed or missed_attack:
                reasons = []
                if rushed:
                    reasons.append(f"Rushed {hero['percent']}%")
                if missed_attack:
                    reasons.append("No war hit")
                bad.append(f"• {player.get('name')} `{tag}` — {', '.join(reasons)}")
        if bad:
            output.append(f"**{clan_name}:**\n" + "\n".join(bad))

    if not output:
        await interaction.edit_original_response(content="No kick suggestions. Clan looks good!")
    else:
        await interaction.edit_original_response(content="📋 Kick suggestions ready.")
        await interaction.followup.send("\n\n".join(output), ephemeral=False)

@client.tree.command(name="raidsleft", description="Show players who did NOT finish capital raid attacks")
@app_commands.describe(clan="(Optional) clan to check; default = all")
@app_commands.autocomplete(clan=clan_autocomplete)
async def raidsleft(interaction: discord.Interaction, clan: Optional[str] = None):
    await interaction.response.send_message("🔎 Checking capital raid status...", ephemeral=False)
    out: List[str] = []

    clans_to_check = CLANS
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return

    for c in clans_to_check:
        raid = await coc_get(f"/clans/{urllib.parse.quote(c['tag'])}/capitalraidseason")
        if not raid:
            continue
        members = raid.get("members", [])
        not_used = [m for m in members if (m.get("attacksUsed", 0) < (m.get("attacksLimit") or 6))]
        if not_used:
            out.append(f"**{c['name']} — Missing Attacks:**")
            for p in not_used:
                out.append(
                    f"• {p.get('name')} `{p.get('tag')}` — "
                    f"{p.get('attacksUsed',0)}/{p.get('attacksLimit',6)}"
                )

    if not out:
        await interaction.edit_original_response(content="Everyone completed raid attacks!")
    else:
        await interaction.edit_original_response(content="🔔 Raid report ready.")
        await interaction.followup.send("\n".join(out), ephemeral=False)

# DONATION COMMANDS
@client.tree.command(name="donations", description="View donation statistics for a player")
@app_commands.describe(tag="Player tag (example: #2PQUE2J) — optional; if omitted uses your linked tag")
async def donations(interaction: discord.Interaction, tag: Optional[str] = None):
    await interaction.response.send_message("💝 Fetching donation stats...", ephemeral=False)
    # If no tag provided, use linked tag for this Discord user
    if not tag:
        linked = get_linked_tag_for_user(interaction.user.id)
        if not linked:
            await interaction.edit_original_response(content="❌ No tag provided and no linked account found. Provide a tag or link your account with `/link`.")
            return
        tag_norm = linked
    else:
        tag_norm = tag.strip().upper()
        if not tag_norm.startswith("#"):
            tag_norm = "#" + tag_norm
    player = await get_player(tag_norm)
    if not player:
        await interaction.edit_original_response(content=(f"❌ Could not fetch player `{tag_norm}`. Check tag or API."))
        return

    # Get lifetime donations from achievements
    lifetime = extract_lifetime_donations(player)
    seasonal = player.get("donations", 0)
    received = player.get("donationsReceived", 0)

    embed = discord.Embed(
        title=f"💝 Donation Stats — {player.get('name', 'Unknown')}",
        color=0x2ecc71,
        timestamp=datetime.now(timezone.utc)
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
        inline=False
    )
    embed.add_field(
        name="📅 Current Season",
        value=f"Sent: **{seasonal:,}**\nReceived: **{received:,}**",
        inline=True
    )

    # Try to get tracked stats if player is in a monitored clan
    for clan in CLANS:
        stats = get_player_donation_stats(tag_norm, clan["tag"])
        if stats:
            embed.add_field(
                name="📈 Tracked Stats",
                value=(
                    f"Tracking since: **{stats.get('tracked_from', 'N/A')}**\n"
                    f"Last snapshot: **{stats.get('snapshot_date', 'N/A')}**"
                ),
                inline=True
            )
            break

    embed.set_footer(text="Lifetime stats from achievements • Seasonal from current season")
    await interaction.edit_original_response(content="✅ Donation stats:", embed=embed)


@client.tree.command(name="donationhistory", description="View monthly donation history for a clan")
@app_commands.describe(
    clan="Clan to check (select ALL CLANS for aggregated view)",
    months="Number of months to show (default: 6, max: 24)"
)
@app_commands.autocomplete(clan=clan_autocomplete)
async def donationhistory(interaction: discord.Interaction, clan: str, months: int = 6):
    await interaction.response.send_message("📊 Building donation history...", ephemeral=False)

    if months < 1:
        months = 1
    if months > 24:
        months = 24

    # Special: ALL CLANS
    if clan == "ALL":
        embed = discord.Embed(
            title=f"📊 Donation History — All Clans",
            color=0x3498db,
            timestamp=datetime.now(timezone.utc)
        )
        for c in CLANS:
            history = get_donation_history(c["tag"], limit=months)
            if not history:
                val = "⚠️ No donation history found."
            else:
                lines = [f"{m.get('month','?')}: {m.get('total_monthly',0):,}" for m in history[:months]]
                val = "\n".join(lines)
            embed.add_field(name=c["name"], value=val or "No data", inline=False)
        embed.set_footer(text=f"Monthly snapshots taken on the {MONTHLY_SNAPSHOT_DAY}th of each month — showing up to {months} months")
        await interaction.edit_original_response(content="✅ Donation history (All Clans):", embed=embed)
        return

    clan_obj = get_clan_by_tag(clan)
    if not clan_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return

    history = get_donation_history(clan_obj["tag"], limit=months)

    if not history:
        await interaction.edit_original_response(
            content=f"⚠️ No donation history found for **{clan_obj['name']}**. "
                   f"Snapshots will be created automatically on the 1st of each month."
        )
        return

    embed = discord.Embed(
        title=f"📊 Donation History — {clan_obj['name']}",
        color=0x3498db,
        timestamp=datetime.now(timezone.utc)
    )

    # Build summary
    lines = []
    total_all_months = 0

    for month_data in history[:months]:
        month = month_data.get("month", "Unknown")
        total_monthly = month_data.get("total_monthly", 0)
        total_all_months += total_monthly
        member_count = len(month_data.get("members", {}))
        lines.append(f"**{month}**: {total_monthly:,} donations ({member_count} members)")

    if lines:
        embed.description = "\n".join(lines)
        embed.add_field(
            name="📈 Summary",
            value=f"Total tracked: **{total_all_months:,}** donations\nMonths shown: **{len(history)}**",
            inline=False
        )
    else:
        embed.description = "No donation data available yet."

    embed.set_footer(text="Monthly snapshots taken on the 1st of each month")
    await interaction.edit_original_response(content="✅ Donation history:", embed=embed)


@client.tree.command(name="takesnapshot", description="Manually take a donation snapshot for a clan (Elder)")
@app_commands.describe(clan="Clan to snapshot")
@app_commands.autocomplete(clan=clan_autocomplete)
async def takesnapshot(interaction: discord.Interaction, clan: str):
    await interaction.response.send_message("📸 Taking snapshot...", ephemeral=False)

    # Support ALL CLANS
    if clan == "ALL":
        results = []
        for c in CLANS:
            clan_obj = c
            members = await get_clan_member_list(clan_obj["tag"])
            if not members:
                results.append((clan_obj["name"], False, "Could not fetch members or clan empty"))
                continue
            tags = [m.get("tag") for m in members if m.get("tag")]
            player_cache = await fetch_players(tags)
            if not player_cache:
                results.append((clan_obj["name"], False, "Could not fetch player data"))
                continue
            snapshot = create_donation_snapshot(clan_obj["tag"], members, player_cache)
            success = save_monthly_snapshot(clan_obj["tag"], snapshot)
            if success:
                results.append((clan_obj["name"], True, f"Saved {len(snapshot.get('members',{}))} members"))
            else:
                results.append((clan_obj["name"], False, "Failed to save snapshot"))
        # Build result embed
        emb = discord.Embed(title="✅ Snapshot Results — All Clans", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
        for rn in results:
            emb.add_field(name=rn[0], value=("✅ " + rn[2]) if rn[1] else ("❌ " + rn[2]), inline=False)
        await interaction.edit_original_response(content="✅ Snapshot results:", embed=emb)
        return

    clan_obj = get_clan_by_tag(clan)
    if not clan_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return

    # Fetch all members
    members = await get_clan_member_list(clan_obj["tag"])
    if not members:
        await interaction.edit_original_response(content="❌ Could not fetch clan or clan is empty.")
        return

    # Fetch player data for all members (parallel)
    tags = [m.get("tag") for m in members if m.get("tag")]
    player_cache = await fetch_players(tags)
    fetched = sum(1 for v in player_cache.values() if v)


    if not player_cache:
        await interaction.edit_original_response(content="❌ Could not fetch any player data.")
        return

    # Create snapshot
    snapshot = create_donation_snapshot(clan_obj["tag"], members, player_cache)
    success = save_monthly_snapshot(clan_obj["tag"], snapshot)

    if success:
        month = snapshot["date"]
        member_count = len(snapshot.get("members", {}))
        embed = discord.Embed(
            title="✅ Snapshot Created",
            color=0x2ecc71,
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Clan", value=clan_obj["name"], inline=True)
        embed.add_field(name="Month", value=month, inline=True)
        embed.add_field(name="Members", value=str(member_count), inline=True)
        embed.add_field(name="Players Fetched", value=str(fetched), inline=True)
        await interaction.edit_original_response(content="✅ Snapshot created:", embed=embed)
    else:
        await interaction.edit_original_response(content="❌ Failed to save snapshot.")


# ============================================
# /upgradecheck — min heroes + clan selector
# ============================================
@client.tree.command(
    name="upgradecheck",
    description="Show players upgrading at least N heroes in a clan (or all clans)."
)
@app_commands.describe(
    min_heroes="Minimum heroes upgrading (0 = diagnostic / everyone)",
    clan="(Optional) clan to check; default = all"
)
@app_commands.autocomplete(clan=clan_autocomplete)
async def upgradecheck(
    interaction: discord.Interaction,
    min_heroes: int = 1,
    clan: Optional[str] = None
):
    await interaction.response.send_message("🔎 Scanning hero upgrades...", ephemeral=False)

    if min_heroes < 0:
        min_heroes = 0

    clans_to_check = CLANS
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return

    lines: List[str] = []
    diag_lines: List[str] = []
    total_checked = 0
    total_hits = 0

    for c in clans_to_check:
        members = await get_clan_member_list(c["tag"])
        if not members:
            continue

        # Bulk fetch players
        tags = [m.get("tag") for m in members if m.get("tag")]
        player_cache = await fetch_players(tags)


    lines: List[str] = []
    diag_lines: List[str] = []
    total_checked = 0
    total_hits = 0

    for c in clans_to_check:
        members = await get_clan_member_list(c["tag"])
        if not members:
            continue
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            player = await get_player(tag)
            if not player:
                continue
            total_checked += 1

            # count upgrading heroes (robust check for upgradeTimeLeft)
            upgrading_count = 0
            upgrading_names: List[str] = []
            for h in player.get("heroes", []) or []:
                ut = h.get("upgradeTimeLeft")
                if ut is not None and ut not in (0, "0", ""):
                    upgrading_count += 1
                    # next level is level+1 (best guess)
                    next_level = (h.get("level") or 0) + 1
                    upgrading_names.append(f"{h.get('name')} → L{next_level}")

            # for diagnostic output even if < min_heroes
            diag_lines.append(
                f"{c['name']} • {player.get('name')} ({tag}) — "
                f"detected {upgrading_count} upgrading hero(oes)"
            )

            if upgrading_count >= min_heroes:
                total_hits += 1
                if upgrading_names:
                    details = "\n    " + "\n    ".join(upgrading_names)
                else:
                    details = ""
                lines.append(
                    f"**{c['name']}** — {player.get('name')} `{tag}` — "
                    f"{upgrading_count} hero(oes) upgrading{details}"
                )

    if min_heroes == 0:
        # pure diagnostic mode
        if not diag_lines:
            await interaction.edit_original_response(
                content="✅ Diagnostic complete: no members checked (empty clans / API failure)."
            )
            return
        msg = (
            f"✅ Diagnostic — hero upgrade detection (0+ heroes)\n"
            f"Checked {total_checked} members.\n\n"
            + "\n".join(diag_lines[:50])
        )
        await interaction.edit_original_response(content=msg)
        return

    if not lines:
        await interaction.edit_original_response(
            content=(
                f"✅ No players upgrading **{min_heroes}** or more heroes "
                f"(checked {total_checked} members across {len(clans_to_check)} clan(s))."
            )
        )
    else:
        header = (
            f"⬆️ Upgrade Check — **{min_heroes}+ Heroes**\n"
            f"Matched **{total_hits}** players (checked {total_checked} members)."
        )
        body = "\n\n".join(lines[:50])
        await interaction.edit_original_response(content="📋 Upgrade check ready.")
        await interaction.followup.send(f"{header}\n\n{body}", ephemeral=False)
# ============================
# BASE LINK COMMANDS
# ============================

BASE_TYPES = ["war", "legend", "anti2", "blizzard"]

def _normalize_tag(tag: str) -> str:
    tag = (tag or "").strip().upper()
    if tag and not tag.startswith("#"):
        tag = "#" + tag
    return tag

@client.tree.command(name="setbase", description="Save a base link for your account (war / legend / anti2 / blizzard).")
@app_commands.describe(
    base_type="Type of base (war, legend, anti2, blizzard).",
    link="Clash of Clans base link.",
    name="Name/label for this base (e.g. Anti 2 Ring #1).",
    tag="(Optional) Player tag, if you are not linked with /link."
)
@app_commands.choices(
    base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
    ]
)
async def setbase(
    interaction: discord.Interaction,
    base_type: app_commands.Choice[str],
    link: str,
    name: str,
    tag: str | None = None
):
    await interaction.response.send_message("📥 Saving base...", ephemeral=True)

    # determine tag (either given, or from /link)
    if tag:
        tag_norm = _normalize_tag(tag)
    else:
        tag_norm = get_linked_tag_for_user(interaction.user.id)
        if not tag_norm:
            await interaction.edit_original_response(
                content="❌ You are not linked yet. Use `/link` or provide a `tag:` in this command."
            )
            return

    link = link.strip()
    if not link:
        await interaction.edit_original_response(content="❌ Please provide a valid base link.")
        return

    bases = load_bases()
    player_bases = bases.get(tag_norm, {})
    t = base_type.value  # "war", "legend", etc.

    entry_list = player_bases.get(t, [])
    if not isinstance(entry_list, list):
        entry_list = []

    entry = {
        "name": name,
        "link": link,
        "addedBy": str(interaction.user.id),
        "addedAt": datetime.now(timezone.utc).isoformat(),
    }
    entry_list.append(entry)
    player_bases[t] = entry_list
    bases[tag_norm] = player_bases
    save_bases(bases)

    emb = discord.Embed(
        title="✅ Base Saved",
        color=0x2ecc71,
        timestamp=datetime.now(timezone.utc)
    )
    emb.add_field(name="Player Tag", value=tag_norm, inline=True)
    emb.add_field(name="Type", value=t, inline=True)
    emb.add_field(name="Name", value=name, inline=False)
    emb.add_field(name="Link", value=link, inline=False)
    emb.set_footer(text="Use /getbase to retrieve it later.")

    await interaction.edit_original_response(content="✅ Base saved!", embed=emb)


@client.tree.command(name="getbase", description="Get a base link (latest) for a given type.")
@app_commands.describe(
    base_type="Type of base (war, legend, anti2, blizzard).",
    tag="(Optional) Player tag; if omitted, uses your linked account."
)
@app_commands.choices(
    base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
    ]
)
async def getbase(
    interaction: discord.Interaction,
    base_type: app_commands.Choice[str],
    tag: str | None = None
):
    await interaction.response.send_message("📤 Fetching base...", ephemeral=True)

    if tag:
        tag_norm = _normalize_tag(tag)
    else:
        tag_norm = get_linked_tag_for_user(interaction.user.id)
        if not tag_norm:
            await interaction.edit_original_response(
                content="❌ You are not linked yet. Use `/link` or provide a `tag:` in this command."
            )
            return

    bases = load_bases()
    player_bases = bases.get(tag_norm, {})
    t = base_type.value
    entries = player_bases.get(t, [])

    if not entries:
        await interaction.edit_original_response(
            content=f"⚠️ No `{t}` bases saved for `{tag_norm}`."
        )
        return

    # show the latest one
    entry = entries[-1]
    emb = discord.Embed(
        title=f"🏰 {t.capitalize()} Base — {entry.get('name','Unnamed')}",
        color=0x3498db,
        timestamp=datetime.now(timezone.utc)
    )
    emb.add_field(name="Player Tag", value=tag_norm, inline=True)
    emb.add_field(name="Base Name", value=entry.get("name", "Unnamed"), inline=False)
    emb.add_field(name="Link", value=entry.get("link","(missing)"), inline=False)
    emb.set_footer(text=f"{len(entries)} {t} bases saved; showing latest.")

    await interaction.edit_original_response(content="✅ Base fetched:", embed=emb)


@client.tree.command(name="basebook", description="Show all saved bases for your account (or a given tag).")
@app_commands.describe(
    tag="(Optional) Player tag; if omitted, uses your linked account."
)
async def basebook(interaction: discord.Interaction, tag: str | None = None):
    await interaction.response.send_message("📚 Building base book...", ephemeral=True)

    if tag:
        tag_norm = _normalize_tag(tag)
    else:
        tag_norm = get_linked_tag_for_user(interaction.user.id)
        if not tag_norm:
            await interaction.edit_original_response(
                content="❌ You are not linked yet. Use `/link` or provide a `tag:` in this command."
            )
            return

    bases = load_bases()
    player_bases = bases.get(tag_norm, {})

    if not player_bases:
        await interaction.edit_original_response(
            content=f"⚠️ No bases saved for `{tag_norm}`."
        )
        return

    lines = []
    for t, entries in player_bases.items():
        if not isinstance(entries, list) or not entries:
            continue
        lines.append(f"**{t.capitalize()} Bases:**")
        for e in entries[:10]:  # limit to 10 per type for safety
            nm = e.get("name","Unnamed")
            lk = e.get("link","(missing link)")
            lines.append(f"• **{nm}** → {lk}")
        lines.append("")  # blank line

    if not lines:
        await interaction.edit_original_response(
            content=f"⚠️ No bases saved for `{tag_norm}`."
        )
        return

    text = "\n".join(lines)
    await interaction.edit_original_response(
        content=f"📚 **Base Book for `{tag_norm}`**\n\n{text}"
    )
# ============================
# AUTO ROLE SYNC (TH ROLES)
# ============================

@client.tree.command(name="syncroles", description="Sync TH roles (TH1–TH18) for all linked players.")
@app_commands.choices(
    clan_tag=[
        app_commands.Choice(name=clan["name"], value=clan["tag"])
        for clan in CLANS
    ] + [app_commands.Choice(name="ALL CLANS", value="ALL")]
)
@app_commands.describe(
    clan_tag="Choose a clan or ALL."
)
async def syncroles(interaction: discord.Interaction, clan_tag: app_commands.Choice[str]):

    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ Use this inside a server, not in DMs.",
            ephemeral=True
        )
        return

    await interaction.response.send_message("🔄 Syncing TH roles…", ephemeral=True)

    guild = interaction.guild
    links = load_json(LINKS_FILE) or {}  # {clash_tag: discord_id}

    # Determine clan selection
    if clan_tag.value == "ALL":
        target_clans = [c["tag"] for c in CLANS]
    else:
        target_clans = [clan_tag.value]

    updated_count = 0
    created_count = 0

    for ctag in target_clans:
        members = await get_clan_member_list(ctag)
        if not members:
            continue

        for m in members:
            clash_tag = _normalize_tag(m.get("tag", ""))
            th = m.get("townHallLevel")
            if not clash_tag or not th:
                continue

            discord_id = links.get(clash_tag)
            if not discord_id:
                continue

            member = guild.get_member(int(discord_id))
            if not member:
                continue

            # Role info
            role_name = f"TH{th}"
            desired_role = discord.utils.get(guild.roles, name=role_name)

            # Auto-create missing TH role
            if desired_role is None:
                color_value = TH_COLORS.get(th, 0x95A5A6)
                try:
                    desired_role = await guild.create_role(
                        name=role_name,
                        color=discord.Color(color_value),
                        reason="CC2 Clash Bot auto-create TH Role"
                    )
                    created_count += 1
                except Exception:
                    continue  # skip if permission missing

            # Assign role, allow multiple TH roles (multi accounts)
            if desired_role not in member.roles:
                try:
                    await member.add_roles(desired_role, reason="TH Sync update")
                    updated_count += 1
                except:
                    pass

    # Final result message
    msg = (
        f"🏰 **TH Role Sync Complete**\n"
        f"👤 Updated: **{updated_count}** members\n"
        f"🆕 Created: **{created_count}** new TH roles\n"
        f"📌 Multi-account support enabled\n"
    )
    await interaction.edit_original_response(content=msg)
    
# ============================================
# /addclan and /removeclan (no admin restriction per your choice C)
# ============================================

# track tasks per clan so we can start/stop dynamically
running_tasks: Dict[str, List[asyncio.Task]] = {}

def start_clan_tasks(clan: Dict[str,str]):
    clan_tag = clan["tag"]
    # avoid double starting
    if clan_tag in running_tasks:
        return
    t1 = asyncio.create_task(track_clan(clan))
    t2 = asyncio.create_task(war_tracker(clan))
    t3 = asyncio.create_task(hero_upgrade_loop(clan))
    t4 = asyncio.create_task(upgrade_alert_loop(clan))
    running_tasks[clan_tag] = [t1, t2, t3, t4]

def stop_clan_tasks(clan_tag: str):
    tasks = running_tasks.get(clan_tag)
    if not tasks:
        return
    for t in tasks:
        t.cancel()
    running_tasks.pop(clan_tag, None)

@client.tree.command(name="addclan", description="Add a new clan to the monitored list.")
@app_commands.describe(name="Clan name (any label you want)", tag="Clan tag (example: #PQUCURCQ)")
async def addclan(interaction: discord.Interaction, name: str, tag: str):
    await interaction.response.send_message("➕ Adding clan...", ephemeral=True)
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm

    if get_clan_by_tag(tag_norm):
        await interaction.edit_original_response(
            content=f"❌ Clan with tag `{tag_norm}` is already in the list."
        )
        return

    # try fetching clan to validate
    data = await coc_get(f"/clans/{urllib.parse.quote(tag_norm)}")
    if not data:
        await interaction.edit_original_response(
            content=f"❌ Could not validate clan tag `{tag_norm}` via API."
        )
        return

    display_name = name.strip() or data.get("name") or "Unnamed Clan"

    new_clan = {"name": display_name, "tag": tag_norm}
    CLANS.append(new_clan)
    save_clans(CLANS)

    # load strict cache & start background tasks
    try:
        strict_join_cache[tag_norm] = load_strict_cache(tag_norm)
    except Exception:
        strict_join_cache[tag_norm] = set()
    start_clan_tasks(new_clan)

    await interaction.edit_original_response(
        content=f"✅ Added clan **{display_name}** (`{tag_norm}`) and started tracking."
    )

@client.tree.command(name="removeclan", description="Remove a clan from monitored list (stops tracking).")
@app_commands.describe(clan="Select the clan to remove")
@app_commands.autocomplete(clan=clan_autocomplete)
async def removeclan(interaction: discord.Interaction, clan: str):
    await interaction.response.send_message("➖ Removing clan...", ephemeral=True)
    c_obj = get_clan_by_tag(clan)
    if not c_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return

    tag_norm = c_obj["tag"]
    name = c_obj["name"]

    # remove from list
    global CLANS
    CLANS = [c for c in CLANS if c["tag"].upper() != tag_norm.upper()]
    save_clans(CLANS)

    # stop background tasks
    stop_clan_tasks(tag_norm)


# ========== Admin maintenance commands ==========
@client.tree.command(name="clearcache", description="Clear in-memory caches (safe). Admins only.")
@app_commands.describe(confirm="Set to true to actually clear caches")
async def clearcache(interaction: discord.Interaction, confirm: bool = False):
    # Restrict to server administrators
    if interaction.guild and not (
        interaction.user.guild_permissions.manage_guild or interaction.user.guild_permissions.administrator
    ):
        await interaction.response.send_message("❌ Only server admins may run this command.", ephemeral=True)
        return

    # Report current counts
    cache_stats = api_cache.get_stats()
    pending_total = len(getattr(request_deduplicator, "_pending", {}))

    if not confirm:
        await interaction.response.send_message(
            content=(
                "⚠️ Dry run: This will clear in-memory API caches and remove completed pending "
                "requests from the deduplicator.\n\n"
                f"API cache keys: **{cache_stats.get('total_keys', 0)}**\n"
                f"Deduplicator pending entries: **{pending_total}**\n\n"
                "Re-run the command with `confirm=true` to perform the clear."
            ),
            ephemeral=True,
        )
        return

    # Perform clear
    await interaction.response.send_message("🧹 Clearing caches now…", ephemeral=False)
    try:
        await api_cache.clear()
        dd = await request_deduplicator.clear()
        await interaction.edit_original_response(content=(
            f"✅ Cache cleared. API keys removed: **{cache_stats.get('total_keys',0)}**.\n"
            f"Deduplicator: total={dd.get('total',0)}, removed_done={dd.get('removed_done',0)}"
        ))
    except Exception as e:
        await interaction.edit_original_response(content=(f"❌ Error clearing caches: {e}"))


@client.tree.command(name="cleanup", description="Find (and optionally remove) __pycache__ and compiled files. Admins only.")
@app_commands.describe(force="Set to true to delete files; default is dry-run (false)")
async def cleanup(interaction: discord.Interaction, force: bool = False):
    if interaction.guild and not (
        interaction.user.guild_permissions.manage_guild or interaction.user.guild_permissions.administrator
    ):
        await interaction.response.send_message("❌ Only server admins may run this command.", ephemeral=True)
        return

    # Determine project root (directory containing this script)
    root = Path(__file__).resolve().parent

    await interaction.response.send_message("🔎 Scanning repository for candidate cleanup files (this may take a moment)…", ephemeral=True)

    def scan_for_candidates(root_path: Path):
        pycache_dirs = []
        compiled_files = []
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(root_path):
            # skip hidden VCS folders (just in case)
            if ".git" in dirpath:
                continue
            if "__pycache__" in dirpath:
                pycache_dirs.append(dirpath)
            for fn in filenames:
                if fn.endswith(('.pyc', '.pyo')) or fn in ("Thumbs.db",):
                    fp = os.path.join(dirpath, fn)
                    try:
                        sz = os.path.getsize(fp)
                    except Exception:
                        sz = 0
                    compiled_files.append((fp, sz))
                    total_size += sz
        return pycache_dirs, compiled_files, total_size

    pycache_dirs, compiled_files, total_size = await asyncio.to_thread(scan_for_candidates, root)

    if not pycache_dirs and not compiled_files:
        await interaction.edit_original_response(content="✅ No candidate cleanup files found.")
        return

    # Dry-run message
    summary_lines = [f"Found {len(pycache_dirs)} __pycache__ dirs and {len(compiled_files)} compiled files (~{total_size/1024:.1f} KB)"]
    if len(pycache_dirs) > 0:
        summary_lines.append("__pycache__ directories (sample up to 10):")
        summary_lines.extend(pycache_dirs[:10])
    if len(compiled_files) > 0:
        summary_lines.append("Compiled files (sample up to 10):")
        summary_lines.extend([f"{p} ({s} bytes)" for p, s in compiled_files[:10]])

    if not force:
        summary_lines.append('\nRun `/cleanup force:true` to actually delete these files (admin only).')
        await interaction.edit_original_response(content="\n".join(summary_lines))
        return

    # Perform deletion
    await interaction.edit_original_response(content="🧹 Deleting candidate files now…")

    def perform_deletion(py_dirs, comp_files):
        deleted = 0
        freed = 0
        # remove compiled files
        for fp, sz in comp_files:
            try:
                os.remove(fp)
                deleted += 1
                freed += sz
            except Exception:
                pass
        # remove __pycache__ directories
        for dp in py_dirs:
            try:
                shutil.rmtree(dp)
                deleted += 1
            except Exception:
                pass
        return deleted, freed

    deleted, freed = await asyncio.to_thread(perform_deletion, pycache_dirs, compiled_files)
    await interaction.edit_original_response(content=(f"✅ Cleanup complete. Deleted {deleted} items and freed ~{freed/1024:.1f} KB."))
# ============================================
# STARTUP TASKS, on_ready, RUN
# ============================================

_tasks_started = False

async def start_background_tasks_once():
    global _tasks_started
    if _tasks_started:
        return
    _tasks_started = True

    # load strict caches
    for c in CLANS:
        try:
            strict_join_cache[c["tag"]] = load_strict_cache(c["tag"])
        except Exception:
            strict_join_cache[c["tag"]] = set()

    # optional: name cache (not heavily used, but we keep it)
    _ = load_json(NAME_CACHE_FILE) or {}

    # send startup status embed
    try:
        ch = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
        if ch:
            for c in CLANS:
                emb = discord.Embed(
                    title=f"🔁 Startup Status — {c['name']}",
                    color=0x3498db,
                    timestamp=datetime.now(timezone.utc)
                )
                emb.description = (
                    "Baseline loaded — bot will not announce existing members. "
                    "Only real joins/leaves are announced."
                )
                await ch.send(embed=emb)
    except Exception:
        pass

    # start trackers & loops for each clan
    for c in CLANS:
        start_clan_tasks(c)

    # start global loops
    asyncio.create_task(fixed_time_reminder_loop())          # old interval-based (optional)
    asyncio.create_task(monthly_donation_snapshot_loop())

@client.event
async def on_ready():
    print(f"[READY] {client.user} (id: {client.user.id})")
    if client.http_session is None:
        client.http_session = aiohttp.ClientSession()
    try:
        await client.tree.sync()
        print("[INFO] Slash commands synced.")
    except Exception as e:
        await log(f"[WARN] Slash sync failed: {e}")
    asyncio.create_task(start_background_tasks_once())

@client.event
async def on_message(message: discord.Message):
    """Simple prefix handler for `cc2` commands (cc2 help, info, donations, roster, status)."""
    try:
        if message.author.bot:
            return
        text = message.content.strip()
        low = text.lower()
        # Accept 'cc2' or 'cc2 ' prefix (case-insensitive)
        if not (low == "cc2" or low.startswith("cc2 ")):
            return
        rest = text[3:].strip()
        if not rest:
            await message.channel.send("Usage: `cc2 <command> [args]` — try `cc2 help`")
            return
        parts = rest.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in ("help", "h"):
            help_text = (
                "**CC2 Prefix Commands**\n"
                "`cc2 help` — show this message\n"
                "`cc2 info <#TAG>` — show player info\n"
                "`cc2 donations <#TAG>` — show player donation stats\n"
                "`cc2 roster <CLAN_TAG>` — export clan roster CSV\n"
                "`cc2 status` — show bot status\n"
                "For full features use Slash Commands (recommended)."
            )
            await message.channel.send(help_text)
            return

        if cmd == "info":
            if not args:
                linked = get_linked_tag_for_user(message.author.id)
                if not linked:
                    await message.channel.send("Usage: `cc2 info <#TAG>` — or link your account with `/link` to use without a tag.")
                    return
                tag = linked
            else:
                tag = _normalize_tag(args[0])
            player = await get_player(tag)
            if not player:
                await message.channel.send(f"❌ Could not fetch player `{tag}`. Check tag or API.")
                return

            # Detect Minion Prince: prefer heroes list, fallback to pets (preserve previous behavior)
            mp_level = None
            # heroes first
            for h in player.get('heroes', []) or []:
                if 'minion prince' in (h.get('name') or '').lower():
                    try:
                        mp_level = int(h.get('level') or 0)
                    except Exception:
                        mp_level = h.get('level') or '?'
                    break
            # fallback to pets (older accounts may report MP there)
            if mp_level is None:
                pets_list = player.get('pets', []) or []
                for p in list(pets_list):
                    if 'minion prince' in (p.get('name') or '').lower():
                        try:
                            mp_level = int(p.get('level') or 0)
                        except Exception:
                            mp_level = p.get('level') or '?'
                        pets_list.remove(p)
                        player['pets'] = pets_list
                        break

            embed = build_info_embed(player, tag)

            # Add hero & lab summary (same as slash /info)
            try:
                # Exclude Minion Prince from rush calculations to keep metrics unchanged
                player_for_rush = dict(player)
                if isinstance(player_for_rush.get('heroes'), list):
                    player_for_rush['heroes'] = [h for h in player_for_rush['heroes'] if 'minion prince' not in (h.get('name') or '').lower()]
                hero_res = calculate_hero_rush(player_for_rush)
                lab_res = calculate_lab_rush(player)

                # Compact Rush Status (no hero levels here; heroes are shown once in the embed)
                rush_info = []
                if hero_res:
                    status = "Rushed" if hero_res['counted'] else "OK"
                    rush_info.append(f"Hero Rush: {hero_res['percent']:.2f}% ({status})")
                if lab_res:
                    status = "Rushed" if lab_res['counted'] else "OK"
                    rush_info.append(f"Lab Rush: {lab_res['percent']:.2f}% ({status})")
                if rush_info:
                    embed.add_field(name="\u200b", value="\u200b", inline=False)
                    embed.add_field(name=f"⚡ {_bold_upper('RUSH STATUS')}", value="\n".join(rush_info), inline=False)
            except Exception:
                # don't block the output on any unexpected calculation issue
                pass

            await message.channel.send(embed=embed, view=PlayerProfileView(tag, player.get('clan', {}).get('tag')))
            return

        if cmd in ("donations", "donation"):
            if not args:
                linked = get_linked_tag_for_user(message.author.id)
                if not linked:
                    await message.channel.send("Usage: `cc2 donations <#TAG>` — or link your account with `/link` to use without a tag.")
                    return
                tag = linked
            else:
                tag = _normalize_tag(args[0])
            player = await get_player(tag)
            if not player:
                await message.channel.send(f"❌ Could not fetch player `{tag}`. Check tag or API.")
                return
            # reuse donation embed construction
            lifetime = extract_lifetime_donations(player)
            seasonal = player.get("donations", 0)
            received = player.get("donationsReceived", 0)
            emb = discord.Embed(title=f"💝 Donation Stats — {player.get('name','Unknown')}", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
            emb.add_field(name="🆔 Tag", value=f"`{tag}`", inline=True)
            emb.add_field(name="📊 Lifetime Donations", value=(
                f"Troops: **{lifetime['troops_donated']:,}**\n"
                f"Spells: **{lifetime['spells_donated']:,}**\n"
                f"Siege: **{lifetime['siege_donated']:,}**\n"
                f"**Total: {lifetime['total_donated']:,}**"
            ), inline=False)
            emb.add_field(name="📅 Current Season", value=f"Sent: **{seasonal:,}**\nReceived: **{received:,}**", inline=True)
            await message.channel.send(embed=emb)
            return

        if cmd == "roster":
            if not args:
                await message.channel.send("Usage: `cc2 roster <CLAN_TAG>`")
                return
            clan_tag = _normalize_tag(args[0])
            c_obj = get_clan_by_tag(clan_tag)
            if not c_obj:
                await message.channel.send("❌ Clan not found in monitored list.")
                return
            members = await get_clan_member_list(c_obj['tag'])
            if not members:
                await message.channel.send("❌ Could not fetch clan or clan is empty.")
                return
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["name","tag","townHall","expLevel","trophies","role"])
            for m in members:
                writer.writerow([
                    m.get("name"), m.get("tag"), m.get("townHallLevel"), m.get("expLevel"), m.get("trophies"), m.get("role")
                ])
            output.seek(0)
            bio = io.BytesIO(output.getvalue().encode())
            bio.name = f"roster_{c_obj['tag'].replace('#','')}.csv"
            await message.channel.send(file=discord.File(bio, filename=bio.name))
            return

        if cmd == "status":
            u = client.user
            guilds = len(client.guilds)
            now = datetime.now(timezone.utc).isoformat()
            text = (
                f"**Bot:** {u}\n"
                f"**Guilds:** {guilds}\n"
                f"**Time:** {now}\n"
                f"**COC concurrency:** {COC_CONCURRENCY}\n"
                f"**Monitored clans:** {', '.join([c['name'] for c in CLANS])}"
            )
            await message.channel.send(text)
            return

        if cmd == "link":
            # link a player tag to the invoking Discord account
            if not args:
                await message.channel.send("Usage: `cc2 link <#TAG>`")
                return
            tag = _normalize_tag(args[0])
            links = load_json(LINKS_FILE) or {}
            links[tag] = str(message.author.id)
            save_json(LINKS_FILE, links)

            # Build embed and DM the user (best-effort)
            emb = discord.Embed(title="Account Linked ✅", color=0x2ecc71, timestamp=datetime.now(timezone.utc))
            emb.add_field(name="Discord User", value=f"{message.author.mention}", inline=True)
            emb.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
            emb.set_footer(text="Use /info tag:#PLAYER to get player info anytime.")
            try:
                await message.author.send(embed=emb)
                await message.channel.send(f"✅ Linked {message.author.mention} to `{tag}` — DM sent.")
            except Exception:
                await message.channel.send(f"✅ Linked {message.author.mention} to `{tag}`. (Couldn't DM you — check privacy settings.)")
            return

        if cmd == "unlink":
            links = load_json(LINKS_FILE) or {}
            removed = []
            for k, v in list(links.items()):
                if v == str(message.author.id):
                    del links[k]
                    removed.append(k)
            if removed:
                save_json(LINKS_FILE, links)
                await message.channel.send(f"✅ Unlinked your account from: {', '.join(removed)}")
            else:
                await message.channel.send("❌ No linked tag found for your account. Use `cc2 link <#TAG>` to link.")
            return

        if cmd == "whois":
            links = load_json(LINKS_FILE) or {}
            linked = [k for k, v in links.items() if v == str(message.author.id)]
            if linked:
                await message.channel.send(f"🔗 Linked tags for {message.author.mention}: {', '.join(linked)}")
            else:
                await message.channel.send("🔍 You have no linked tags. Use `cc2 link <#TAG>` to link your account.")
            return

        # unknown
        await message.channel.send(f"Unknown cc2 command: `{cmd}`. Try `cc2 help`.")
    except Exception as e:
        try:
            await message.channel.send(f"❌ Error running command: {e}")
        except Exception:
            pass

async def _close_session():
    if client.http_session:
        try:
            await client.http_session.close()
        except Exception:
            pass

# RUN
if __name__ == "__main__":
    if (
        not DISCORD_TOKEN or not COC_API_KEYS
        or DISCORD_TOKEN.startswith("token")
        or all(str(k).startswith("api") for k in COC_API_KEYS.values())
    ):
        print("[FATAL] Set DISCORD_TOKEN and COC_API_KEYS (at least one valid key) in the environment or config.")
    else:
        try:
            client.run(DISCORD_TOKEN)
        finally:
            # best-effort close if event loop still exists
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                    loop.run_until_complete(_close_session())
            except Exception:
                pass

