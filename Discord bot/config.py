"""Configuration settings for the Discord bot."""
import os
import sys
import json
from typing import Dict, List

# Load from environment variables first
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
COC_API_KEY = os.getenv("COC_API_KEY", "")

# Fallback: Try to read from old file if environment variables not set
if not DISCORD_TOKEN or not COC_API_KEY:
    try:
        # Try importing from old file (for migration)
        if os.path.exists("discordwelcomebot.py"):
            import importlib.util
            spec = importlib.util.spec_from_file_location("old_bot", "discordwelcomebot.py")
            if spec and spec.loader:
                # Read file directly to extract tokens (safer than importing)
                with open("discordwelcomebot.py", "r", encoding="utf-8") as f:
                    content = f.read()
                    # Extract tokens from first two lines
                    lines = content.split('\n')[:2]
                    for line in lines:
                        if 'DISCORD_TOKEN' in line and '=' in line:
                            token = line.split('=')[1].strip().strip("'\"")
                            if token and not DISCORD_TOKEN:
                                DISCORD_TOKEN = token
                        elif 'COC_API_KEY' in line and '=' in line:
                            key = line.split('=')[1].strip().strip("'\"")
                            if key and not COC_API_KEY:
                                COC_API_KEY = key
    except Exception:
        pass  # Silently fail, will show error on startup

# Channel IDs
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1439346726048633053"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", str(CHANNEL_ID)))
ANNOUNCE_CHANNEL_ID = int(os.getenv("ANNOUNCE_CHANNEL_ID", str(CHANNEL_ID)))

# Intervals (seconds)
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))
WAR_POLL_INTERVAL = int(os.getenv("WAR_POLL_INTERVAL", "6"))
REMINDER_INTERVAL = int(os.getenv("REMINDER_INTERVAL", "7200"))  # 2 hours
UPGRADE_CHECK_INTERVAL = int(os.getenv("UPGRADE_CHECK_INTERVAL", "900"))  # 15 minutes
UPGRADE_ALERT_CHECK = int(os.getenv("UPGRADE_ALERT_CHECK", "300"))  # 5 minutes

# API Configuration
COC_CONCURRENCY = int(os.getenv("COC_CONCURRENCY", "6"))
COC_TIMEOUT = int(os.getenv("COC_TIMEOUT", "12"))
COC_API_BASE_URL = "https://api.clashofclans.com/v1"

# Support multiple API keys mapped by egress IP (JSON string env var or legacy single key)
# Example env: COC_API_KEYS='{"1.2.3.4":"KEY_A","5.6.7.8":"KEY_B"}'
COC_API_KEYS = {}
COC_API_KEYS_RAW = os.getenv("COC_API_KEYS", "")
if COC_API_KEYS_RAW:
    try:
        parsed = json.loads(COC_API_KEYS_RAW)
        if isinstance(parsed, dict):
            COC_API_KEYS = parsed
    except Exception:
        COC_API_KEYS = {}
# Backcompat: if no mapping provided, fall back to legacy COC_API_KEY
if not COC_API_KEYS and COC_API_KEY:
    COC_API_KEYS = {"*": COC_API_KEY}

COC_IP_DETECT_URL = os.getenv("COC_IP_DETECT_URL", "https://api.ipify.org")
COC_IP_CACHE_TTL = int(os.getenv("COC_IP_CACHE_TTL", "300"))  # seconds to cache detected IP and selected key

LEAVE_DEBOUNCE_COUNT = int(os.getenv("LEAVE_DEBOUNCE_COUNT", "2"))
SKIP_EMPTY_MEMBER_LIST = os.getenv("SKIP_EMPTY_MEMBER_LIST", "1") in ("1", "true", "True")

# Cache TTLs (seconds) - reduces API calls significantly
PLAYER_CACHE_TTL = int(os.getenv("PLAYER_CACHE_TTL", "300"))  # 5 minutes
CLAN_CACHE_TTL = int(os.getenv("CLAN_CACHE_TTL", "60"))  # 1 minute
WAR_CACHE_TTL = int(os.getenv("WAR_CACHE_TTL", "30"))  # 30 seconds

# File names
LINKS_FILE = "links.json"
NAME_CACHE_FILE = "names.json"
MEMBERS_PREFIX = "members_"
WAR_PREFIX = "war_"
CLANS_FILE = "clans.json"
BASES_FILE = "bases.json"

# Rush thresholds (configurable)
HERO_RUSH_THRESHOLD = float(os.getenv("HERO_RUSH_THRESHOLD", "5.0"))
LAB_RUSH_THRESHOLD = float(os.getenv("LAB_RUSH_THRESHOLD", "25.0"))
BASE_RUSH_THRESHOLD = float(os.getenv("BASE_RUSH_THRESHOLD", "5.0"))

# Kick suggestion thresholds
MIN_DONATION_RATIO = float(os.getenv("MIN_DONATION_RATIO", "0.5"))  # donations/received ratio
MIN_WAR_STARS = int(os.getenv("MIN_WAR_STARS", "0"))
INACTIVE_DAYS_THRESHOLD = int(os.getenv("INACTIVE_DAYS_THRESHOLD", "7"))

# Town Hall colors
TH_COLORS: Dict[int, int] = {
    18: 0x2C2F33,  # TH18 – dark metallic steel / high-tech
    17: 0x003F5D,  # TH17 – deep Prussian blue / justice
    16: 0xD4A017,  # TH16 – golden-yellow nature theme
    15: 0x3C2F7E,  # TH15 – enchanted indigo / magic
    14: 0x1ABB6D,  # TH14 – jungle green
    13: 0x009AA9,  # TH13 – frost turquoise / ice
    12: 0x1E62D0,  # TH12 – electric blue
    11: 0xDCDDDA,  # TH11 – white stone / sacred
    10: 0xA30419,  # TH10 – fire red / lava
    9: 0x1C1C1C,  # TH9 – black fortress
    8: 0x5B5B5B,  # TH8 – dark grey stone
    7: 0x6E6E6E,  # TH7 – castle stone
    6: 0xA78C5A,  # TH6 – golden pillars
    5: 0x9E7A5C,  # TH5 – muted stone
    4: 0xB56C24,  # TH4 – tan/orange stone
    3: 0xC77A2E,  # TH3 – darker orange tiles
    2: 0xD98C36,  # TH2 – orange brick
    1: 0xE39B4A,  # TH1 – light orange wood
}

# Hero caps per TH level
HERO_CAPS: Dict[int, Dict[str, int]] = {
    8: {"BK": 10, "AQ": 0, "GW": 0, "RC": 0},
    9: {"BK": 20, "AQ": 0, "GW": 0, "RC": 0},
    10: {"BK": 30, "AQ": 30, "GW": 0, "RC": 0},
    11: {"BK": 40, "AQ": 40, "GW": 0, "RC": 0},
    12: {"BK": 50, "AQ": 50, "GW": 20, "RC": 0},
    13: {"BK": 65, "AQ": 65, "GW": 40, "RC": 0},
    14: {"BK": 75, "AQ": 75, "GW": 50, "RC": 25},
    15: {"BK": 85, "AQ": 85, "GW": 60, "RC": 35},
    16: {"BK": 90, "AQ": 90, "GW": 65, "RC": 40},
    17: {"BK": 95, "AQ": 95, "GW": 70, "RC": 45},
    18: {"BK": 100, "AQ": 100, "GW": 75, "RC": 50},
}

# Lab caps per TH level
LAB_CAPS: Dict[int, Dict[str, int]] = {
    1: {"total": 0}, 2: {"total": 0}, 3: {"total": 10}, 4: {"total": 40},
    5: {"total": 90}, 6: {"total": 160}, 7: {"total": 260}, 8: {"total": 390},
    9: {"total": 560}, 10: {"total": 760}, 11: {"total": 950}, 12: {"total": 1100},
    13: {"total": 1300}, 14: {"total": 1500}, 15: {"total": 1700}, 16: {"total": 1900},
    17: {"total": 2100}, 18: {"total": 2300},
}

# Base caps per TH level
BASE_CAPS: Dict[int, Dict[str, int]] = {
    1: {"total": 0}, 2: {"total": 15}, 3: {"total": 55}, 4: {"total": 140},
    5: {"total": 260}, 6: {"total": 430}, 7: {"total": 650}, 8: {"total": 920},
    9: {"total": 1250}, 10: {"total": 1650}, 11: {"total": 2000}, 12: {"total": 2350},
    13: {"total": 2700}, 14: {"total": 3100}, 15: {"total": 3500}, 16: {"total": 3900},
    17: {"total": 4300}, 18: {"total": 4700},
}

# Base types
BASE_TYPES = ["war", "legend", "anti2", "blizzard"]

# Donation tracking
DONATION_SNAPSHOTS_FILE = "donation_snapshots.json"
MONTHLY_SNAPSHOT_DAY = int(os.getenv("MONTHLY_SNAPSHOT_DAY", "1"))  # Day of month to take snapshot

