"""Configuration settings for the Discord bot."""
import os
import sys
from typing import Dict, List


def _load_local_dotenv(dotenv_path: str | None = None) -> None:
    """Load key=value pairs from a local .env file into os.environ.

    Existing environment variables are not overwritten.
    """
    if dotenv_path is None:
        dotenv_path = os.path.join(os.path.dirname(__file__), ".env")

    if not os.path.exists(dotenv_path):
        return

    try:
        with open(dotenv_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()

                if not key:
                    continue

                if (
                    len(value) >= 2
                    and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'"))
                ):
                    value = value[1:-1]

                if key not in os.environ:
                    os.environ[key] = value
    except Exception:
        # Keep config import resilient even if .env is malformed.
        pass


# Load local .env (if present) before reading environment variables.
_load_local_dotenv()

# Load from environment variables only
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
COC_API_KEY = os.getenv("COC_API_KEY", "")

# Channel IDs
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "1439346726048633053"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", str(CHANNEL_ID)))
ANNOUNCE_CHANNEL_ID = int(os.getenv("ANNOUNCE_CHANNEL_ID", str(CHANNEL_ID)))
AUDIT_CHANNEL_ID = int(os.getenv("AUDIT_CHANNEL_ID", "0"))
BASE_LAYOUT_CHANNEL_ID = int(os.getenv("BASE_LAYOUT_CHANNEL_ID", "0"))
ATTACK_STRATEGY_CHANNEL_ID = int(os.getenv("ATTACK_STRATEGY_CHANNEL_ID", "0"))
LEADERSHIP_ROLE_ID = int(os.getenv("LEADERSHIP_ROLE_ID", "0"))
BOT_ADMIN_ROLE_ID = int(os.getenv("BOT_ADMIN_ROLE_ID", "0"))

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

# Legacy single-key configuration (read from env `COC_API_KEY`)
# Multi-key support was removed; set `COC_API_KEY` in environment if needed.

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
PET_RUSH_THRESHOLD = float(os.getenv("PET_RUSH_THRESHOLD", "25.0"))
WALL_RUSH_THRESHOLD = float(os.getenv("WALL_RUSH_THRESHOLD", "25.0"))

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

# Hero caps per TH level (includes Minion Prince)
HERO_CAPS: Dict[int, Dict[str, int]] = {
    7:  {"BK": 10,  "AQ": 0,   "GW": 0,  "RC": 0,  "MP": 0},
    8:  {"BK": 20,  "AQ": 10,  "GW": 0,  "RC": 0,  "MP": 0},
    9:  {"BK": 30,  "AQ": 30,  "GW": 0,  "RC": 0,  "MP": 0},
    10: {"BK": 40,  "AQ": 40,  "GW": 20, "RC": 0,  "MP": 0},
    11: {"BK": 50,  "AQ": 50,  "GW": 40, "RC": 0,  "MP": 0},
    12: {"BK": 65,  "AQ": 65,  "GW": 40, "RC": 0,  "MP": 0},
    13: {"BK": 75,  "AQ": 75,  "GW": 50, "RC": 25, "MP": 0},
    14: {"BK": 80,  "AQ": 80,  "GW": 55, "RC": 30, "MP": 0},
    15: {"BK": 90,  "AQ": 90,  "GW": 65, "RC": 40, "MP": 0},
    16: {"BK": 95,  "AQ": 95,  "GW": 70, "RC": 45, "MP": 0},
    17: {"BK": 100, "AQ": 100, "GW": 75, "RC": 50, "MP": 90},
    18: {"BK": 105, "AQ": 105, "GW": 80, "RC": 55, "MP": 95},
}

# Lab caps per TH level (troops + spells, excludes pets)
LAB_CAPS: Dict[int, int] = {
    1: 0, 2: 0, 3: 10, 4: 40,
    5: 90, 6: 160, 7: 250, 8: 360,
    9: 500, 10: 700, 11: 900, 12: 1100,
    13: 1300, 14: 1500, 15: 1700, 16: 1900,
    17: 2100, 18: 2300,
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

