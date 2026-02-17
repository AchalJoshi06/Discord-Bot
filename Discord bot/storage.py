"""File-based storage helpers for persistent data."""
import json
import os
from typing import Optional, Dict, Any, List

from config import (
    LINKS_FILE, CLANS_FILE, BASES_FILE, MEMBERS_PREFIX, WAR_PREFIX,
    DONATION_SNAPSHOTS_FILE
)


def load_json(path: str) -> Optional[Any]:
    """Load JSON file, return None if file doesn't exist or is invalid."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[STORAGE] Error loading {path}: {e}")
            return None
    return None


def save_json(path: str, data: Any) -> bool:
    """Save data to JSON file. Returns True on success."""
    try:
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"[STORAGE] Error saving {path}: {e}")
        return False


def members_filename(tag: str) -> str:
    """Get filename for clan member cache."""
    return f"{MEMBERS_PREFIX}{tag.replace('#', '')}.json"


def war_filename(tag: str) -> str:
    """Get filename for war cache."""
    return f"{WAR_PREFIX}{tag.replace('#', '')}.json"


# ============================
# CLAN MANAGEMENT
# ============================

def load_clans() -> List[Dict[str, str]]:
    """Load clan list from file, with fallback to defaults."""
    data = load_json(CLANS_FILE)
    if isinstance(data, list) and data:
        # Sanitize and normalize
        out = []
        for c in data:
            name = str(c.get("name", "Unnamed"))
            tag = str(c.get("tag", "")).upper()
            if not tag.startswith("#"):
                tag = "#" + tag
            out.append({"name": name, "tag": tag})
        return out
    
    # Fallback defaults
    default = [
        {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
        {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
    ]
    save_json(CLANS_FILE, default)
    return default


def save_clans(clans: List[Dict[str, str]]) -> bool:
    """Save clan list to file."""
    return save_json(CLANS_FILE, clans)


# ============================
# LINK MANAGEMENT
# ============================

def load_links() -> Dict[str, str]:
    """Load Discord-Clash tag links."""
    return load_json(LINKS_FILE) or {}


def save_links(links: Dict[str, str]) -> bool:
    """Save Discord-Clash tag links."""
    return save_json(LINKS_FILE, links)


def get_linked_tag_for_user(user_id: int) -> Optional[str]:
    """Reverse lookup: Discord user ID -> Clash player tag."""
    links = load_links()
    for tag, did in links.items():
        if str(did) == str(user_id):
            return tag
    return None


# ============================
# BASE STORAGE
# ============================

def load_bases() -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Load base links storage."""
    data = load_json(BASES_FILE)
    return data if isinstance(data, dict) else {}


def save_bases(data: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> bool:
    """Save base links storage."""
    if not isinstance(data, dict):
        return False
    return save_json(BASES_FILE, data)


# ============================
# MEMBER CACHE (for join/leave tracking)
# ============================

def load_strict_cache(clan_tag: str) -> set:
    """Load persistent member cache for a clan."""
    data = load_json(members_filename(clan_tag))
    return set(data) if isinstance(data, list) else set()


def save_strict_cache(clan_tag: str, tags: set) -> bool:
    """Save persistent member cache for a clan."""
    return save_json(members_filename(clan_tag), list(tags))


# ============================
# DONATION SNAPSHOTS
# ============================

def load_donation_snapshots() -> Dict[str, List[Dict[str, Any]]]:
    """
    Load donation snapshots.
    Structure: {clan_tag: [{"date": "YYYY-MM", "members": {tag: {"seasonal": int, "lifetime": {...}}}}]}
    """
    data = load_json(DONATION_SNAPSHOTS_FILE)
    return data if isinstance(data, dict) else {}


def save_donation_snapshots(snapshots: Dict[str, List[Dict[str, Any]]]) -> bool:
    """Save donation snapshots."""
    if not isinstance(snapshots, dict):
        return False
    return save_json(DONATION_SNAPSHOTS_FILE, snapshots)


def get_latest_snapshot(clan_tag: str) -> Optional[Dict[str, Any]]:
    """Get the most recent snapshot for a clan."""
    snapshots = load_donation_snapshots()
    clan_snapshots = snapshots.get(clan_tag, [])
    if not clan_snapshots:
        return None
    # Sort by date (most recent first)
    sorted_snapshots = sorted(clan_snapshots, key=lambda x: x.get("date", ""), reverse=True)
    return sorted_snapshots[0] if sorted_snapshots else None


