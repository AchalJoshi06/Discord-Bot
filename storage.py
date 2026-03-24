"""File-based storage helpers for persistent data."""
import json
import logging
import os
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

try:
    import db as db_layer
except Exception:  # pragma: no cover - fallback when db module unavailable
    db_layer = None

from config import (
    LINKS_FILE, CLANS_FILE, BASES_FILE, MEMBERS_PREFIX, WAR_PREFIX,
    DONATION_SNAPSHOTS_FILE
)

logger = logging.getLogger("cc2bot.storage")

SETTINGS_FILE = "settings.json"
DEFAULT_SETTINGS = {
    "raid_reminder_enabled": True,
    "raid_dm_reminder_enabled": False,
    "war_reminder_enabled": True,
    "kick_review_day": 0,
    "base_layout_channel_id": 0,
    "base_layout_channel_ids": [],
    "attack_strategy_channel_id": 0,
    "primary_tags": {},
    "guild_settings": {},
}
MEMBER_ACTIVITY_FILE = "member_activity.json"
WAR_RESULTS_FILE = "war_results.json"
WAR_PLAYER_STATS_FILE = "war_player_stats.json"
WAR_ATTACK_LOG_FILE = "war_attack_log.json"
RAID_HISTORY_FILE = "raid_history.json"
CAPITAL_PROGRESS_FILE = "capital_progress.json"
MONTHLY_LEADERBOARD_FILE = "monthly_leaderboard.json"
ACHIEVEMENTS_FILE = "achievements.json"
CHALLENGES_FILE = "challenges.json"
TRANSFERS_FILE = "transfers.json"
REMINDERS_FILE = "reminders.json"
RUSH_HISTORY_ENTRIES_FILE = "rush_history_entries.json"
ATTACK_STRATEGIES_FILE = "attack_strategies.json"


def _load_dict_blob(name: str) -> Dict[str, Any]:
    data = None
    if db_layer is not None:
        data = db_layer.load_json_blob(name)
    if not isinstance(data, dict):
        data = load_json(name)
    return data if isinstance(data, dict) else {}


def _save_dict_blob(name: str, data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    json_ok = save_json(name, data)
    db_ok = True
    if db_layer is not None:
        db_ok = db_layer.save_json_blob(name, data)
    return bool(json_ok and db_ok)


def load_json(path: str) -> Optional[Any]:
    """Load JSON file, return None if file doesn't exist or is invalid."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error("Error loading %s: %s", path, e)
            return None
    return None


def save_json(path: str, data: Any) -> bool:
    """Save data to JSON file. Returns True on success."""
    try:
        folder = os.path.dirname(path) if os.path.dirname(path) else "."
        os.makedirs(folder, exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        logger.error("Error saving %s: %s", path, e)
        return False


def load_settings() -> Dict[str, Any]:
    """Load runtime settings with defaults."""
    file_data = load_json(SETTINGS_FILE)
    if not isinstance(file_data, dict):
        file_data = {}

    db_data = None
    if db_layer is not None:
        db_data = db_layer.load_settings()
    if not isinstance(db_data, dict):
        db_data = {}

    # Priority: defaults < DB settings < file settings
    # File settings should win to respect explicit manual configuration.
    merged = dict(DEFAULT_SETTINGS)
    merged.update(db_data)
    merged.update(file_data)
    return merged


def save_settings(settings: Dict[str, Any]) -> bool:
    """Persist runtime settings."""
    if not isinstance(settings, dict):
        return False
    merged = dict(DEFAULT_SETTINGS)
    merged.update(settings)
    json_ok = save_json(SETTINGS_FILE, merged)
    db_ok = True
    if db_layer is not None:
        db_ok = db_layer.save_settings(merged)
    return bool(json_ok and db_ok)


def load_guild_settings(guild_id: int) -> Dict[str, Any]:
    """Load per-guild settings map for a Discord guild ID."""
    settings = load_settings()
    all_guild = settings.get("guild_settings", {})
    if not isinstance(all_guild, dict):
        return {}
    row = all_guild.get(str(guild_id), {})
    return row if isinstance(row, dict) else {}


def save_guild_settings(guild_id: int, guild_settings: Dict[str, Any], merge: bool = True) -> bool:
    """Persist per-guild settings for a Discord guild ID."""
    if not isinstance(guild_settings, dict):
        return False

    settings = load_settings()
    all_guild = settings.get("guild_settings")
    if not isinstance(all_guild, dict):
        all_guild = {}

    gid = str(guild_id)
    existing = all_guild.get(gid, {}) if isinstance(all_guild.get(gid, {}), dict) else {}
    payload = dict(existing) if merge else {}
    payload.update(guild_settings)
    all_guild[gid] = payload
    settings["guild_settings"] = all_guild
    return save_settings(settings)


def get_effective_setting(key: str, default: Any = None, guild_id: Optional[int] = None) -> Any:
    """Get setting value with optional per-guild override fallback to global."""
    settings = load_settings()
    if guild_id is not None:
        guild_map = settings.get("guild_settings", {})
        if isinstance(guild_map, dict):
            row = guild_map.get(str(guild_id), {})
            if isinstance(row, dict) and key in row:
                return row.get(key)

    if key in settings:
        return settings.get(key)
    return default


def _sanitize_clans(data: Any) -> List[Dict[str, str]]:
    if not isinstance(data, list):
        return []
    out: List[Dict[str, str]] = []
    for c in data:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name", "Unnamed"))
        tag = str(c.get("tag", "")).upper()
        if not tag:
            continue
        if not tag.startswith("#"):
            tag = "#" + tag
        out.append({"name": name, "tag": tag})
    return out


def load_guild_clans(guild_id: int) -> List[Dict[str, str]]:
    """Load monitored clans for a guild. Returns empty list when not configured."""
    row = load_guild_settings(guild_id)
    return _sanitize_clans(row.get("clans", []))


def save_guild_clans(guild_id: int, clans: List[Dict[str, str]]) -> bool:
    """Persist monitored clans for a guild."""
    normalized = _sanitize_clans(clans)
    return save_guild_settings(guild_id, {"clans": normalized}, merge=True)


def get_effective_clans(guild_id: Optional[int], global_clans: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Return guild-specific clans when configured; otherwise fallback to global clans."""
    if guild_id is None:
        return _sanitize_clans(global_clans)
    scoped = load_guild_clans(guild_id)
    if scoped:
        return scoped
    return _sanitize_clans(global_clans)


def members_filename(tag: str) -> str:
    """Get filename for clan member cache."""
    return f"{MEMBERS_PREFIX}{tag.replace('#', '')}.json"


def war_filename(tag: str) -> str:
    """Get filename for war cache."""
    return f"{WAR_PREFIX}{tag.replace('#', '')}.json"


def load_war_baseline(clan_tag: str) -> Dict[str, Any]:
    """Load per-clan war attack baseline map for tracker diffing."""
    data = load_json(war_filename(clan_tag))
    return data if isinstance(data, dict) else {}


def save_war_baseline(clan_tag: str, baseline: Dict[str, Any]) -> bool:
    """Persist per-clan war attack baseline map used by war tracker."""
    if not isinstance(baseline, dict):
        return False
    return save_json(war_filename(clan_tag), baseline)


# ============================
# CLAN MANAGEMENT
# ============================

def load_clans() -> List[Dict[str, str]]:
    """Load clan list from file, with fallback to defaults."""
    data = None
    if db_layer is not None:
        data = db_layer.load_clans()
    if not isinstance(data, list) or not data:
        data = load_json(CLANS_FILE)
    if isinstance(data, list) and data:
        return _sanitize_clans(data)
    
    # Fallback defaults
    default = [
        {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
        {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
    ]
    save_json(CLANS_FILE, default)
    return default


def save_clans(clans: List[Dict[str, str]]) -> bool:
    """Save clan list to file."""
    json_ok = save_json(CLANS_FILE, clans)
    db_ok = True
    if db_layer is not None:
        db_ok = db_layer.save_clans(clans)
    return bool(json_ok and db_ok)


# ============================
# LINK MANAGEMENT
# ============================

def load_links() -> Dict[str, str]:
    """Load Discord-Clash tag links."""
    return load_json(LINKS_FILE) or {}


def save_links(links: Dict[str, str]) -> bool:
    """Save Discord-Clash tag links."""
    return save_json(LINKS_FILE, links)


def _normalize_tag_value(tag: str) -> str:
    t = str(tag or "").strip().upper()
    if t and not t.startswith("#"):
        t = "#" + t
    return t


def get_linked_tags_for_user(user_id: int) -> List[str]:
    """Return all linked player tags for a Discord user (in insertion order)."""
    links = load_links()
    tags: List[str] = []
    target = str(user_id)
    for raw_tag, did in links.items():
        if str(did) != target:
            continue
        norm = _normalize_tag_value(raw_tag)
        if norm and norm not in tags:
            tags.append(norm)
    return tags


def get_primary_tag_for_user(user_id: int) -> Optional[str]:
    """Get a user's configured primary tag, falling back to first linked tag."""
    tags = get_linked_tags_for_user(user_id)
    if not tags:
        return None

    settings = load_settings()
    primary_map = settings.get("primary_tags", {})
    if isinstance(primary_map, dict):
        saved = primary_map.get(str(user_id))
        if isinstance(saved, str):
            saved_norm = _normalize_tag_value(saved)
            if saved_norm in tags:
                return saved_norm

    return tags[0]


def set_primary_tag_for_user(user_id: int, tag: str) -> bool:
    """Set primary linked player tag for a Discord user.

    Returns False if tag is not linked to the user.
    """
    norm_tag = _normalize_tag_value(tag)
    linked = get_linked_tags_for_user(user_id)
    if norm_tag not in linked:
        return False

    settings = load_settings()
    primary_map = settings.get("primary_tags")
    if not isinstance(primary_map, dict):
        primary_map = {}
    primary_map[str(user_id)] = norm_tag
    settings["primary_tags"] = primary_map
    return save_settings(settings)


def get_linked_tag_for_user(user_id: int) -> Optional[str]:
    """Reverse lookup: Discord user ID -> primary Clash player tag."""
    return get_primary_tag_for_user(user_id)


def get_linked_user_for_tag(tag: str) -> Optional[int]:
    """Forward lookup: Clash player tag -> Discord user ID."""
    links = load_links()
    norm_tag = _normalize_tag_value(tag)
    if norm_tag in links:
        try:
            return int(links[norm_tag])
        except (ValueError, TypeError):
            return None
    # Fallback for legacy unnormalized keys in links.json
    for raw_tag, did in links.items():
        if _normalize_tag_value(raw_tag) != norm_tag:
            continue
        try:
            return int(did)
        except (ValueError, TypeError):
            return None
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
# ATTACK STRATEGY STORAGE
# ============================

def load_attack_strategies() -> List[Dict[str, Any]]:
    """Load attack strategy rows."""
    data = None
    if db_layer is not None:
        data = db_layer.load_json_blob(ATTACK_STRATEGIES_FILE)
    if not isinstance(data, list):
        data = load_json(ATTACK_STRATEGIES_FILE)
    return data if isinstance(data, list) else []


def save_attack_strategies(rows: List[Dict[str, Any]]) -> bool:
    """Save attack strategy rows."""
    if not isinstance(rows, list):
        return False
    json_ok = save_json(ATTACK_STRATEGIES_FILE, rows)
    db_ok = True
    if db_layer is not None:
        db_ok = bool(db_layer.save_json_blob(ATTACK_STRATEGIES_FILE, rows))
    return bool(json_ok and db_ok)


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
    data = None
    if db_layer is not None:
        data = db_layer.load_json_blob(DONATION_SNAPSHOTS_FILE)
    if not isinstance(data, dict):
        data = load_json(DONATION_SNAPSHOTS_FILE)
    return data if isinstance(data, dict) else {}


def save_donation_snapshots(snapshots: Dict[str, List[Dict[str, Any]]]) -> bool:
    """Save donation snapshots."""
    if not isinstance(snapshots, dict):
        return False
    if db_layer is not None:
        return bool(db_layer.save_json_blob(DONATION_SNAPSHOTS_FILE, snapshots))
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


# ============================
# MEMBER ACTIVITY TRACKING
# ============================

def load_member_activity() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Load member activity map: {clan_tag: {player_tag: {last_seen, name}}}."""
    data = None
    if db_layer is not None:
        data = db_layer.load_member_activity()
    if not isinstance(data, dict):
        data = load_json(MEMBER_ACTIVITY_FILE)
    return data if isinstance(data, dict) else {}


def save_member_activity(activity: Dict[str, Dict[str, Dict[str, Any]]]) -> bool:
    """Save member activity map."""
    if not isinstance(activity, dict):
        return False
    if db_layer is not None:
        return bool(db_layer.save_member_activity(activity))
    # Legacy fallback only when DB layer is unavailable.
    return save_json(MEMBER_ACTIVITY_FILE, activity)


def load_war_results() -> Dict[str, List[Dict[str, Any]]]:
    data = _load_dict_blob(WAR_RESULTS_FILE)
    return data if isinstance(data, dict) else {}


def save_war_results(data: Dict[str, List[Dict[str, Any]]]) -> bool:
    if not isinstance(data, dict):
        return False
    if db_layer is not None:
        return bool(db_layer.save_json_blob(WAR_RESULTS_FILE, data))
    return save_json(WAR_RESULTS_FILE, data)


def load_war_player_stats() -> Dict[str, Dict[str, Dict[str, Any]]]:
    data = _load_dict_blob(WAR_PLAYER_STATS_FILE)
    return data if isinstance(data, dict) else {}


def save_war_player_stats(data: Dict[str, Dict[str, Dict[str, Any]]]) -> bool:
    if not isinstance(data, dict):
        return False
    if db_layer is not None:
        return bool(db_layer.save_json_blob(WAR_PLAYER_STATS_FILE, data))
    return save_json(WAR_PLAYER_STATS_FILE, data)


def load_war_attack_log() -> Dict[str, List[Dict[str, Any]]]:
    data = _load_dict_blob(WAR_ATTACK_LOG_FILE)
    return data if isinstance(data, dict) else {}


def save_war_attack_log(data: Dict[str, List[Dict[str, Any]]]) -> bool:
    if not isinstance(data, dict):
        return False
    if db_layer is not None:
        return bool(db_layer.save_json_blob(WAR_ATTACK_LOG_FILE, data))
    return save_json(WAR_ATTACK_LOG_FILE, data)


def load_raid_history() -> Dict[str, Any]:
    return _load_dict_blob(RAID_HISTORY_FILE)


def save_raid_history(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    if db_layer is not None:
        return bool(db_layer.save_json_blob(RAID_HISTORY_FILE, data))
    return save_json(RAID_HISTORY_FILE, data)


def create_rush_history_entry(
    player_tag: str,
    score: float,
    payload: Dict[str, Any],
    clan_tag: Optional[str] = None,
    created_at_iso: Optional[str] = None,
) -> bool:
    """Persist a rush score history point for one player."""
    created_at = str(created_at_iso or datetime.now(timezone.utc).isoformat())

    if db_layer is not None and hasattr(db_layer, "save_rush_history_entry"):
        return bool(
            db_layer.save_rush_history_entry(
                player_tag=str(player_tag),
                clan_tag=str(clan_tag) if clan_tag else None,
                score=float(score),
                payload=payload if isinstance(payload, dict) else {},
                created_at=created_at,
            )
        )

    data = load_json(RUSH_HISTORY_ENTRIES_FILE)
    if not isinstance(data, dict):
        data = {}
    key = str(player_tag)
    rows = data.get(key, [])
    if not isinstance(rows, list):
        rows = []
    rows.append(
        {
            "player_tag": key,
            "clan_tag": str(clan_tag) if clan_tag else None,
            "score": float(score),
            "payload": payload if isinstance(payload, dict) else {},
            "created_at": created_at,
        }
    )
    data[key] = rows
    return bool(save_json(RUSH_HISTORY_ENTRIES_FILE, data))


def load_rush_history_for_player(player_tag: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Load latest rush score history points for a player, newest first."""
    lim = max(1, int(limit or 1))

    if db_layer is not None and hasattr(db_layer, "load_rush_history_entries"):
        rows = db_layer.load_rush_history_entries(str(player_tag), limit=lim)
        return rows if isinstance(rows, list) else []

    data = load_json(RUSH_HISTORY_ENTRIES_FILE)
    if not isinstance(data, dict):
        return []

    rows = data.get(str(player_tag), [])
    if not isinstance(rows, list):
        return []

    cleaned = [r for r in rows if isinstance(r, dict)]
    cleaned.sort(key=lambda r: str(r.get("created_at", "")), reverse=True)
    return cleaned[:lim]


def load_capital_progress_data() -> Dict[str, Any]:
    return _load_dict_blob(CAPITAL_PROGRESS_FILE)


def save_capital_progress_data(data: Dict[str, Any]) -> bool:
    return _save_dict_blob(CAPITAL_PROGRESS_FILE, data)


def load_monthly_leaderboard() -> Dict[str, Any]:
    return _load_dict_blob(MONTHLY_LEADERBOARD_FILE)


def save_monthly_leaderboard(data: Dict[str, Any]) -> bool:
    return _save_dict_blob(MONTHLY_LEADERBOARD_FILE, data)


def load_leaderboard_snapshot(clan_tag: str, month_key: str) -> Optional[Dict[str, Any]]:
    """Load one clan/month leaderboard snapshot (DB table first, JSON fallback)."""
    if db_layer is not None and hasattr(db_layer, "load_leaderboard_snapshot"):
        row = db_layer.load_leaderboard_snapshot(clan_tag, month_key)
        if isinstance(row, dict):
            return row

    data = load_monthly_leaderboard()
    clan_rows = data.get(clan_tag, {}) if isinstance(data, dict) else {}
    if not isinstance(clan_rows, dict):
        return None
    row = clan_rows.get(month_key)
    return row if isinstance(row, dict) else None


def save_leaderboard_snapshot(clan_tag: str, month_key: str, payload: Dict[str, Any]) -> bool:
    """Persist one clan/month leaderboard snapshot in both legacy and DB stores."""
    if not isinstance(payload, dict):
        return False

    # Keep legacy dataset for backward compatibility with existing consumers.
    data = load_monthly_leaderboard()
    if not isinstance(data, dict):
        data = {}
    data.setdefault(clan_tag, {})
    data[clan_tag][month_key] = payload
    json_ok = save_monthly_leaderboard(data)

    db_ok = True
    if db_layer is not None and hasattr(db_layer, "save_leaderboard_snapshot"):
        db_ok = db_layer.save_leaderboard_snapshot(clan_tag, month_key, payload)

    return bool(json_ok and db_ok)


def load_achievements_data() -> Dict[str, Any]:
    return _load_dict_blob(ACHIEVEMENTS_FILE)


def save_achievements_data(data: Dict[str, Any]) -> bool:
    return _save_dict_blob(ACHIEVEMENTS_FILE, data)


def load_challenges_data() -> Dict[str, Any]:
    return _load_dict_blob(CHALLENGES_FILE)


def save_challenges_data(data: Dict[str, Any]) -> bool:
    return _save_dict_blob(CHALLENGES_FILE, data)


def load_transfers_data() -> Dict[str, Any]:
    if db_layer is not None and hasattr(db_layer, "load_transfer_events"):
        events = db_layer.load_transfer_events(limit=500)
        if isinstance(events, list):
            normalized: List[Dict[str, Any]] = []
            for row in events:
                if not isinstance(row, dict):
                    continue
                payload = row.get("payload")
                if isinstance(payload, dict) and payload.get("player_tag"):
                    normalized.append(payload)
                else:
                    normalized.append(
                        {
                            "timestamp": row.get("created_at"),
                            "player_tag": row.get("player_tag", ""),
                            "from": {"tag": row.get("from_clan_tag", "")},
                            "to": {"tag": row.get("to_clan_tag", "")},
                        }
                    )
            return {"events": normalized}
    return _load_dict_blob(TRANSFERS_FILE)


def save_transfers_data(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False

    json_ok = save_json(TRANSFERS_FILE, data)
    db_ok = True
    if db_layer is not None and hasattr(db_layer, "replace_transfer_events"):
        events = data.get("events", [])
        db_ok = db_layer.replace_transfer_events(events if isinstance(events, list) else [])
    return bool(json_ok and db_ok)


def create_personal_reminder(
    user_id: int,
    message: str,
    due_at_iso: str,
    channel_id: Optional[int] = None,
) -> Optional[int]:
    """Create persistent personal reminder row and return reminder id."""
    payload = {
        "user_id": int(user_id),
        "message": str(message),
        "channel_id": int(channel_id) if channel_id else None,
    }

    if db_layer is not None and hasattr(db_layer, "save_reminder"):
        return db_layer.save_reminder(
            kind="personal_reminder",
            scope_key=str(user_id),
            payload=payload,
            due_at=str(due_at_iso),
        )

    data = load_json(REMINDERS_FILE)
    if not isinstance(data, dict):
        data = {"next_id": 1, "rows": []}
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        rows = []
    next_id = int(data.get("next_id", 1) or 1)
    rows.append(
        {
            "id": next_id,
            "kind": "personal_reminder",
            "scope_key": str(user_id),
            "payload": payload,
            "due_at": str(due_at_iso),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    data["rows"] = rows
    data["next_id"] = next_id + 1
    if save_json(REMINDERS_FILE, data):
        return next_id
    return None


def load_personal_reminders() -> List[Dict[str, Any]]:
    """Load persistent personal reminders."""
    if db_layer is not None and hasattr(db_layer, "load_reminders"):
        rows = db_layer.load_reminders(kind="personal_reminder")
        return rows if isinstance(rows, list) else []

    data = load_json(REMINDERS_FILE)
    if not isinstance(data, dict):
        return []
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and r.get("kind") == "personal_reminder"]


def delete_personal_reminder(reminder_id: int) -> bool:
    """Delete persistent personal reminder by id."""
    if db_layer is not None and hasattr(db_layer, "delete_reminder"):
        return bool(db_layer.delete_reminder(int(reminder_id)))

    data = load_json(REMINDERS_FILE)
    if not isinstance(data, dict):
        return False
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        return False

    before = len(rows)
    rows = [r for r in rows if int(r.get("id", -1) or -1) != int(reminder_id)]
    data["rows"] = rows
    if len(rows) == before:
        return False
    return save_json(REMINDERS_FILE, data)


