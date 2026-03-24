"""Migrate selected JSON datasets into SQLite tables.

Current scope:
- settings.json
- clans.json
- member_activity.json
- donation_snapshots.json
- war_results.json
- war_player_stats.json
- raid_history.json
- monthly_leaderboard.json
- achievements.json
- challenges.json
- transfers.json

Usage:
    python migrate_json_to_sqlite.py
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import db


BLOB_DATASETS = [
    "donation_snapshots.json",
    "war_results.json",
    "war_player_stats.json",
    "raid_history.json",
    "monthly_leaderboard.json",
    "achievements.json",
    "challenges.json",
    "transfers.json",
]

TABLE_DATASETS = [
    "transfers_table",
    "leaderboard_snapshots_table",
    "reminders_table",
    "rush_history_table",
]

SPECIAL_HANDLED_FILES = {
    "settings.json",
    "clans.json",
    "member_activity.json",
}


def _db_has_migrated_core() -> bool:
    """Return True when at least one core dataset is already in SQLite."""
    if db.load_settings():
        return True
    if db.load_clans():
        return True
    if db.load_member_activity():
        return True
    for name in BLOB_DATASETS:
        if db.load_json_blob(name) is not None:
            return True
    if db.load_transfer_events(limit=1):
        return True
    return False


def _load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _discover_json_files() -> List[str]:
    """Return all JSON files in cwd except special table-backed datasets."""
    out: List[str] = []
    for p in sorted(Path(".").glob("*.json")):
        name = p.name
        if name in SPECIAL_HANDLED_FILES:
            continue
        out.append(name)
    return out


def _safe_db_backup() -> str | None:
    """Create a timestamped DB backup before migration for rollback safety."""
    src = Path(db.DB_FILE)
    if not src.exists():
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dst = src.with_name(f"{src.stem}.pre_migration.{ts}{src.suffix}")
    shutil.copy2(src, dst)
    return str(dst)


def migrate_settings() -> bool:
    data = _load_json("settings.json")
    if not isinstance(data, dict):
        return False
    return db.save_settings(data)


def migrate_clans() -> bool:
    data = _load_json("clans.json")
    if not isinstance(data, list):
        return False
    clans: List[Dict[str, str]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        clans.append({"name": str(row.get("name", "Unnamed")), "tag": str(row.get("tag", ""))})
    if not clans:
        return False
    return db.save_clans(clans)


def migrate_member_activity() -> bool:
    data = _load_json("member_activity.json")
    if not isinstance(data, dict):
        return False
    normalized: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for clan_tag, players in data.items():
        if not isinstance(players, dict):
            continue
        normalized[str(clan_tag)] = {}
        for player_tag, payload in players.items():
            normalized[str(clan_tag)][str(player_tag)] = payload if isinstance(payload, dict) else {}
    if not normalized:
        return False
    return db.save_member_activity(normalized)


def migrate_blob_json(filename: str) -> bool:
    data = _load_json(filename)
    if data is None:
        return False
    return db.save_json_blob(filename, data)


def migrate_transfers_table() -> bool:
    """Migrate transfers.json events into normalized transfers table."""
    data = _load_json("transfers.json")
    if not isinstance(data, dict):
        return False
    events = data.get("events")
    if not isinstance(events, list):
        return False
    return db.replace_transfer_events(events)


def migrate_leaderboard_snapshots_table() -> bool:
    """Migrate monthly_leaderboard.json into leaderboard_snapshots table."""
    data = _load_json("monthly_leaderboard.json")
    if not isinstance(data, dict):
        return False

    wrote_any = False
    for clan_tag, month_map in data.items():
        if not isinstance(month_map, dict):
            continue
        for month_key, payload in month_map.items():
            if not isinstance(payload, dict):
                continue
            ok = db.save_leaderboard_snapshot(str(clan_tag), str(month_key), payload)
            wrote_any = wrote_any or ok
    return wrote_any


def migrate_reminders_table() -> bool:
    """Migrate reminders.json fallback rows into reminders table."""
    data = _load_json("reminders.json")
    if not isinstance(data, dict):
        return False

    rows = data.get("rows")
    if not isinstance(rows, list):
        return False

    wrote_any = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        rid = db.save_reminder(
            kind=str(row.get("kind", "personal_reminder")),
            scope_key=str(row.get("scope_key")) if row.get("scope_key") is not None else None,
            payload=row.get("payload") if isinstance(row.get("payload"), dict) else {},
            due_at=str(row.get("due_at")) if row.get("due_at") is not None else None,
            created_at=str(row.get("created_at")) if row.get("created_at") is not None else None,
        )
        wrote_any = wrote_any or (rid is not None)
    return wrote_any


def migrate_rush_history_table() -> bool:
    """Migrate rush_history_entries.json fallback rows into rush_history table."""
    data = _load_json("rush_history_entries.json")
    if not isinstance(data, dict):
        return False

    wrote_any = False
    for player_tag, rows in data.items():
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            ok = db.save_rush_history_entry(
                player_tag=str(row.get("player_tag") or player_tag),
                clan_tag=str(row.get("clan_tag")) if row.get("clan_tag") is not None else None,
                score=float(row.get("score", 0.0) or 0.0),
                payload=row.get("payload") if isinstance(row.get("payload"), dict) else {},
                created_at=str(row.get("created_at")) if row.get("created_at") is not None else None,
            )
            wrote_any = wrote_any or ok
    return wrote_any


def migrate_if_needed(force: bool = False) -> Dict[str, bool]:
    """Migrate JSON data to SQLite if DB doesn't appear initialized.

    Returns a result map keyed by dataset name.
    """
    db.init_db()
    if not force and _db_has_migrated_core():
        return {}

    _safe_db_backup()

    # Merge declared and discovered JSON files so slipped datasets are captured.
    blob_files = sorted(set(BLOB_DATASETS + _discover_json_files()))

    results = {
        "settings": migrate_settings(),
        "clans": migrate_clans(),
        "member_activity": migrate_member_activity(),
    }
    for name in blob_files:
        key = name.replace(".json", "")
        results[key] = migrate_blob_json(name)

    # Structured table migrations for datasets with dedicated relational tables.
    results["transfers_table"] = migrate_transfers_table()
    results["leaderboard_snapshots_table"] = migrate_leaderboard_snapshots_table()
    results["reminders_table"] = migrate_reminders_table()
    results["rush_history_table"] = migrate_rush_history_table()

    return results


def main() -> int:
    results = migrate_if_needed(force=True)

    print("Migration summary:")
    for key, ok in results.items():
        status = "migrated" if ok else "skipped"
        print(f"- {key}: {status}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
