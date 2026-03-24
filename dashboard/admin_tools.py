"""Admin helpers for dashboard actions (kick suggestions, roster export, basebook)."""

from __future__ import annotations

import csv
import io
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
BOT_ROOT = BASE_DIR.parent
if str(BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(BOT_ROOT))

from storage import (  # type: ignore
    load_bases,
    save_bases,
    load_member_activity,
    load_war_player_stats,
)


def user_can_admin(user: dict[str, Any] | None) -> bool:
    if not user:
        return False
    if bool(user.get("is_leadership")):
        return True
    return os.getenv("DASHBOARD_ADMIN_OPEN", "0").strip().lower() in {"1", "true", "yes"}


def build_kick_suggestions(member_rows: list[dict[str, Any]], clan_tag: str) -> list[str]:
    lines: list[str] = []
    activity = load_member_activity()
    war_stats = load_war_player_stats()
    clan_activity = activity.get(clan_tag, {}) if isinstance(activity, dict) else {}
    clan_war_stats = war_stats.get(clan_tag, {}) if isinstance(war_stats, dict) else {}

    for row in member_rows:
        tag = str(row.get("tag", ""))
        reasons: list[str] = []

        rush_score = row.get("rush_score")
        if isinstance(rush_score, (int, float)) and float(rush_score) >= 25.0:
            reasons.append(f"Rushed {float(rush_score):.1f}%")

        activity_score = row.get("activity_score")
        if isinstance(activity_score, (int, float)) and float(activity_score) < 40.0:
            reasons.append(f"Activity {float(activity_score):.1f}/100")

        rec = clan_activity.get(tag, {}) if isinstance(clan_activity, dict) else {}
        last_seen = rec.get("last_seen")
        if isinstance(last_seen, str) and last_seen:
            try:
                last_dt = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                inactive_days = max(0, (datetime.now(timezone.utc) - last_dt).days)
                threshold = int(os.getenv("INACTIVE_DAYS_THRESHOLD", "14") or 14)
                if inactive_days >= threshold:
                    reasons.append(f"Inactive {inactive_days}d")
            except Exception:
                pass

        row_stats = clan_war_stats.get(tag, {}) if isinstance(clan_war_stats, dict) else {}
        if isinstance(row_stats, dict):
            missed_streak = int(row_stats.get("missed_streak", 0) or 0)
            if missed_streak >= 2:
                reasons.append(f"Missed streak {missed_streak} wars")

        if reasons:
            lines.append(f"{row.get('name', tag)} ({tag}) - {', '.join(reasons)}")

    lines.sort()
    return lines


def roster_csv_bytes(member_rows: list[dict[str, Any]], clan_name: str, clan_tag: str) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["clan_name", "clan_tag", "name", "tag", "seasonal_donations", "activity_score", "rush_score", "last_seen"])
    for row in member_rows:
        writer.writerow(
            [
                clan_name,
                clan_tag,
                row.get("name", ""),
                row.get("tag", ""),
                row.get("seasonal", 0),
                f"{float(row['activity_score']):.1f}" if isinstance(row.get("activity_score"), (int, float)) else "",
                f"{float(row['rush_score']):.1f}" if isinstance(row.get("rush_score"), (int, float)) else "",
                row.get("last_seen", ""),
            ]
        )
    return output.getvalue().encode("utf-8")


def get_basebook(player_tag: str) -> dict[str, list[dict[str, Any]]]:
    bases = load_bases()
    row = bases.get(player_tag, {}) if isinstance(bases, dict) else {}
    return row if isinstance(row, dict) else {}


def add_base_entry(player_tag: str, base_type: str, name: str, link: str, actor_id: str) -> None:
    clean_type = str(base_type).strip().lower()
    if clean_type not in {"war", "legend", "anti2", "blizzard"}:
        raise ValueError("Unsupported base type")

    clean_name = str(name).strip()
    clean_link = str(link).strip()
    if not clean_name:
        raise ValueError("Base name is required")
    if not clean_link:
        raise ValueError("Base link is required")

    bases = load_bases()
    if not isinstance(bases, dict):
        bases = {}

    player_row = bases.get(player_tag, {})
    if not isinstance(player_row, dict):
        player_row = {}

    entries = player_row.get(clean_type, [])
    if not isinstance(entries, list):
        entries = []

    entries.append(
        {
            "name": clean_name,
            "link": clean_link,
            "addedBy": str(actor_id),
            "addedAt": datetime.now(timezone.utc).isoformat(),
        }
    )
    player_row[clean_type] = entries
    bases[player_tag] = player_row
    save_bases(bases)
