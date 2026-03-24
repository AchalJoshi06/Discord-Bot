"""Read-only data access helpers for the CC2 dashboard."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class DashboardRepository:
    data_dir: Path
    db_file: Path

    def __post_init__(self) -> None:
        self.data_dir = Path(self.data_dir)
        if not self.db_file.is_absolute():
            self.db_file = self.data_dir / self.db_file

    def _connect(self) -> sqlite3.Connection | None:
        if not self.db_file.exists():
            return None
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def normalize_tag(tag: str) -> str:
        value = (tag or "").strip().upper()
        if value and not value.startswith("#"):
            value = f"#{value}"
        return value

    def _load_json_file(self, name: str) -> Any:
        path = self.data_dir / name
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def _load_json_blob(self, name: str) -> Any:
        conn = self._connect()
        if conn is not None:
            try:
                row = conn.execute(
                    "SELECT payload_json FROM json_blobs WHERE name = ?",
                    (name,),
                ).fetchone()
                if row and row["payload_json"]:
                    return json.loads(row["payload_json"])
            except Exception:
                pass
            finally:
                conn.close()
        return self._load_json_file(name)

    def load_clans(self) -> list[dict[str, str]]:
        conn = self._connect()
        if conn is not None:
            try:
                rows = conn.execute("SELECT raw_json FROM clans ORDER BY name ASC").fetchall()
                out: list[dict[str, str]] = []
                for row in rows:
                    parsed = json.loads(row["raw_json"])
                    if isinstance(parsed, dict):
                        tag = self.normalize_tag(str(parsed.get("tag", "")))
                        name = str(parsed.get("name", "Unnamed"))
                        if tag:
                            out.append({"name": name, "tag": tag})
                if out:
                    return out
            except Exception:
                pass
            finally:
                conn.close()

        data = self._load_json_file("clans.json")
        if not isinstance(data, list):
            return []
        out = []
        for row in data:
            if not isinstance(row, dict):
                continue
            tag = self.normalize_tag(str(row.get("tag", "")))
            if not tag:
                continue
            out.append({"name": str(row.get("name", "Unnamed")), "tag": tag})
        return out

    def _load_member_activity(self) -> dict[str, dict[str, dict[str, Any]]]:
        out: dict[str, dict[str, dict[str, Any]]] = {}
        conn = self._connect()
        if conn is not None:
            try:
                rows = conn.execute(
                    "SELECT clan_tag, player_tag, payload_json FROM member_activity"
                ).fetchall()
                for row in rows:
                    clan_tag = self.normalize_tag(str(row["clan_tag"] or ""))
                    player_tag = self.normalize_tag(str(row["player_tag"] or ""))
                    if not clan_tag or not player_tag:
                        continue
                    payload = json.loads(row["payload_json"])
                    if isinstance(payload, dict):
                        out.setdefault(clan_tag, {})[player_tag] = payload
            except Exception:
                pass
            finally:
                conn.close()

        if out:
            return out

        file_data = self._load_json_file("member_activity.json")
        if not isinstance(file_data, dict):
            return {}
        normalized: dict[str, dict[str, dict[str, Any]]] = {}
        for clan_tag, players in file_data.items():
            if not isinstance(players, dict):
                continue
            ctag = self.normalize_tag(str(clan_tag))
            normalized[ctag] = {}
            for player_tag, payload in players.items():
                if isinstance(payload, dict):
                    normalized[ctag][self.normalize_tag(str(player_tag))] = payload
        return normalized

    def _load_latest_rush_scores(self) -> dict[str, float]:
        latest: dict[str, tuple[str, float]] = {}

        conn = self._connect()
        if conn is not None:
            try:
                rows = conn.execute(
                    """
                    SELECT player_tag, score, created_at
                    FROM rush_history
                    ORDER BY created_at DESC, id DESC
                    """
                ).fetchall()
                for row in rows:
                    tag = self.normalize_tag(str(row["player_tag"] or ""))
                    if not tag or tag in latest:
                        continue
                    latest[tag] = (str(row["created_at"] or ""), float(row["score"]))
            except Exception:
                pass
            finally:
                conn.close()

        if latest:
            return {k: v for k, (_, v) in latest.items()}

        file_data = self._load_json_file("rush_history_entries.json")
        if not isinstance(file_data, dict):
            return {}
        out: dict[str, float] = {}
        for player_tag, rows in file_data.items():
            if not isinstance(rows, list) or not rows:
                continue
            newest = sorted(
                [r for r in rows if isinstance(r, dict)],
                key=lambda r: str(r.get("created_at", "")),
                reverse=True,
            )
            if not newest:
                continue
            tag = self.normalize_tag(str(player_tag))
            try:
                out[tag] = float(newest[0].get("score", 0.0))
            except Exception:
                continue
        return out

    def latest_member_rows(self, clan_tag: str) -> list[dict[str, Any]]:
        clan = self.normalize_tag(clan_tag)
        snapshots = self._load_json_blob("donation_snapshots.json")
        clan_snapshots = snapshots.get(clan, []) if isinstance(snapshots, dict) else []
        latest_snapshot = {}
        if isinstance(clan_snapshots, list) and clan_snapshots:
            latest_snapshot = sorted(
                [row for row in clan_snapshots if isinstance(row, dict)],
                key=lambda row: (str(row.get("date", "")), str(row.get("timestamp", ""))),
                reverse=True,
            )[0]

        member_map = latest_snapshot.get("members", {}) if isinstance(latest_snapshot, dict) else {}
        member_activity = self._load_member_activity().get(clan, {})
        rush_scores = self._load_latest_rush_scores()

        rows: list[dict[str, Any]] = []
        for player_tag, payload in member_map.items():
            if not isinstance(payload, dict):
                continue
            tag = self.normalize_tag(str(player_tag))
            activity_payload = member_activity.get(tag, {})
            activity_score = activity_payload.get("activity_score")
            if activity_score is None:
                activity_score = payload.get("activity_score")
            row = {
                "tag": tag,
                "name": str(payload.get("name", tag)),
                "seasonal": int(payload.get("seasonal", 0) or 0),
                "activity_score": float(activity_score) if isinstance(activity_score, (int, float)) else None,
                "rush_score": rush_scores.get(tag),
                "last_seen": activity_payload.get("last_seen"),
            }
            rows.append(row)

        rows.sort(
            key=lambda row: (
                row["activity_score"] is None,
                -(row["activity_score"] or 0.0),
                -row["seasonal"],
            )
        )
        return rows

    def donation_chart(self, clan_tag: str) -> dict[str, list[Any]]:
        clan = self.normalize_tag(clan_tag)
        snapshots = self._load_json_blob("donation_snapshots.json")
        clan_snapshots = snapshots.get(clan, []) if isinstance(snapshots, dict) else []
        if not isinstance(clan_snapshots, list):
            return {"labels": [], "values": []}

        ordered = sorted(
            [row for row in clan_snapshots if isinstance(row, dict)],
            key=lambda row: (str(row.get("date", "")), str(row.get("timestamp", ""))),
        )
        labels: list[str] = []
        values: list[int] = []
        for row in ordered:
            members = row.get("members", {})
            if not isinstance(members, dict):
                continue
            total = 0
            for payload in members.values():
                if isinstance(payload, dict):
                    total += int(payload.get("seasonal", 0) or 0)
            labels.append(str(row.get("date", "snapshot")))
            values.append(total)
        return {"labels": labels, "values": values}

    def war_timeline(self, clan_tag: str) -> dict[str, Any]:
        clan = self.normalize_tag(clan_tag)
        payload = self._load_json_blob("war_results.json")
        wars = payload.get(clan, []) if isinstance(payload, dict) else []
        if not isinstance(wars, list):
            wars = []

        ordered = sorted(
            [row for row in wars if isinstance(row, dict)],
            key=lambda row: str(row.get("timestamp", row.get("date", ""))),
        )
        labels: list[str] = []
        values: list[int] = []
        win = 0
        loss = 0
        tie = 0

        for row in ordered:
            result = str(row.get("result", "")).lower()
            if result.startswith("w"):
                values.append(1)
                win += 1
            elif result.startswith("l"):
                values.append(-1)
                loss += 1
            else:
                values.append(0)
                tie += 1
            labels.append(str(row.get("timestamp") or row.get("date") or f"war {len(labels)+1}"))

        return {
            "labels": labels,
            "values": values,
            "summary": {"win": win, "loss": loss, "tie": tie, "total": len(ordered)},
        }

    def raid_heatmap(self, clan_tag: str) -> dict[str, list[Any]]:
        clan = self.normalize_tag(clan_tag)
        payload = self._load_json_blob("raid_history.json")
        weekends = payload.get(clan, []) if isinstance(payload, dict) else []
        if not isinstance(weekends, list):
            weekends = []

        ordered = sorted(
            [row for row in weekends if isinstance(row, dict)],
            key=lambda row: str(row.get("timestamp", row.get("weekend", row.get("date", "")))),
        )
        labels: list[str] = []
        values: list[int] = []

        for row in ordered:
            label = str(
                row.get("weekend")
                or row.get("date")
                or row.get("timestamp")
                or f"raid {len(labels)+1}"
            )
            completion = self._completion_percent(row)
            labels.append(label)
            values.append(completion)

        return {"labels": labels, "values": values}

    @staticmethod
    def _completion_percent(row: dict[str, Any]) -> int:
        direct = row.get("completion")
        if isinstance(direct, (int, float)):
            return max(0, min(100, int(round(direct))))

        if isinstance(row.get("full_completion"), bool):
            return 100 if row.get("full_completion") else 0

        destroyed = row.get("districts_destroyed")
        total = row.get("districts_total")
        if isinstance(destroyed, (int, float)) and isinstance(total, (int, float)) and total > 0:
            return max(0, min(100, int(round((float(destroyed) / float(total)) * 100))))

        return 0
