"""SQLite persistence layer for staged JSON -> DB migration."""
import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional

logger = logging.getLogger("cc2bot.db")

DB_FILE = os.getenv("BOT_DB_FILE", "bot_data.sqlite3")


class DBError(RuntimeError):
    """Raised when a database operation fails."""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create core tables required for staged migration."""
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS clans (
                tag TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                raw_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS member_activity (
                clan_tag TEXT NOT NULL,
                player_tag TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (clan_tag, player_tag)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS json_blobs (
                name TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rush_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_tag TEXT NOT NULL,
                clan_tag TEXT,
                score REAL NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_tag TEXT NOT NULL,
                from_clan_tag TEXT,
                to_clan_tag TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
                clan_tag TEXT NOT NULL,
                month_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (clan_tag, month_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                scope_key TEXT,
                payload_json TEXT NOT NULL,
                due_at TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _utc_now_iso() -> str:
    with _connect() as conn:
        row = conn.execute("SELECT CURRENT_TIMESTAMP AS now_utc").fetchone()
    return str(row["now_utc"]) if row and row["now_utc"] else ""


def load_json_blob(name: str) -> Optional[Any]:
    """Load a single JSON blob by logical name."""
    try:
        init_db()
        with _connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM json_blobs WHERE name = ?",
                (str(name),),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["payload_json"])
    except Exception as e:
        logger.warning("SQLite load_json_blob failed (%s): %s", name, e)
        return None


def save_json_blob(name: str, payload: Any) -> bool:
    """Persist a single JSON blob by logical name."""
    try:
        init_db()
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO json_blobs (name, payload_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(name) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (str(name), json.dumps(payload, ensure_ascii=False)),
            )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_json_blob failed (%s): %s", name, e)
        return False


def load_settings() -> Optional[Dict[str, Any]]:
    """Load settings map from SQLite; returns None when empty."""
    try:
        init_db()
        with _connect() as conn:
            rows = conn.execute("SELECT key, value_json FROM settings").fetchall()
        if not rows:
            return None
        out: Dict[str, Any] = {}
        for row in rows:
            try:
                out[row["key"]] = json.loads(row["value_json"])
            except Exception:
                out[row["key"]] = row["value_json"]
        return out
    except Exception as e:
        logger.warning("SQLite load_settings failed: %s", e)
        return None


def save_settings(settings: Dict[str, Any]) -> bool:
    """Persist settings map to SQLite."""
    try:
        init_db()
        with _connect() as conn:
            conn.execute("DELETE FROM settings")
            for key, val in settings.items():
                conn.execute(
                    "INSERT OR REPLACE INTO settings (key, value_json) VALUES (?, ?)",
                    (str(key), json.dumps(val, ensure_ascii=False)),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_settings failed: %s", e)
        return False


def load_clans() -> Optional[List[Dict[str, str]]]:
    """Load clan list from SQLite; returns None when empty."""
    try:
        init_db()
        with _connect() as conn:
            rows = conn.execute("SELECT raw_json FROM clans ORDER BY name ASC").fetchall()
        if not rows:
            return None
        out: List[Dict[str, str]] = []
        for row in rows:
            parsed = json.loads(row["raw_json"])
            if isinstance(parsed, dict):
                out.append(parsed)
        return out
    except Exception as e:
        logger.warning("SQLite load_clans failed: %s", e)
        return None


def save_clans(clans: List[Dict[str, str]]) -> bool:
    """Persist clan list to SQLite."""
    try:
        init_db()
        with _connect() as conn:
            conn.execute("DELETE FROM clans")
            for clan in clans:
                tag = str(clan.get("tag", "")).upper()
                if tag and not tag.startswith("#"):
                    tag = "#" + tag
                name = str(clan.get("name", "Unnamed"))
                normalized = {"name": name, "tag": tag}
                conn.execute(
                    "INSERT OR REPLACE INTO clans (tag, name, raw_json) VALUES (?, ?, ?)",
                    (tag, name, json.dumps(normalized, ensure_ascii=False)),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_clans failed: %s", e)
        return False


def load_member_activity() -> Optional[Dict[str, Dict[str, Dict[str, Any]]]]:
    """Load activity map from SQLite; returns None when empty."""
    try:
        init_db()
        with _connect() as conn:
            rows = conn.execute(
                "SELECT clan_tag, player_tag, payload_json FROM member_activity"
            ).fetchall()
        if not rows:
            return None

        out: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for row in rows:
            clan_tag = row["clan_tag"]
            player_tag = row["player_tag"]
            payload = json.loads(row["payload_json"])
            out.setdefault(clan_tag, {})[player_tag] = payload if isinstance(payload, dict) else {}
        return out
    except Exception as e:
        logger.warning("SQLite load_member_activity failed: %s", e)
        return None


def save_member_activity(activity: Dict[str, Dict[str, Dict[str, Any]]]) -> bool:
    """Persist activity map to SQLite."""
    try:
        init_db()
        with _connect() as conn:
            conn.execute("DELETE FROM member_activity")
            for clan_tag, players in activity.items():
                if not isinstance(players, dict):
                    continue
                for player_tag, payload in players.items():
                    clean_payload = payload if isinstance(payload, dict) else {}
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO member_activity (clan_tag, player_tag, payload_json)
                        VALUES (?, ?, ?)
                        """,
                        (
                            str(clan_tag),
                            str(player_tag),
                            json.dumps(clean_payload, ensure_ascii=False),
                        ),
                    )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_member_activity failed: %s", e)
        return False


def save_rush_history_entry(
    player_tag: str,
    score: float,
    payload: Dict[str, Any],
    clan_tag: Optional[str] = None,
    created_at: Optional[str] = None,
) -> bool:
    """Insert a rush history row for a player."""
    try:
        init_db()
        ts = created_at or _utc_now_iso()
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO rush_history (player_tag, clan_tag, score, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(player_tag),
                    str(clan_tag) if clan_tag is not None else None,
                    float(score),
                    json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False),
                    ts,
                ),
            )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_rush_history_entry failed: %s", e)
        return False


def load_rush_history_entries(player_tag: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Load latest rush history rows for a player."""
    try:
        init_db()
        lim = max(1, int(limit))
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT player_tag, clan_tag, score, payload_json, created_at
                FROM rush_history
                WHERE player_tag = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (str(player_tag), lim),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
            out.append(
                {
                    "player_tag": row["player_tag"],
                    "clan_tag": row["clan_tag"],
                    "score": float(row["score"]),
                    "payload": payload if isinstance(payload, dict) else {},
                    "created_at": row["created_at"],
                }
            )
        return out
    except Exception as e:
        logger.warning("SQLite load_rush_history_entries failed: %s", e)
        return []


def save_transfer_event(
    player_tag: str,
    payload: Dict[str, Any],
    from_clan_tag: Optional[str] = None,
    to_clan_tag: Optional[str] = None,
    created_at: Optional[str] = None,
) -> bool:
    """Insert a single transfer event row."""
    try:
        init_db()
        ts = created_at or _utc_now_iso()
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO transfers (player_tag, from_clan_tag, to_clan_tag, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(player_tag),
                    str(from_clan_tag) if from_clan_tag is not None else None,
                    str(to_clan_tag) if to_clan_tag is not None else None,
                    json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False),
                    ts,
                ),
            )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_transfer_event failed: %s", e)
        return False


def load_transfer_events(limit: int = 200) -> List[Dict[str, Any]]:
    """Load latest transfer events."""
    try:
        init_db()
        lim = max(1, int(limit))
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT player_tag, from_clan_tag, to_clan_tag, payload_json, created_at
                FROM transfers
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (lim,),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
            out.append(
                {
                    "player_tag": row["player_tag"],
                    "from_clan_tag": row["from_clan_tag"],
                    "to_clan_tag": row["to_clan_tag"],
                    "payload": payload if isinstance(payload, dict) else {},
                    "created_at": row["created_at"],
                }
            )
        return out
    except Exception as e:
        logger.warning("SQLite load_transfer_events failed: %s", e)
        return []


def replace_transfer_events(events: List[Dict[str, Any]]) -> bool:
    """Replace transfer events table contents from an events payload list."""
    try:
        init_db()
        with _connect() as conn:
            conn.execute("DELETE FROM transfers")
            for row in events[-500:]:
                if not isinstance(row, dict):
                    continue
                payload = dict(row)
                ts = str(payload.get("timestamp", "")) or _utc_now_iso()
                from_tag = None
                to_tag = None
                if isinstance(payload.get("from"), dict):
                    from_tag = payload["from"].get("tag")
                if isinstance(payload.get("to"), dict):
                    to_tag = payload["to"].get("tag")
                conn.execute(
                    """
                    INSERT INTO transfers (player_tag, from_clan_tag, to_clan_tag, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(payload.get("player_tag", "")),
                        str(from_tag) if from_tag else None,
                        str(to_tag) if to_tag else None,
                        json.dumps(payload, ensure_ascii=False),
                        ts,
                    ),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite replace_transfer_events failed: %s", e)
        return False


def save_leaderboard_snapshot(clan_tag: str, month_key: str, payload: Dict[str, Any]) -> bool:
    """Upsert a leaderboard snapshot for one clan/month."""
    try:
        init_db()
        ts = _utc_now_iso()
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO leaderboard_snapshots (clan_tag, month_key, payload_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(clan_tag, month_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    str(clan_tag),
                    str(month_key),
                    json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False),
                    ts,
                ),
            )
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite save_leaderboard_snapshot failed: %s", e)
        return False


def load_leaderboard_snapshot(clan_tag: str, month_key: str) -> Optional[Dict[str, Any]]:
    """Load one leaderboard snapshot by clan/month."""
    try:
        init_db()
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json FROM leaderboard_snapshots
                WHERE clan_tag = ? AND month_key = ?
                """,
                (str(clan_tag), str(month_key)),
            ).fetchone()
        if row is None:
            return None
        parsed = json.loads(row["payload_json"])
        return parsed if isinstance(parsed, dict) else {}
    except Exception as e:
        logger.warning("SQLite load_leaderboard_snapshot failed: %s", e)
        return None


def save_reminder(
    kind: str,
    payload: Dict[str, Any],
    scope_key: Optional[str] = None,
    due_at: Optional[str] = None,
    created_at: Optional[str] = None,
) -> Optional[int]:
    """Insert reminder row and return its ID."""
    try:
        init_db()
        ts = created_at or _utc_now_iso()
        with _connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO reminders (kind, scope_key, payload_json, due_at, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(kind),
                    str(scope_key) if scope_key is not None else None,
                    json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False),
                    str(due_at) if due_at is not None else None,
                    ts,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)
    except Exception as e:
        logger.warning("SQLite save_reminder failed: %s", e)
        return None


def load_reminders(kind: Optional[str] = None, scope_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load reminders, optionally filtered by kind/scope."""
    try:
        init_db()
        clauses: List[str] = []
        params: List[Any] = []
        if kind is not None:
            clauses.append("kind = ?")
            params.append(str(kind))
        if scope_key is not None:
            clauses.append("scope_key = ?")
            params.append(str(scope_key))

        where_sql = ""
        if clauses:
            where_sql = " WHERE " + " AND ".join(clauses)

        with _connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, kind, scope_key, payload_json, due_at, created_at
                FROM reminders
                {where_sql}
                ORDER BY created_at ASC, id ASC
                """,
                tuple(params),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
            out.append(
                {
                    "id": int(row["id"]),
                    "kind": row["kind"],
                    "scope_key": row["scope_key"],
                    "payload": payload if isinstance(payload, dict) else {},
                    "due_at": row["due_at"],
                    "created_at": row["created_at"],
                }
            )
        return out
    except Exception as e:
        logger.warning("SQLite load_reminders failed: %s", e)
        return []


def delete_reminder(reminder_id: int) -> bool:
    """Delete reminder row by id."""
    try:
        init_db()
        with _connect() as conn:
            conn.execute("DELETE FROM reminders WHERE id = ?", (int(reminder_id),))
            conn.commit()
        return True
    except Exception as e:
        logger.warning("SQLite delete_reminder failed: %s", e)
        return False
