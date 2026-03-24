import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db  # noqa: E402


def test_transfers_replace_and_load(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    events = [
        {
            "timestamp": "2026-03-18T10:00:00+00:00",
            "player_tag": "#AAA",
            "from": {"tag": "#C1", "name": "Clan 1"},
            "to": {"tag": "#C2", "name": "Clan 2"},
        },
        {
            "timestamp": "2026-03-18T11:00:00+00:00",
            "player_tag": "#BBB",
            "from": {"tag": "#C2", "name": "Clan 2"},
            "to": {"tag": "#C1", "name": "Clan 1"},
        },
    ]

    assert db.replace_transfer_events(events) is True
    loaded = db.load_transfer_events(limit=10)
    assert len(loaded) == 2
    assert loaded[0]["payload"]["player_tag"] == "#BBB"
    assert loaded[1]["payload"]["player_tag"] == "#AAA"


def test_leaderboard_snapshot_upsert_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "members": {
            "#AAA": {"name": "Alice", "donations": 1234},
        }
    }
    assert db.save_leaderboard_snapshot("#PQUCURCQ", "2026-03", payload) is True
    loaded = db.load_leaderboard_snapshot("#PQUCURCQ", "2026-03")
    assert loaded == payload


def test_reminders_create_load_delete(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    rid = db.save_reminder(
        kind="raid",
        scope_key="#PQUCURCQ",
        payload={"channel_id": 123, "message": "Raid starts soon"},
        due_at="2026-03-19T18:00:00+00:00",
    )
    assert isinstance(rid, int)

    all_rows = db.load_reminders()
    assert len(all_rows) == 1
    assert all_rows[0]["kind"] == "raid"

    filtered = db.load_reminders(kind="raid", scope_key="#PQUCURCQ")
    assert len(filtered) == 1
    assert filtered[0]["id"] == rid

    assert db.delete_reminder(rid) is True
    assert db.load_reminders(kind="raid", scope_key="#PQUCURCQ") == []


def test_rush_history_insert_and_load(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert db.save_rush_history_entry(
        player_tag="#AAA",
        clan_tag="#PQUCURCQ",
        score=42.5,
        payload={"town_hall": 14, "rush_score": 42.5},
    ) is True

    rows = db.load_rush_history_entries("#AAA", limit=5)
    assert len(rows) == 1
    assert rows[0]["score"] == 42.5
    assert rows[0]["payload"]["town_hall"] == 14
