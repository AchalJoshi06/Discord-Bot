import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from storage import (  # noqa: E402
    load_member_activity,
    save_member_activity,
    load_settings,
    save_settings,
    load_guild_settings,
    save_guild_settings,
    get_effective_setting,
    load_guild_clans,
    save_guild_clans,
    get_effective_clans,
    load_clans,
    save_clans,
    load_raid_history,
    save_raid_history,
    create_rush_history_entry,
    load_rush_history_for_player,
    load_capital_progress_data,
    save_capital_progress_data,
    load_war_results,
    save_war_results,
    load_war_player_stats,
    save_war_player_stats,
    load_donation_snapshots,
    save_donation_snapshots,
    load_challenges_data,
    save_challenges_data,
    load_war_baseline,
    save_war_baseline,
    load_transfers_data,
    save_transfers_data,
    save_leaderboard_snapshot,
    load_leaderboard_snapshot,
    create_personal_reminder,
    load_personal_reminders,
    delete_personal_reminder,
)


def test_member_activity_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "#PQUCURCQ": {
            "#PLAYER1": {"name": "Alice", "last_seen": "2026-03-18T10:00:00+00:00"},
            "#PLAYER2": {"name": "Bob", "last_seen": "2026-03-18T11:00:00+00:00"},
        }
    }

    assert save_member_activity(payload) is True
    loaded = load_member_activity()
    assert loaded == payload


def test_member_activity_defaults_to_empty_when_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert not os.path.exists("member_activity.json")
    assert load_member_activity() == {}


def test_settings_roundtrip_dual_write(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    payload = {"raid_reminder_enabled": False, "war_reminder_enabled": True}

    assert save_settings(payload) is True
    assert os.path.exists("settings.json")
    assert os.path.exists("bot_data.sqlite3")

    loaded = load_settings()
    assert loaded["raid_reminder_enabled"] is False
    assert loaded["war_reminder_enabled"] is True
    assert loaded["kick_review_day"] == 0


def test_clans_load_from_db_when_json_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    clans = [
        {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
        {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
    ]

    assert save_clans(clans) is True
    assert os.path.exists("clans.json")
    os.remove("clans.json")

    loaded = load_clans()
    loaded_tags = {c["tag"] for c in loaded}
    assert "#PQUCURCQ" in loaded_tags
    assert "#2JJJCCRQR" in loaded_tags


def test_raid_history_loads_from_db_when_json_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    payload = {
        "#PQUCURCQ": {
            "2026-03-16": {
                "members": {
                    "#AAA": {"name": "Alice", "attacks": 6, "limit": 6, "loot": 12345}
                }
            }
        }
    }

    assert save_raid_history(payload) is True
    assert not os.path.exists("raid_history.json")

    loaded = load_raid_history()
    assert loaded == payload


def test_war_results_roundtrip_db_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "#PQUCURCQ": [
            {"date": "2026-03-18", "opponent": "#XXX", "result": "win", "stars": 34}
        ]
    }
    assert save_war_results(payload) is True
    assert not os.path.exists("war_results.json")
    assert load_war_results() == payload


def test_capital_progress_roundtrip_db_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "#PQUCURCQ": {
            "capital_hall_level": 9,
            "district_levels": {
                "Barbarian Camp": 5,
                "Wizard Valley": 4,
            },
            "updated_at": "2026-03-21T12:00:00+00:00",
        }
    }

    assert save_capital_progress_data(payload) is True
    assert os.path.exists("capital_progress.json")
    os.remove("capital_progress.json")

    loaded = load_capital_progress_data()
    assert loaded == payload


def test_war_player_stats_roundtrip_db_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "#PQUCURCQ": {
            "#AAA": {"name": "Alice", "wars": 10, "stars": 28},
        }
    }
    assert save_war_player_stats(payload) is True
    assert not os.path.exists("war_player_stats.json")
    assert load_war_player_stats() == payload


def test_donation_snapshots_roundtrip_db_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "#PQUCURCQ": [
            {
                "date": "2026-03",
                "members": {
                    "#AAA": {"name": "Alice", "seasonal": 1200, "lifetime": {"total_donated": 90000}}
                },
            }
        ]
    }
    assert save_donation_snapshots(payload) is True
    assert not os.path.exists("donation_snapshots.json")
    assert load_donation_snapshots() == payload


def test_guild_settings_roundtrip_and_effective_lookup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert save_settings({"announce_channel_id": 111}) is True
    assert save_guild_settings(12345, {"announce_channel_id": 222, "war_reminder_enabled": False}) is True

    guild_cfg = load_guild_settings(12345)
    assert guild_cfg["announce_channel_id"] == 222
    assert guild_cfg["war_reminder_enabled"] is False

    effective_channel = get_effective_setting("announce_channel_id", guild_id=12345)
    assert effective_channel == 222

    # Falls back to global value when guild override is absent.
    effective_global_fallback = get_effective_setting("announce_channel_id", guild_id=99999)
    assert effective_global_fallback == 111


def test_guild_clans_roundtrip_and_effective_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    global_clans = [{"name": "Global Clan", "tag": "#AAA111"}]
    guild_clans = [{"name": "Guild Clan", "tag": "#BBB222"}]

    assert save_clans(global_clans) is True
    assert save_guild_clans(555, guild_clans) is True

    loaded_guild = load_guild_clans(555)
    assert loaded_guild == guild_clans

    effective_for_guild = get_effective_clans(555, global_clans)
    assert effective_for_guild == guild_clans

    effective_without_override = get_effective_clans(999, global_clans)
    assert effective_without_override == global_clans


def test_challenges_load_from_db_when_json_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "2026-W12": {
            "id": "donation_push",
            "type": "donations",
            "goal": 10000,
            "posted": True,
        }
    }

    assert save_challenges_data(payload) is True
    assert os.path.exists("challenges.json")
    os.remove("challenges.json")

    loaded = load_challenges_data()
    assert loaded == payload


def test_war_baseline_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    clan_tag = "#PQUCURCQ"
    payload = {
        "#AAA111": [{"stars": 3, "destructionPercentage": 100}],
        "#BBB222": [],
    }

    assert save_war_baseline(clan_tag, payload) is True
    loaded = load_war_baseline(clan_tag)
    assert loaded == payload


def test_transfers_load_from_db_when_json_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "events": [
            {
                "timestamp": "2026-03-18T12:00:00+00:00",
                "player_tag": "#AAA",
                "from": {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
                "to": {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
            }
        ]
    }

    assert save_transfers_data(payload) is True
    assert os.path.exists("transfers.json")
    os.remove("transfers.json")

    loaded = load_transfers_data()
    assert isinstance(loaded.get("events"), list)
    assert loaded["events"][0]["player_tag"] == "#AAA"


def test_leaderboard_snapshot_roundtrip_db_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    payload = {
        "members": {
            "#AAA": {"name": "Alice", "donations": 1500, "war_stars": 20, "activity_score": 77.5}
        }
    }

    assert save_leaderboard_snapshot("#PQUCURCQ", "2026-03", payload) is True
    loaded = load_leaderboard_snapshot("#PQUCURCQ", "2026-03")
    assert loaded == payload


def test_personal_reminder_crud_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    rid = create_personal_reminder(
        user_id=123456,
        message="Upgrade BK",
        due_at_iso="2026-03-19T18:00:00+00:00",
        channel_id=999,
    )
    assert isinstance(rid, int)

    rows = load_personal_reminders()
    assert len(rows) >= 1
    row = next((r for r in rows if int(r.get("id", 0) or 0) == rid), None)
    assert row is not None
    payload = row.get("payload", {})
    assert payload.get("user_id") == 123456
    assert payload.get("message") == "Upgrade BK"

    assert delete_personal_reminder(rid) is True
    rows_after = load_personal_reminders()
    assert all(int(r.get("id", 0) or 0) != rid for r in rows_after)


def test_rush_history_roundtrip_db_first(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    assert create_rush_history_entry(
        player_tag="#AAA",
        clan_tag="#PQUCURCQ",
        score=12.5,
        payload={"name": "Alice", "town_hall": 14},
        created_at_iso="2026-03-18T10:00:00+00:00",
    ) is True
    assert create_rush_history_entry(
        player_tag="#AAA",
        clan_tag="#PQUCURCQ",
        score=10.0,
        payload={"name": "Alice", "town_hall": 14},
        created_at_iso="2026-03-19T10:00:00+00:00",
    ) is True

    rows = load_rush_history_for_player("#AAA", limit=5)
    assert len(rows) == 2
    assert float(rows[0].get("score", 0.0)) == 10.0
    assert float(rows[1].get("score", 0.0)) == 12.5
