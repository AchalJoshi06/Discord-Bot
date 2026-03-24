import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from calculations import (  # noqa: E402
    extract_hero_levels,
    calculate_donation_ratio_score,
    calculate_activity_score,
    calculate_weighted_rush_score,
    calculate_base_rush,
    extract_equipment_offenders,
    calculate_clan_health_score,
    calculate_player_streaks,
    estimate_progression_speed,
)
from storage import save_war_player_stats, save_raid_history  # noqa: E402


def test_extract_hero_levels_reads_known_heroes():
    player = {
        "heroes": [
            {"name": "Barbarian King", "level": 70},
            {"name": "Archer Queen", "level": 72},
            {"name": "Grand Warden", "level": 50},
            {"name": "Royal Champion", "level": 28},
            {"name": "Minion Prince", "level": 30},
        ]
    }
    levels = extract_hero_levels(player)
    assert levels["BK"] == 70
    assert levels["AQ"] == 72
    assert levels["GW"] == 50
    assert levels["RC"] == 28
    assert levels["MP"] == 30


def test_calculate_donation_ratio_score_caps_at_100():
    assert calculate_donation_ratio_score(500, 100) == 100.0
    assert calculate_donation_ratio_score(50, 100) == 50.0
    assert calculate_donation_ratio_score(0, 100) == 0.0


def test_calculate_activity_score_returns_expected_shape():
    player = {
        "donations": 80,
        "donationsReceived": 100,
        "warStars": 250,
    }
    out = calculate_activity_score(player, war_attack_rate_pct=75.0, raid_completion_rate_pct=60.0)
    assert 0.0 <= out["score"] <= 100.0
    assert out["war_attack_rate"] == 75.0
    assert out["raid_completion_rate"] == 60.0
    assert 0.0 <= out["donation_ratio_score"] <= 100.0


def test_calculate_activity_score_uses_fallbacks_when_missing_rates():
    player = {
        "donations": 100,
        "donationsReceived": 0,
        "warStars": 100,
    }
    out = calculate_activity_score(player)
    assert 0.0 <= out["score"] <= 100.0
    assert out["donation_ratio_score"] == 100.0


def test_weighted_rush_score_returns_none_for_low_th():
    player = {"townHallLevel": 6}
    assert calculate_weighted_rush_score(player) is None


def test_weighted_rush_score_returns_dict_for_valid_th():
    player = {
        "townHallLevel": 12,
        "heroes": [
            {"name": "Barbarian King", "level": 40},
            {"name": "Archer Queen", "level": 40},
            {"name": "Grand Warden", "level": 20},
            {"name": "Royal Champion", "level": 0},
        ],
        "troops": [{"level": 5}, {"level": 6}],
        "spells": [{"level": 5}],
        "heroEquipment": [{"name": "EQ1", "level": 10, "maxLevel": 20}],
        "pets": [],
    }
    out = calculate_weighted_rush_score(player)
    assert isinstance(out, dict)
    assert "score" in out
    assert "is_rushed" in out
    assert "hero_gap" in out


def test_extract_equipment_offenders_returns_top_gaps_desc():
    player = {
        "heroEquipment": [
            {"name": "Giant Arrow", "level": 3, "maxLevel": 18},   # gap 15
            {"name": "Rage Gem", "level": 10, "maxLevel": 18},     # gap 8
            {"name": "Frozen Arrow", "level": 16, "maxLevel": 18}, # gap 2
            {"name": "Maxed Item", "level": 18, "maxLevel": 18},   # gap 0
        ]
    }

    out = extract_equipment_offenders(player, top_n=3)
    assert len(out) == 3
    assert out[0]["name"] == "Giant Arrow"
    assert out[0]["gap"] == 15
    assert out[1]["name"] == "Rage Gem"
    assert out[2]["name"] == "Frozen Arrow"


def test_calculate_clan_health_score_returns_weighted_summary():
    players = [
        {
            "townHallLevel": 15,
            "donations": 300,
            "donationsReceived": 200,
            "warStars": 350,
            "heroes": [
                {"name": "Barbarian King", "level": 95},
                {"name": "Archer Queen", "level": 95},
                {"name": "Grand Warden", "level": 70},
                {"name": "Royal Champion", "level": 45},
            ],
            "troops": [{"level": 10}],
            "spells": [{"level": 9}],
            "heroEquipment": [{"name": "Giant Arrow", "level": 18, "maxLevel": 18}],
            "pets": [{"name": "L.A.S.S.I", "level": 10}],
        },
        {
            "townHallLevel": 13,
            "donations": 100,
            "donationsReceived": 300,
            "warStars": 80,
            "heroes": [
                {"name": "Barbarian King", "level": 45},
                {"name": "Archer Queen", "level": 50},
                {"name": "Grand Warden", "level": 25},
            ],
            "troops": [{"level": 5}],
            "spells": [{"level": 5}],
            "heroEquipment": [{"name": "Rage Gem", "level": 6, "maxLevel": 18}],
            "pets": [],
        },
    ]

    out = calculate_clan_health_score(players, war_win_rate_pct=60.0, raid_completion_rate_pct=72.0)
    assert 0.0 <= out["score"] <= 100.0
    assert out["tier"] in {"Strong", "Stable", "At Risk", "Critical"}
    assert out["member_count"] == 2
    assert out["war_win_rate"] == 60.0
    assert out["raid_completion_rate"] == 72.0


def test_calculate_player_streaks_reads_war_and_raid_sources(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    war_stats = {
        "#PQUCURCQ": {
            "#AAA": {
                "name": "Alice",
                "wars_participated": 12,
                "missed_streak": 0,
                "participation_streak": 7,
            }
        }
    }
    raid_history = {
        "#PQUCURCQ": {
            "2026-03-01": {
                "members": {
                    "#AAA": {"attacks": 6, "limit": 6}
                }
            },
            "2026-03-08": {
                "members": {
                    "#AAA": {"attacks": 6, "limit": 6}
                }
            },
            "2026-03-15": {
                "members": {
                    "#AAA": {"attacks": 5, "limit": 6}
                }
            },
        }
    }

    assert save_war_player_stats(war_stats) is True
    assert save_raid_history(raid_history) is True

    out = calculate_player_streaks("#AAA", clan_tags=["#PQUCURCQ"])
    assert out["war_participation_streak"] == 7
    assert out["raid_full_streak"] == 0


def test_estimate_progression_speed_returns_recent_timeline_lines():
    player = {
        "townHallLevel": 14,
        "warStars": 1200,
        "heroes": [
            {"name": "Barbarian King", "level": 70},
            {"name": "Archer Queen", "level": 75},
            {"name": "Grand Warden", "level": 50},
            {"name": "Royal Champion", "level": 30},
        ],
        "troops": [{"level": 8}, {"level": 9}],
        "spells": [{"level": 7}, {"level": 6}],
        "achievements": [
            {"name": "Friend in Need", "value": 650000},
        ],
    }

    out = estimate_progression_speed(player)
    assert out["available"] is True
    assert out["current_th"] == 14
    assert isinstance(out["estimated_total_months"], int)
    assert len(out["timeline_lines"]) <= 5
    assert out["timeline_lines"][-1].startswith("TH14")


def test_estimate_progression_speed_increases_with_higher_lifetime_proxies():
    base_player = {
        "townHallLevel": 13,
        "warStars": 400,
        "heroes": [
            {"name": "Barbarian King", "level": 45},
            {"name": "Archer Queen", "level": 50},
            {"name": "Grand Warden", "level": 30},
            {"name": "Royal Champion", "level": 15},
        ],
        "troops": [{"level": 7}],
        "spells": [{"level": 6}],
        "achievements": [{"name": "Friend in Need", "value": 120000}],
    }

    rich_proxy_player = {
        **base_player,
        "warStars": 2200,
        "achievements": [{"name": "Friend in Need", "value": 1700000}],
    }

    low = estimate_progression_speed(base_player)
    high = estimate_progression_speed(rich_proxy_player)
    assert high["estimated_total_months"] > low["estimated_total_months"]


def test_calculate_base_rush_uses_base_total_levels_when_available():
    player = {
        "townHallLevel": 13,
        "baseTotalLevels": 1800,
    }
    out = calculate_base_rush(player)
    assert out["counted"] is True
    assert out["status"] in {"OK", "Semi-Rushed", "Rushed"}
    assert out["required"] > 0
    assert out["missing"] >= 0


def test_calculate_base_rush_returns_not_counted_without_building_data():
    player = {
        "townHallLevel": 13,
        "heroes": [{"name": "Barbarian King", "level": 50}],
    }
    out = calculate_base_rush(player)
    assert out["counted"] is False
    assert out["status"] == "N/A"
