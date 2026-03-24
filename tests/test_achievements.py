import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.achievements import _evaluate_custom_badges, _milestone_state, _format_milestone_progress  # noqa: E402


def test_evaluate_custom_badges_mixed_threshold_rules():
    custom_defs = [
        {"name": "CC2 Loyal", "metric": "war_participated", "threshold": 90},
        {"name": "Donate Beast", "metric": "donations_total", "threshold": 10000},
        {"name": "Ultra Clean", "metric": "rush_score_max", "threshold": 5},
    ]
    metrics = {
        "war_participated": 95,
        "donations_total": 12000,
        "rush_score_max": 4.5,
    }

    out = _evaluate_custom_badges(custom_defs, metrics)
    assert "CC2 Loyal" in out
    assert "Donate Beast" in out
    assert "Ultra Clean" in out


def test_evaluate_custom_badges_skips_unmet_rules():
    custom_defs = [
        {"name": "Top Trophy", "metric": "best_trophies", "threshold": 6000},
        {"name": "No Rush", "metric": "rush_score_max", "threshold": 3},
    ]
    metrics = {
        "best_trophies": 5400,
        "rush_score_max": 7,
    }

    out = _evaluate_custom_badges(custom_defs, metrics)
    assert out == []


def test_milestone_state_between_targets():
    state = _milestone_state(3200, [1000, 5000, 10000], width=10)

    assert state["next"] == 5000
    assert state["remaining"] == 1800
    assert state["reached"] == 1
    assert state["total"] == 3
    assert state["pct"] == 55.0
    assert len(state["bar"]) == 10


def test_milestone_state_marks_top_complete():
    state = _milestone_state(7500, [1000, 5000, 6000], width=8)

    assert state["next"] is None
    assert state["remaining"] == 0
    assert state["reached"] == 3
    assert state["total"] == 3
    assert state["pct"] == 100.0
    assert state["bar"] == "█" * 8


def test_format_milestone_progress_contains_gap_and_reached_lines():
    text = _format_milestone_progress(1200, [1000, 5000, 10000], width=10)
    assert "Current: **1,200**" in text
    assert "Next: **5,000** (3,800 to go)" in text
    assert "Milestones reached: **1/3**" in text


def test_format_milestone_progress_shows_top_reached_message():
    text = _format_milestone_progress(6500, [1000, 5000, 6000], width=10)
    assert "Top milestone reached (6,000)" in text
    assert "Milestones reached: **3/3**" in text
