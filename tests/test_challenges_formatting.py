import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.challenges import _metric_label, _progress_bar, _days_left_in_week, _next_steps  # noqa: E402
from datetime import datetime, timezone


def test_metric_label_maps_known_and_unknown_values():
    assert _metric_label("donations") == "Donations"
    assert _metric_label("war_stars") == "War Stars"
    assert _metric_label("trophies") == "Trophies"
    assert _metric_label("something_else") == "something_else"


def test_progress_bar_formats_zero_partial_and_full_progress():
    assert _progress_bar(0, 100, width=10) == "░" * 10
    mid = _progress_bar(50, 100, width=10)
    assert mid.count("█") > 0
    assert mid.count("░") > 0
    assert _progress_bar(100, 100, width=10) == "█" * 10


def test_days_left_in_week_is_bounded_and_inclusive():
    monday = datetime(2026, 3, 23, 12, 0, 0, tzinfo=timezone.utc)  # Monday
    sunday = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)  # Sunday

    assert _days_left_in_week(monday) == 7
    assert _days_left_in_week(sunday) == 1


def test_next_steps_guidance_changes_by_metric_and_remaining():
    done_text = _next_steps("donations", 0)
    assert "Goal reached" in done_text

    donation_text = _next_steps("donations", 100)
    war_text = _next_steps("war_stars", 100)
    trophy_text = _next_steps("trophies", 100)

    assert "requests" in donation_text.lower()
    assert "attacks" in war_text.lower()
    assert "trophy" in trophy_text.lower()
