import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.war import _war_performance_band, _war_performance_action_hint  # noqa: E402


def test_war_performance_band_levels():
    assert _war_performance_band(95.0, 2.4, 0) == ("Elite", "🟢")
    assert _war_performance_band(82.0, 2.0, 1) == ("Reliable", "🟡")
    assert _war_performance_band(60.0, 1.5, 1) == ("Developing", "🟠")
    assert _war_performance_band(45.0, 1.0, 2) == ("At Risk", "🔴")


def test_war_performance_action_hint_paths():
    streak_risk = _war_performance_action_hint(80.0, 2.0, 2)
    low_part = _war_performance_action_hint(65.0, 2.0, 0)
    low_stars = _war_performance_action_hint(85.0, 1.6, 0)
    elite = _war_performance_action_hint(95.0, 2.3, 0)

    assert "Stop streak risk" in streak_risk
    assert "Participation is below target" in low_part
    assert "Improve hit value" in low_stars
    assert "Strong performance" in elite
