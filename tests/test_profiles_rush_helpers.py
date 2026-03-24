import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.profiles import _rush_status_band, _rush_trend_outlook, _rush_action_hint  # noqa: E402


def test_rush_status_band_thresholds():
    assert _rush_status_band(8.0) == ("Very Clean", "🟢")
    assert _rush_status_band(18.0) == ("Stable", "🟡")
    assert _rush_status_band(28.0) == ("At Risk", "🟠")
    assert _rush_status_band(36.0) == ("Rushed", "🔴")


def test_rush_trend_outlook_categories():
    assert _rush_trend_outlook(-6.0) == "Strong improvement trend"
    assert _rush_trend_outlook(-1.2) == "Improving trend"
    assert _rush_trend_outlook(0.2) == "Mostly flat trend"
    assert _rush_trend_outlook(2.5) == "Worsening trend"
    assert _rush_trend_outlook(7.0) == "Sharp deterioration trend"


def test_rush_action_hint_paths():
    clean = _rush_action_hint(9.0, -0.2)
    stable = _rush_action_hint(18.0, -1.0)
    rising = _rush_action_hint(24.0, 1.5)
    rushed = _rush_action_hint(35.0, 0.8)

    assert "Maintain current pace" in clean
    assert "Good direction" in stable
    assert "Trend is rising" in rising
    assert "High rush risk" in rushed
