import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.war import _warpreview_pressure_band, _warpreview_action_hint  # noqa: E402


def test_warpreview_pressure_band_levels():
    assert _warpreview_pressure_band("High", 15.2) == ("High Pressure", "🔴")
    assert _warpreview_pressure_band("Medium", 14.1) == ("Balanced Pressure", "🟡")
    assert _warpreview_pressure_band("Low", 13.2) == ("Favorable", "🟢")


def test_warpreview_action_hint_paths():
    high_top_heavy = _warpreview_action_hint("High Pressure", 0.45)
    high_not_top_heavy = _warpreview_action_hint("High Pressure", 0.30)
    balanced = _warpreview_action_hint("Balanced Pressure", 0.33)
    favorable = _warpreview_action_hint("Favorable", 0.20)

    assert "safe 2-star plans" in high_top_heavy
    assert "disciplined mirror" in high_not_top_heavy
    assert "mirror-first openers" in balanced
    assert "aggressive triple plans" in favorable
