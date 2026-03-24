import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _inactive_severity, _inactive_action_hint  # noqa: E402


def test_inactive_severity_watch_high_critical():
    threshold = 7

    assert _inactive_severity(7, threshold) == ("Watch", "🟡")
    assert _inactive_severity(14, threshold) == ("High", "🟠")
    assert _inactive_severity(21, threshold) == ("Critical", "🔴")


def test_inactive_action_hint_no_flagged():
    out = _inactive_action_hint(0, 50, 7)
    assert "No urgent action needed" in out


def test_inactive_action_hint_low_moderate_high_load():
    low = _inactive_action_hint(4, 50, 7)
    moderate = _inactive_action_hint(12, 50, 7)
    high = _inactive_action_hint(25, 50, 7)

    assert "Low inactivity load" in low
    assert "Moderate inactivity load" in moderate
    assert "High inactivity load" in high
    assert "14+ days" in high
