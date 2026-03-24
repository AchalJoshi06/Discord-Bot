import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.war import _war_pending_urgency, _war_pending_action_hint  # noqa: E402


def test_war_pending_urgency_levels():
    assert _war_pending_urgency(0, 30) == ("On Track", "🟢")
    assert _war_pending_urgency(4, 30) == ("Watch", "🟡")
    assert _war_pending_urgency(10, 30) == ("Needs Push", "🟠")
    assert _war_pending_urgency(16, 30) == ("Critical", "🔴")


def test_war_pending_action_hint_paths():
    done = _war_pending_action_hint(0, 30)
    light = _war_pending_action_hint(4, 30)
    moderate = _war_pending_action_hint(10, 30)
    critical = _war_pending_action_hint(16, 30)

    assert "All attacks used" in done
    assert "Small pending list" in light
    assert "Moderate pending load" in moderate
    assert "High pending risk" in critical
