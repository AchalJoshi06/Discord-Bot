import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _promotion_confidence, _promotion_action_hint  # noqa: E402


def test_promotion_confidence_bands():
    assert _promotion_confidence(90.0) == ("Promote Now", "🟢")
    assert _promotion_confidence(70.0) == ("Review Soon", "🟡")
    assert _promotion_confidence(60.0) == ("Coach First", "🟠")
    assert _promotion_confidence(40.0) == ("Not Ready", "🔴")


def test_promotion_action_hint_uses_blocker_context():
    assert "immediate promotion review" in _promotion_action_hint(85.0, [])
    assert "consistent activity" in _promotion_action_hint(72.0, ["Low activity"])
    assert "hero/lab catch-up" in _promotion_action_hint(60.0, ["High rush score"])
    assert "donation support ratio" in _promotion_action_hint(58.0, ["Low donation ratio"])
    assert "Hold promotion" in _promotion_action_hint(40.0, [])
