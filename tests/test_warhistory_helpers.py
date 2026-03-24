import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.war import _warhistory_momentum_band, _warhistory_action_hint  # noqa: E402


def test_warhistory_momentum_band_levels():
    assert _warhistory_momentum_band(8, 1, 1, 3) == ("Strong Momentum", "🟢")
    assert _warhistory_momentum_band(5, 4, 1, 1) == ("Balanced", "🟡")
    assert _warhistory_momentum_band(4, 6, 0, 0) == ("Unstable", "🟠")
    assert _warhistory_momentum_band(2, 8, 0, 0) == ("Downtrend", "🔴")


def test_warhistory_action_hint_paths():
    strong = _warhistory_action_hint("Strong Momentum", 4, 8, 2)
    balanced = _warhistory_action_hint("Balanced", 1, 5, 4)
    unstable = _warhistory_action_hint("Unstable", 0, 4, 6)
    down = _warhistory_action_hint("Downtrend", 0, 2, 8)

    assert "Keep current war plans consistent" in strong
    assert "Results are mixed" in balanced
    assert "Momentum is shaky" in unstable
    assert "Current downtrend" in down
