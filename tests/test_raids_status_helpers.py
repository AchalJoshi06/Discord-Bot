import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.raids import _raid_urgency_band, _raid_action_hint  # noqa: E402


def test_raid_urgency_band_levels():
    assert _raid_urgency_band(97.0, 0) == ("On Track", "🟢")
    assert _raid_urgency_band(84.0, 4) == ("Watch", "🟡")
    assert _raid_urgency_band(65.0, 10) == ("Needs Push", "🟠")
    assert _raid_urgency_band(45.0, 12) == ("Critical", "🔴")


def test_raid_action_hint_paths():
    done = _raid_action_hint(98.0, 0, 0)
    close = _raid_action_hint(88.0, 3, 0)
    no_attacks = _raid_action_hint(72.0, 8, 3)
    low_util = _raid_action_hint(50.0, 12, 0)

    assert "All attacks are complete" in done
    assert "Close to finish" in close
    assert "no-attack members" in no_attacks
    assert "Low utilization risk" in low_util
