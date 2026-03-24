import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.war import _warmap_pressure_band, _warmap_action_hint  # noqa: E402


def test_warmap_pressure_band_levels():
    assert _warmap_pressure_band(0, 0, 30) == ("Stable", "🟢")
    assert _warmap_pressure_band(3, 6, 30) == ("Watch", "🟡")
    assert _warmap_pressure_band(5, 12, 30) == ("Needs Push", "🟠")
    assert _warmap_pressure_band(12, 10, 30) == ("Critical", "🔴")


def test_warmap_action_hint_paths():
    done = _warmap_action_hint(0, 0, 30)
    mixed = _warmap_action_hint(4, 7, 30)
    unopened = _warmap_action_hint(6, 0, 30)
    cleanup = _warmap_action_hint(0, 8, 30)

    assert "All attacks used" in done
    assert "Prioritize zero-hit members first" in mixed
    assert "have not opened yet" in unopened
    assert "second-hit cleanup remains" in cleanup
