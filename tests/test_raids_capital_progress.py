import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.raids import build_capital_upgrade_lines, extract_capital_progress  # noqa: E402


def test_extract_capital_progress_from_clan_payload():
    clan_payload = {
        "clanCapital": {
            "capitalHallLevel": 9,
            "districts": [
                {"name": "Barbarian Camp", "districtHallLevel": 5},
                {"name": "Wizard Valley", "districtHallLevel": 4},
            ],
        }
    }

    out = extract_capital_progress(clan_payload)
    assert out["capital_hall_level"] == 9
    assert out["district_levels"]["Barbarian Camp"] == 5
    assert out["district_levels"]["Wizard Valley"] == 4


def test_build_capital_upgrade_lines_detects_only_increases():
    previous = {
        "capital_hall_level": 8,
        "district_levels": {
            "Barbarian Camp": 4,
            "Wizard Valley": 4,
        },
    }
    current = {
        "capital_hall_level": 9,
        "district_levels": {
            "Barbarian Camp": 5,
            "Wizard Valley": 4,
            "Balloon Lagoon": 3,
        },
    }

    lines = build_capital_upgrade_lines(previous, current)
    assert any("Capital Hall" in line and "8" in line and "9" in line for line in lines)
    assert any("Barbarian Camp" in line and "4" in line and "5" in line for line in lines)
    assert any("Balloon Lagoon" in line and "0" in line and "3" in line for line in lines)
    assert not any("Wizard Valley" in line for line in lines)
