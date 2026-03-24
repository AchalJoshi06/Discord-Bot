import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.war import _opponent_lineup_action_hint  # noqa: E402


def test_opponent_lineup_action_hint_variants():
    hard = _opponent_lineup_action_hint(0.50, 1.3)
    top_manageable = _opponent_lineup_action_hint(0.50, 1.9)
    breakable = _opponent_lineup_action_hint(0.30, 2.2)
    balanced = _opponent_lineup_action_hint(0.30, 1.6)

    assert hard[0] == "Top-heavy Hard"
    assert "safe 2-star routes" in hard[1]

    assert top_manageable[0] == "Top-heavy Manageable"
    assert "split strongest attackers" in top_manageable[1]

    assert breakable[0] == "Breakable Defense"
    assert "aggressive triple plans" in breakable[1]

    assert balanced[0] == "Balanced Lineup"
    assert "mirror-first planning" in balanced[1]
