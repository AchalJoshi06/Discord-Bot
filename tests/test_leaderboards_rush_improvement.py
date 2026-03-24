import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.leaderboards import LeaderboardsCog  # noqa: E402


def test_compute_rush_improvement_two_points_in_month():
    rows = [
        {"score": 14.0, "created_at": "2026-03-02T00:00:00+00:00"},
        {"score": 11.5, "created_at": "2026-03-18T00:00:00+00:00"},
    ]
    out = LeaderboardsCog._compute_rush_improvement_from_rows(rows, "2026-03")
    assert out == 2.5


def test_compute_rush_improvement_uses_prior_baseline_when_single_point_in_month():
    rows = [
        {"score": 18.0, "created_at": "2026-02-27T00:00:00+00:00"},
        {"score": 16.25, "created_at": "2026-03-05T00:00:00+00:00"},
    ]
    out = LeaderboardsCog._compute_rush_improvement_from_rows(rows, "2026-03")
    assert out == 1.75


def test_compute_rush_improvement_negative_when_worsened():
    rows = [
        {"score": 10.0, "created_at": "2026-03-01T00:00:00+00:00"},
        {"score": 12.0, "created_at": "2026-03-20T00:00:00+00:00"},
    ]
    out = LeaderboardsCog._compute_rush_improvement_from_rows(rows, "2026-03")
    assert out == -2.0
