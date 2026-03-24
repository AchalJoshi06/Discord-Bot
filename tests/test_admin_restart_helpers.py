import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _resolve_restart_relaunch  # noqa: E402


def test_resolve_restart_relaunch_explicit_modes():
    assert _resolve_restart_relaunch("relaunch") is True
    assert _resolve_restart_relaunch("close") is False


def test_resolve_restart_relaunch_invalid_mode():
    assert _resolve_restart_relaunch("bad-mode") is None


def test_resolve_restart_relaunch_auto_returns_bool():
    out = _resolve_restart_relaunch("auto")
    assert isinstance(out, bool)
