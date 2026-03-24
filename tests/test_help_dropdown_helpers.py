import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _build_help_embed, _HELP_SECTIONS  # noqa: E402


def test_help_sections_include_expected_groups():
    assert "quick_start" in _HELP_SECTIONS
    assert "war" in _HELP_SECTIONS
    assert "raid" in _HELP_SECTIONS
    assert "leadership" in _HELP_SECTIONS


def test_build_help_embed_uses_selected_section():
    emb = _build_help_embed("war")
    assert "War Commands" in (emb.title or "")
    assert emb.fields
    assert "warhistory" in emb.fields[0].value


def test_build_help_embed_falls_back_to_quick_start():
    emb = _build_help_embed("missing_key")
    assert "Quick Start" in (emb.title or "")
