"""Tests for Discord Links layout, sorting, and pagination."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _build_discord_links_embed  # noqa: E402


def _sample_clan_large() -> dict:
    return {
        "name": "CC2 Dominion",
        "tag": "#2JJJCCRQR",
        "memberList": [
            {
                "name": f"Player {i:02d}",
                "tag": f"#{i:08X}",
                "townHallLevel": 12 + (i % 6),
            }
            for i in range(45)
        ],
    }


def _sample_links_for_large() -> dict:
    links = {}
    for i in range(10):
        links[f"#{i:08X}"] = str(111111111111111100 + i)
    return links


def test_discord_links_embed_paginates_not_linked_rows():
    clan = _sample_clan_large()
    links = _sample_links_for_large()

    emb, total_pages = _build_discord_links_embed(clan, links, page=0, sort_mode="th_desc")
    assert total_pages == 2
    assert "Page 1/2" in (emb.description or "")

    emb2, total_pages2 = _build_discord_links_embed(clan, links, page=1, sort_mode="th_desc")
    assert total_pages2 == 2
    assert "Page 2/2" in (emb2.description or "")


def test_discord_links_embed_sort_mode_label_changes():
    clan = _sample_clan_large()
    links = _sample_links_for_large()

    emb_desc, _ = _build_discord_links_embed(clan, links, sort_mode="th_desc")
    emb_asc, _ = _build_discord_links_embed(clan, links, sort_mode="th_asc")

    assert "TH High->Low" in (emb_desc.description or "")
    assert "TH Low->High" in (emb_asc.description or "")


def test_discord_links_embed_contains_three_layout_sections():
    clan = _sample_clan_large()
    links = _sample_links_for_large()

    emb, _ = _build_discord_links_embed(clan, links, page=0)
    field_names = [f.name or "" for f in emb.fields]

    assert any("Link Coverage" in n for n in field_names)
    assert any("Players In Server" in n for n in field_names)
    assert any("Players Not In Server" in n for n in field_names)
    assert any("Players Not Linked" in n for n in field_names)
