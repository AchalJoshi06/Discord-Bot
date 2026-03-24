"""Tests for heroes embed pagination and sorting."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import _build_heroes_weight_embed  # noqa: E402


def _sample_clan_large():
    """Sample clan with 45 members for pagination testing."""
    return {
        "name": "Big Clan",
        "tag": "#BIGCLAN1",
        "clanLevel": 12,
        "members": 45,
        "maxMembers": 50,
        "memberList": [
            {
                "name": f"Member {i:02d}",
                "tag": f"#{i:08X}",
                "townHallLevel": 16 - (i % 5),  # Vary TH levels
                "role": "member",
            }
            for i in range(45)
        ],
    }


def _sample_player_map_large():
    """Player map with hero data for 45 members."""
    return {
        f"#{i:08X}": {
            "townHallLevel": 16 - (i % 5),
            "heroes": [
                {"name": "Barbarian King", "level": 90 - (i % 20)},
                {"name": "Archer Queen", "level": 90 - (i % 20)},
                {"name": "Grand Warden", "level": 70 - (i % 20)},
                {"name": "Royal Champion", "level": 50 - (i % 20)},
                {"name": "Minion Prince", "level": 40 - (i % 20)},
            ],
        }
        for i in range(45)
    }


def test_heroes_embed_pagination_first_page():
    """Test first page shows 20 members."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    emb, total_pages = _build_heroes_weight_embed(clan, players, page=0, sort_mode="power")
    
    assert total_pages == 3, f"Expected 3 pages (45 members / 20 per page), got {total_pages}"
    assert "Page 1/3" in emb.description
    body = "\n".join(f.value for f in emb.fields)
    assert "TH  BK  AQ  GW  RC  MP  NAME" in body


def test_heroes_embed_pagination_middle_page():
    """Test middle page (page 1)."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    emb, total_pages = _build_heroes_weight_embed(clan, players, page=1, sort_mode="power")
    
    assert total_pages == 3
    assert "Page 2/3" in emb.description


def test_heroes_embed_pagination_last_page():
    """Test last page shows remaining 5 members."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    emb, total_pages = _build_heroes_weight_embed(clan, players, page=2, sort_mode="power")
    
    assert total_pages == 3
    assert "Page 3/3" in emb.description


def test_heroes_embed_sort_by_th_descending():
    """Test sorting by TH descending."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    emb, _ = _build_heroes_weight_embed(clan, players, page=0, sort_mode="th_desc")
    
    assert "by TH (High→Low)" in emb.description
    body = "\n".join(f.value for f in emb.fields)
    assert "TH  BK  AQ  GW  RC  MP  NAME" in body


def test_heroes_embed_sort_by_th_ascending():
    """Test sorting by TH ascending."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    emb, _ = _build_heroes_weight_embed(clan, players, page=0, sort_mode="th_asc")
    
    assert "by TH (Low→High)" in emb.description


def test_heroes_embed_th_summary_only_on_first_page():
    """Test that TH summary only appears on first page."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    
    emb_page0, _ = _build_heroes_weight_embed(clan, players, page=0, sort_mode="power")
    emb_page1, _ = _build_heroes_weight_embed(clan, players, page=1, sort_mode="power")
    
    page0_fields = [f.name for f in emb_page0.fields]
    page1_fields = [f.name for f in emb_page1.fields]
    
    assert "By Town Hall" in page0_fields, "First page should have TH summary"
    assert "By Town Hall" not in page1_fields, "Page 2+ should not have TH summary"


def test_heroes_embed_invalid_page_clamps():
    """Test that invalid page numbers are clamped."""
    clan = _sample_clan_large()
    players = _sample_player_map_large()
    
    # Page way out of bounds should clamp to last page
    emb, total_pages = _build_heroes_weight_embed(clan, players, page=999, sort_mode="power")
    assert "Page 3/3" in emb.description
    
    # Negative page should clamp to first page
    emb, total_pages = _build_heroes_weight_embed(clan, players, page=-5, sort_mode="power")
    assert "Page 1/3" in emb.description
