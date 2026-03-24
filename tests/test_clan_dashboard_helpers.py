import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.admin import (  # noqa: E402
    _build_clan_overview_embed,
    _build_discord_links_embed,
    _build_heroes_weight_embed,
    _build_last_joining_embed,
)


def _sample_clan_data() -> dict:
    return {
        "name": "CC2 Dominion",
        "tag": "#2JJJCCRQR",
        "clanLevel": 12,
        "members": 3,
        "maxMembers": 50,
        "clanPoints": 59350,
        "versusPoints": 29291,
        "clanCapitalPoints": 1234,
        "warWins": 68,
        "warLosses": 25,
        "warTies": 13,
        "warWinStreak": 3,
        "warFrequency": "always",
        "isWarLogPublic": True,
        "warLeague": {"name": "Crystal League II"},
        "memberList": [
            {
                "name": "ice",
                "tag": "#AAA",
                "townHallLevel": 16,
                "role": "leader",
                "trophies": 5200,
                "attackWins": 111,
                "defenseWins": 15,
                "donations": 100,
                "donationsReceived": 40,
            },
            {
                "name": "Nova",
                "tag": "#BBB",
                "townHallLevel": 15,
                "role": "coLeader",
                "trophies": 5000,
                "attackWins": 95,
                "defenseWins": 12,
                "donations": 90,
                "donationsReceived": 50,
            },
            {
                "name": "Echo",
                "tag": "#CCC",
                "townHallLevel": 14,
                "role": "member",
                "trophies": 4800,
                "attackWins": 80,
                "defenseWins": 9,
                "donations": 10,
                "donationsReceived": 5,
            },
        ],
    }


def test_build_clan_overview_embed_contains_core_sections():
    emb = _build_clan_overview_embed(_sample_clan_data())
    assert "CC2 Dominion" in (emb.title or "")
    assert any((f.name or "") == "Core Stats" for f in emb.fields)
    assert any((f.name or "") == "War & League" for f in emb.fields)
    assert any((f.name or "") == "Town Halls" for f in emb.fields)


def test_build_discord_links_embed_counts_link_coverage():
    links = {
        "#AAA": "111111111111111111",
        "#CCC": "222222222222222222",
    }
    emb, total_pages = _build_discord_links_embed(_sample_clan_data(), links)
    assert "Discord Links" in (emb.title or "")
    assert emb.fields
    assert "Linked: 2" in emb.fields[0].value
    assert "Unlinked: 1" in emb.fields[0].value
    field_names = [f.name for f in emb.fields]
    assert any("Players In Server" in (n or "") for n in field_names)
    assert any("Players Not In Server" in (n or "") for n in field_names)
    assert any("Players Not Linked" in (n or "") for n in field_names)
    assert total_pages == 1


def test_build_last_joining_embed_shows_latest_join_for_member():
    events = [
        {
            "timestamp": "2026-03-20T10:00:00+00:00",
            "player_tag": "#AAA",
            "to": {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
            "from": {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
        },
        {
            "timestamp": "2026-03-22T10:00:00+00:00",
            "player_tag": "#AAA",
            "to": {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
            "from": {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
        },
    ]
    emb = _build_last_joining_embed(_sample_clan_data(), events)
    assert "Last Joining Date" in (emb.title or "")
    assert emb.fields
    body = "\n".join(f.value for f in emb.fields)
    assert "TH  IN  OUT  NAME" in body
    assert "ice" in body
    assert "  2" in body


def test_build_heroes_weight_embed_includes_per_hero_levels():
    player_map = {
        "#AAA": {
            "townHallLevel": 16,
            "heroes": [
                {"name": "Barbarian King", "level": 95},
                {"name": "Archer Queen", "level": 95},
                {"name": "Grand Warden", "level": 70},
                {"name": "Royal Champion", "level": 45},
                {"name": "Minion Prince", "level": 40},
            ],
        }
    }
    emb, total_pages = _build_heroes_weight_embed(_sample_clan_data(), player_map)
    assert "Heroes/War Weight" in (emb.title or "")
    assert emb.fields
    body = "\n".join(f.value for f in emb.fields)
    assert "TH  BK  AQ  GW  RC  MP  NAME" in body
    assert "95" in body
    assert "70" in body
    assert "45" in body
    assert "40" in body
    assert "ice" in body
    assert total_pages == 1
