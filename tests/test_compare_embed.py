import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from embeds import build_compare_embed  # noqa: E402


def _player(name: str, th: int, trophies: int, donations: int, received: int, war_stars: int, hero_base: int):
    return {
        "name": name,
        "townHallLevel": th,
        "trophies": trophies,
        "donations": donations,
        "donationsReceived": received,
        "warStars": war_stars,
        "heroes": [
            {"name": "Barbarian King", "level": hero_base},
            {"name": "Archer Queen", "level": hero_base},
            {"name": "Grand Warden", "level": max(0, hero_base - 10)},
            {"name": "Royal Champion", "level": max(0, hero_base - 20)},
        ],
        "troops": [{"level": 8}, {"level": 9}],
        "spells": [{"level": 7}],
        "heroEquipment": [{"name": "Rage Gem", "level": 10, "maxLevel": 18}],
        "pets": [{"name": "L.A.S.S.I", "level": 5}],
    }


def test_build_compare_embed_contains_decision_and_fit_sections():
    a = _player("Alpha", th=16, trophies=5600, donations=2500, received=400, war_stars=1200, hero_base=90)
    b = _player("Beta", th=15, trophies=5200, donations=1500, received=900, war_stars=900, hero_base=80)

    emb = build_compare_embed(a, "#AAA", b, "#BBB")

    names = [f.name for f in emb.fields]
    assert "🧭 Decision Summary" in names
    assert "🎯 Best Fit Recommendations" in names

    decision = next(f.value for f in emb.fields if f.name == "🧭 Decision Summary")
    fit = next(f.value for f in emb.fields if f.name == "🎯 Best Fit Recommendations")

    assert "War" in decision or "TH" in decision
    assert "War attacker fit" in fit
    assert "Support donor fit" in fit
    assert "Trophy push fit" in fit
