import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cogs.upgrades import UpgradesCog  # noqa: E402


class _DummyBot:
    def __init__(self):
        self._clans = [
            {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
            {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
        ]

    def get_scoped_clans(self, guild_id=None):
        return list(self._clans)


def test_resolve_upgradecheck_clans_all_and_specific_filters():
    cog = UpgradesCog(_DummyBot())

    all_rows = cog._resolve_upgradecheck_clans(123, None)
    by_all_token = cog._resolve_upgradecheck_clans(123, "ALL")
    by_tag = cog._resolve_upgradecheck_clans(123, "#PQUCURCQ")
    by_name = cog._resolve_upgradecheck_clans(123, "cc2 dominion")

    assert len(all_rows) == 2
    assert len(by_all_token) == 2
    assert len(by_tag) == 1
    assert by_tag[0]["tag"] == "#PQUCURCQ"
    assert len(by_name) == 1
    assert by_name[0]["tag"] == "#2JJJCCRQR"


def test_extract_upgrading_hero_names_uses_api_and_active_cache():
    cog = UpgradesCog(_DummyBot())
    tag = "#PTEST"

    # Simulate known active cached upgrade for AQ even when API value is absent.
    from datetime import datetime, timezone, timedelta

    cog._active_hero_upgrades[tag] = {
        "Archer Queen": datetime.now(timezone.utc) + timedelta(minutes=30)
    }

    player = {
        "heroes": [
            {"name": "Barbarian King", "level": 80, "upgradeTimeLeft": "1h 20m"},
            {"name": "Archer Queen", "level": 80, "upgradeTimeLeft": None},
            {"name": "Grand Warden", "level": 55, "upgradeTimeLeft": 0},
        ]
    }

    out = cog._extract_upgrading_hero_names(player, tag)
    assert "Barbarian King" in out
    assert "Archer Queen" in out
    assert "Grand Warden" not in out
