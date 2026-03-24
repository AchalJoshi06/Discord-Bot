import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from migrate_json_to_sqlite import migrate_if_needed  # noqa: E402
from storage import load_settings, load_clans  # noqa: E402


def test_migrate_if_needed_moves_json_data_once(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    (tmp_path / "settings.json").write_text(
        '{"raid_reminder_enabled": false, "war_reminder_enabled": true}',
        encoding="utf-8",
    )
    (tmp_path / "clans.json").write_text(
        '[{"name":"CC2 Academy","tag":"#PQUCURCQ"}]',
        encoding="utf-8",
    )

    first = migrate_if_needed(force=False)
    assert isinstance(first, dict)
    assert first.get("settings") is True
    assert first.get("clans") is True
    assert os.path.exists("bot_data.sqlite3")

    # Second run should be a no-op because DB has data.
    second = migrate_if_needed(force=False)
    assert second == {}

    settings = load_settings()
    assert settings["raid_reminder_enabled"] is False

    clans = load_clans()
    assert clans
    assert clans[0]["tag"] == "#PQUCURCQ"
