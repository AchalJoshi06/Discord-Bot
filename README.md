# CC2 Discord Bot

CC2 Discord Bot is a Clash of Clans automation bot for war tracking, raid analytics, leaderboards, and member-management workflows.

## Roadmap status

All tracked improvements in `bot_improvemtns.txt` are marked complete as of March 19, 2026.

## Run locally

```powershell
Set-Location "D:\CC2 Academy\Discord bot"
.\.venv\Scripts\Activate.ps1
python discordwelcomebot.py
```

## Web dashboard (read-only)

The dashboard lives in `dashboard/` and reads existing bot datasets in read-only mode.

Environment variables for auth-enabled mode:
- `DASHBOARD_SESSION_SECRET` (required in production)
- `DISCORD_CLIENT_ID`
- `DISCORD_CLIENT_SECRET`
- `DISCORD_REDIRECT_URI` (for example: `http://127.0.0.1:8080/auth/callback`)
- `DASHBOARD_GUILD_ID` (optional; restrict access to members of one server)
- `DASHBOARD_LEADERSHIP_ROLE_ID` (optional; leadership role id for admin tools)
- `DASHBOARD_ADMIN_ROLE_ID` (optional; alternate admin role id)
- `DASHBOARD_BOT_TOKEN` (optional; falls back to `DISCORD_TOKEN` to query guild member roles)
- `DASHBOARD_ADMIN_OPEN` (optional, default `0`; if `1`, any authenticated user can access `/admin`)

```powershell
Set-Location "D:\CC2 Academy\Discord bot\dashboard"
"d:/CC2 Academy/.venv/Scripts/python.exe" -m pip install -r requirements.txt
"d:/CC2 Academy/.venv/Scripts/python.exe" -m uvicorn app:app --reload --port 8080
```

Open `http://127.0.0.1:8080`.

Current scope:
- Clan/member stats table (rush + activity when available)
- Donation trend chart
- War win/loss timeline
- Raid weekend completion chart
- Discord OAuth login for protected clan dashboard pages
- Admin tools page (`/admin`) for kick suggestions snapshot, roster CSV export, and basebook management

## CI

GitHub Actions workflow is defined at `.github/workflows/ci.yml`.
It runs on `push` and `pull_request` with:
- Critical lint checks (`python -m ruff check . --select E9,F63,F7,F82`)
- Syntax compile validation (`python -m compileall -q .`)
- Test suite (`pytest -q`)

## Auto-restart deployment

### Linux (systemd)

1. Copy `deployment/systemd/cc2bot.service.template` to `/etc/systemd/system/cc2bot.service`.
2. Edit paths and environment variables in the service file.
3. Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable cc2bot
sudo systemctl restart cc2bot
sudo systemctl status cc2bot
```

The service uses:
- `Restart=on-failure`
- `RestartSec=5`

### Windows (Task Scheduler or startup script)

Option A: Use `deployment/windows/restart-bot.ps1` as the scheduled task action:

```powershell
powershell.exe -ExecutionPolicy Bypass -File "D:\CC2 Academy\Discord bot\deployment\windows\restart-bot.ps1"
```

Option B: Use NSSM to run the same script as a service.

Both options implement automatic restart after unexpected exits.
