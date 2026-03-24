"""FastAPI dashboard for read-only CC2 bot analytics."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from admin_tools import (
    add_base_entry,
    build_kick_suggestions,
    get_basebook,
    roster_csv_bytes,
    user_can_admin,
)
from auth import build_oauth_router, get_current_user
from data_access import DashboardRepository

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent
DB_FILE = Path(os.getenv("BOT_DB_FILE", "bot_data.sqlite3"))

app = FastAPI(title="CC2 Dashboard", version="0.1.0")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("DASHBOARD_SESSION_SECRET", "cc2-dashboard-dev-secret-change-me"),
)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
repo = DashboardRepository(data_dir=DATA_DIR, db_file=DB_FILE)
app.include_router(build_oauth_router())


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    clans = repo.load_clans()
    user = get_current_user(request)
    cards: list[dict[str, object]] = []

    for clan in clans:
        tag = str(clan.get("tag", ""))
        members = repo.latest_member_rows(tag)
        donations = repo.donation_chart(tag)
        war = repo.war_timeline(tag)
        raid = repo.raid_heatmap(tag)
        cards.append(
            {
                "name": clan.get("name", "Unnamed"),
                "tag": tag,
                "member_count": len(members),
                "latest_total_donations": donations["values"][-1] if donations["values"] else 0,
                "wars_tracked": war["summary"]["total"],
                "avg_raid_completion": round(sum(raid["values"]) / len(raid["values"]), 1)
                if raid["values"]
                else 0.0,
            }
        )

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "clans": clans,
            "cards": cards,
            "user": user,
        },
    )


@app.get("/clan/{clan_tag}", response_class=HTMLResponse)
def clan_dashboard(request: Request, clan_tag: str) -> HTMLResponse:
    user = get_current_user(request)
    if user is None:
        return templates.TemplateResponse(
            request,
            "auth_required.html",
            {
                "clans": repo.load_clans(),
            },
            status_code=401,
        )

    normalized = repo.normalize_tag(clan_tag)
    clans = repo.load_clans()
    clan = next((c for c in clans if c.get("tag") == normalized), None)

    if clan is None:
        raise HTTPException(status_code=404, detail=f"Clan not found: {normalized}")

    members = repo.latest_member_rows(normalized)
    donation_chart = repo.donation_chart(normalized)
    war_timeline = repo.war_timeline(normalized)
    raid_heatmap = repo.raid_heatmap(normalized)

    return templates.TemplateResponse(
        request,
        "clan.html",
        {
            "clan": clan,
            "clans": clans,
            "user": user,
            "members": members,
            "donation_chart": donation_chart,
            "war_timeline": war_timeline,
            "raid_heatmap": raid_heatmap,
        },
    )


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/admin", response_class=HTMLResponse)
def admin_dashboard(
    request: Request,
    clan_tag: str | None = None,
    player_tag: str | None = None,
    message: str | None = None,
    error: str | None = None,
) -> HTMLResponse:
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/auth/login", status_code=302)
    if not user_can_admin(user):
        raise HTTPException(status_code=403, detail="Admin access denied")

    clans = repo.load_clans()
    selected_tag = repo.normalize_tag(clan_tag or (clans[0]["tag"] if clans else ""))
    selected_clan = next((c for c in clans if c.get("tag") == selected_tag), None)
    members = repo.latest_member_rows(selected_tag) if selected_clan else []
    kick_lines = build_kick_suggestions(members, selected_tag) if selected_clan else []

    normalized_player_tag = repo.normalize_tag(player_tag or "") if player_tag else None
    basebook = get_basebook(normalized_player_tag) if normalized_player_tag else {}

    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "clans": clans,
            "clan": selected_clan,
            "selected_tag": selected_tag,
            "members": members,
            "kick_lines": kick_lines,
            "user": user,
            "player_tag": normalized_player_tag,
            "basebook": basebook,
            "message": message,
            "error": error,
        },
    )


@app.get("/admin/roster.csv")
def admin_roster_csv(request: Request, clan_tag: str) -> Response:
    user = get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Login required")
    if not user_can_admin(user):
        raise HTTPException(status_code=403, detail="Admin access denied")

    normalized = repo.normalize_tag(clan_tag)
    clan = next((c for c in repo.load_clans() if c.get("tag") == normalized), None)
    if clan is None:
        raise HTTPException(status_code=404, detail=f"Clan not found: {normalized}")

    rows = repo.latest_member_rows(normalized)
    payload = roster_csv_bytes(rows, str(clan.get("name", "Unnamed")), normalized)
    filename = f"roster_{normalized.replace('#', '')}.csv"
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.post("/admin/basebook/add")
async def admin_basebook_add(
    request: Request,
    player_tag: str,
    base_type: str,
    name: str,
    link: str,
) -> RedirectResponse:
    user = get_current_user(request)
    if user is None:
        return RedirectResponse(url="/auth/login", status_code=302)
    if not user_can_admin(user):
        raise HTTPException(status_code=403, detail="Admin access denied")

    normalized = repo.normalize_tag(player_tag)
    try:
        add_base_entry(
            player_tag=normalized,
            base_type=base_type,
            name=name,
            link=link,
            actor_id=str(user.get("id", "0")),
        )
        return RedirectResponse(url=f"/admin?player_tag={normalized}&message=Base+saved", status_code=303)
    except Exception as exc:
        return RedirectResponse(url=f"/admin?player_tag={normalized}&error={str(exc)}", status_code=303)
