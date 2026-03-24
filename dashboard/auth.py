"""Discord OAuth helpers and routes for dashboard auth."""

from __future__ import annotations

import os
import json
import asyncio
from typing import Any
from urllib import request, parse, error

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

DISCORD_AUTH_URL = "https://discord.com/api/oauth2/authorize"
DISCORD_TOKEN_URL = "https://discord.com/api/oauth2/token"
DISCORD_ME_URL = "https://discord.com/api/users/@me"
DISCORD_GUILDS_URL = "https://discord.com/api/users/@me/guilds"
DISCORD_GUILD_MEMBER_URL = "https://discord.com/api/guilds/{guild_id}/members/{user_id}"


def _http_post_form_json(url: str, form_data: dict[str, str]) -> dict[str, Any]:
    body = parse.urlencode(form_data).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def _http_get_json(url: str, access_token: str) -> Any:
    req = request.Request(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def _http_get_json_bot(url: str, bot_token: str) -> Any:
    req = request.Request(
        url,
        headers={"Authorization": f"Bot {bot_token}"},
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_oauth_router() -> APIRouter:
    router = APIRouter(prefix="/auth", tags=["auth"])

    @router.get("/login")
    async def login() -> RedirectResponse:
        client_id = _require_env("DISCORD_CLIENT_ID")
        redirect_uri = _require_env("DISCORD_REDIRECT_URI")
        scopes = "identify guilds"
        url = (
            f"{DISCORD_AUTH_URL}?client_id={client_id}"
            f"&response_type=code&redirect_uri={redirect_uri}"
            f"&scope={scopes.replace(' ', '%20')}"
            "&prompt=none"
        )
        return RedirectResponse(url=url, status_code=302)

    @router.get("/callback")
    async def callback(code: str, request: Request) -> RedirectResponse:
        client_id = _require_env("DISCORD_CLIENT_ID")
        client_secret = _require_env("DISCORD_CLIENT_SECRET")
        redirect_uri = _require_env("DISCORD_REDIRECT_URI")

        try:
            token_payload = await asyncio.to_thread(
                _http_post_form_json,
                DISCORD_TOKEN_URL,
                {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
            )
            access_token = token_payload.get("access_token")
            if not access_token:
                return RedirectResponse(url="/?auth=failed", status_code=302)

            me = await asyncio.to_thread(_http_get_json, DISCORD_ME_URL, access_token)
            guilds = await asyncio.to_thread(_http_get_json, DISCORD_GUILDS_URL, access_token)
        except Exception:
            return RedirectResponse(url="/?auth=failed", status_code=302)

        guild_id = os.getenv("DASHBOARD_GUILD_ID", "").strip()
        if guild_id:
            in_guild = any(str(g.get("id", "")) == guild_id for g in guilds if isinstance(g, dict))
            if not in_guild:
                return RedirectResponse(url="/?auth=not_member", status_code=302)

        leadership_role_id = os.getenv("DASHBOARD_LEADERSHIP_ROLE_ID", "").strip()
        admin_role_id = os.getenv("DASHBOARD_ADMIN_ROLE_ID", "").strip()
        configured_role_ids = {
            rid
            for rid in (leadership_role_id, admin_role_id)
            if str(rid).strip()
        }
        is_leadership = False

        user_id = str(me.get("id", "")).strip()
        if guild_id and user_id and configured_role_ids:
            bot_token = os.getenv("DASHBOARD_BOT_TOKEN", "").strip() or os.getenv("DISCORD_TOKEN", "").strip()
            if bot_token:
                member_url = DISCORD_GUILD_MEMBER_URL.format(guild_id=guild_id, user_id=user_id)
                try:
                    guild_member = await asyncio.to_thread(_http_get_json_bot, member_url, bot_token)
                    member_roles = set(
                        str(role_id)
                        for role_id in (guild_member.get("roles", []) if isinstance(guild_member, dict) else [])
                    )
                    is_leadership = bool(member_roles.intersection(configured_role_ids))
                except Exception:
                    is_leadership = False

        request.session["user"] = {
            "id": user_id,
            "username": str(me.get("username", "discord-user")),
            "global_name": str(me.get("global_name") or me.get("username") or "discord-user"),
            "avatar": str(me.get("avatar") or ""),
            "is_leadership": is_leadership,
        }
        return RedirectResponse(url="/", status_code=302)

    @router.get("/logout")
    async def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return RedirectResponse(url="/", status_code=302)

    return router


def get_current_user(request: Request) -> dict[str, Any] | None:
    user = request.session.get("user")
    if isinstance(user, dict) and user.get("id"):
        return user
    return None
