"""
CC2 Academy Discord Bot — Modular Cog Architecture (v2.0)
=========================================================
Refactored from the original 3,288-line monolith into a clean
``commands.Bot`` with modular Cogs, structured logging, persistent
views, hybrid commands, and discord.ext.tasks scheduled loops.

The original file is preserved as ``discordwelcomebot_backup.py``.

Run with:  python discordwelcomebot.py
"""

import asyncio
import logging
import signal
import urllib.parse
from collections import defaultdict, deque
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from config import (
    DISCORD_TOKEN, COC_API_KEY,
    COC_CONCURRENCY, COC_TIMEOUT, COC_API_BASE_URL,
    PLAYER_CACHE_TTL, CLAN_CACHE_TTL, WAR_CACHE_TTL,
    ANNOUNCE_CHANNEL_ID, NAME_CACHE_FILE,
    LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID,
)
from cache import api_cache, request_deduplicator
from storage import load_clans, load_strict_cache, load_json, load_settings, get_effective_setting, get_effective_clans
from migrate_json_to_sqlite import migrate_if_needed
from utils.logging_setup import setup_logging
from utils.helpers import has_leadership_role

logger = logging.getLogger("cc2bot")


class MaintenanceModeError(commands.CheckFailure):
    """Raised when maintenance mode blocks command execution."""


# ════════════════════════════════════════════
# Case-insensitive prefix helper
# ════════════════════════════════════════════

def _get_prefix(bot: commands.Bot, message: discord.Message):
    """Return 'cc2 ' prefix (case-insensitive) plus @mentions."""
    prefixes = commands.when_mentioned(bot, message)
    content = message.content
    if len(content) >= 4 and content[:4].lower() == "cc2 ":
        prefixes.append(content[:4])
    return prefixes


# ════════════════════════════════════════════
# Bot class
# ════════════════════════════════════════════

class CC2Bot(commands.Bot):
    """CC2 Academy Discord Bot with modular cog architecture."""

    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True

        super().__init__(
            command_prefix=_get_prefix,
            intents=intents,
            case_insensitive=True,
            help_command=None,  # custom help in admin cog
        )

        # Shared state (accessed by Cogs via self.bot.<attr>)
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.coc_semaphore: asyncio.Semaphore = asyncio.Semaphore(COC_CONCURRENCY)
        self.clans: List[Dict[str, str]] = load_clans()
        self.strict_join_cache: Dict[str, set] = {}
        self.initialized_baseline: set = set()
        settings = load_settings()
        self.raid_reminder_enabled: bool = bool(settings.get("raid_reminder_enabled", True))
        self.war_reminder_enabled: bool = bool(settings.get("war_reminder_enabled", True))
        self.start_time: datetime = datetime.now(timezone.utc)
        self.commands_run: int = 0
        self.command_usage: Dict[str, int] = defaultdict(int)
        self.command_usage_by_role: Dict[str, int] = defaultdict(int)
        self.command_usage_by_hour: Dict[int, int] = defaultdict(int)
        self._global_cooldown_seconds: float = 3.0
        self._last_command_by_user: Dict[int, datetime] = {}
        self._mute_until_by_user: Dict[int, datetime] = {}
        self._spam_tracker: Dict[tuple[int, str], deque] = defaultdict(deque)
        self.maintenance_mode: bool = bool(settings.get("maintenance_mode", False))
        self.maintenance_message: str = str(
            settings.get(
                "maintenance_message",
                "🛠️ Bot is under maintenance. Please try again later.",
            )
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Global slash command cooldown + mute check (parity with prefix)."""
        if interaction.type != discord.InteractionType.application_command:
            return True

        user = interaction.user
        if not user or getattr(user, "bot", False):
            return False

        if self.maintenance_mode and not has_leadership_role(user, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
            await interaction.response.send_message(
                self.maintenance_message,
                ephemeral=True,
            )
            return False

        now = datetime.now(timezone.utc)
        mute_until = self._mute_until_by_user.get(user.id)
        if mute_until and now < mute_until:
            remaining = int((mute_until - now).total_seconds())
            await interaction.response.send_message(
                f"🚫 You are temporarily muted from bot commands for spam ({remaining}s left).",
                ephemeral=True,
            )
            return False

        last = self._last_command_by_user.get(user.id)
        if last is not None and (now - last).total_seconds() < self._global_cooldown_seconds:
            await interaction.response.send_message(
                f"⏳ Global cooldown active. Try again in {self._global_cooldown_seconds:.0f}s.",
                ephemeral=True,
            )
            return False

        self._last_command_by_user[user.id] = now
        return True

    def _record_command_usage(self, user: Any, cmd_name: str) -> None:
        """Record command analytics and enforce anti-spam mute windows."""
        self.commands_run += 1

        cmd_name = (cmd_name or "unknown").lower()
        self.command_usage[cmd_name] += 1

        role_key = "dm_or_unknown"
        perms = getattr(user, "guild_permissions", None)
        if perms:
            if perms.administrator:
                role_key = "administrator"
            elif perms.manage_guild:
                role_key = "manage_guild"
            elif perms.manage_messages:
                role_key = "moderator"
            else:
                role_key = "member"
        self.command_usage_by_role[role_key] += 1

        hour = datetime.now(timezone.utc).hour
        self.command_usage_by_hour[hour] += 1

        if user and not getattr(user, "bot", False):
            key = (user.id, cmd_name)
            now = datetime.now(timezone.utc)
            q = self._spam_tracker[key]
            q.append(now)
            while q and (now - q[0]).total_seconds() > 10:
                q.popleft()

            if len(q) >= 5:
                self._mute_until_by_user[user.id] = now + timedelta(minutes=5)
                q.clear()

    def resolve_effective_setting(self, key: str, default: Any = None, guild_id: Optional[int] = None) -> Any:
        """Resolve a setting with optional guild-scoped override fallback to global."""
        try:
            return get_effective_setting(key, default=default, guild_id=guild_id)
        except Exception:
            return default

    def get_scoped_clans(self, guild_id: Optional[int] = None) -> List[Dict[str, str]]:
        """Return monitored clan list for a guild, with fallback to global list."""
        try:
            return get_effective_clans(guild_id, self.clans)
        except Exception:
            return list(self.clans)

    def get_all_monitored_clans(self) -> List[Dict[str, str]]:
        """Return a deduplicated union of global and guild-scoped clan configs."""
        by_tag: Dict[str, Dict[str, str]] = {}
        for c in self.clans:
            tag = str(c.get("tag", "")).upper()
            if tag:
                by_tag[tag] = {"name": c.get("name", "Unnamed"), "tag": tag}

        for g in self.guilds:
            for c in self.get_scoped_clans(g.id):
                tag = str(c.get("tag", "")).upper()
                if tag and tag not in by_tag:
                    by_tag[tag] = {"name": c.get("name", "Unnamed"), "tag": tag}

        return list(by_tag.values())

    def is_clan_monitored_anywhere(self, clan_tag: str) -> bool:
        target = str(clan_tag or "").upper()
        if not target.startswith("#"):
            target = "#" + target
        return any(c.get("tag", "").upper() == target for c in self.get_all_monitored_clans())

    def get_guild_ids_for_clan(self, clan_tag: str) -> List[int]:
        """Return guild IDs whose effective clan list includes the given clan tag."""
        target = str(clan_tag or "").upper()
        if not target.startswith("#"):
            target = "#" + target
        ids: List[int] = []
        for g in self.guilds:
            scoped = self.get_scoped_clans(g.id)
            if any(c.get("tag", "").upper() == target for c in scoped):
                ids.append(g.id)
        return ids

    async def get_announce_channels_for_clan(self, clan_tag: str) -> List[Any]:
        """Return announce channels for guilds that monitor this clan."""
        channels = []
        seen_ids = set()
        for gid in self.get_guild_ids_for_clan(clan_tag):
            ch = await self.get_announce_channel(gid)
            if ch and getattr(ch, "id", None) not in seen_ids:
                channels.append(ch)
                seen_ids.add(ch.id)

        if not channels:
            ch = await self.get_announce_channel()
            if ch:
                channels.append(ch)
        return channels

    async def get_all_announce_channels(self) -> List[Any]:
        """Return all configured announce channels across connected guilds."""
        channels = []
        seen_ids = set()
        for g in self.guilds:
            ch = await self.get_announce_channel(g.id)
            if ch and getattr(ch, "id", None) not in seen_ids:
                channels.append(ch)
                seen_ids.add(ch.id)
        if not channels:
            ch = await self.get_announce_channel()
            if ch:
                channels.append(ch)
        return channels

    async def get_announce_channel(self, guild_id: Optional[int] = None):
        """Resolve and return the configured announce channel."""
        raw = self.resolve_effective_setting("announce_channel_id", ANNOUNCE_CHANNEL_ID, guild_id=guild_id)
        try:
            channel_id = int(raw)
        except Exception:
            channel_id = ANNOUNCE_CHANNEL_ID

        ch = self.get_channel(channel_id)
        if ch is not None:
            return ch
        try:
            return await self.fetch_channel(channel_id)
        except Exception:
            return None

    # ────────────────────────────
    # Lifecycle
    # ────────────────────────────
    async def setup_hook(self):
        """Called once before the bot connects.  Initializes shared
        resources, registers persistent views, and loads all cogs.
        """
        logger.info("Running setup_hook …")

        # One-time staged migration from JSON -> SQLite.
        try:
            migration_result = migrate_if_needed(force=False)
            if migration_result:
                migrated = [k for k, ok in migration_result.items() if ok]
                skipped = [k for k, ok in migration_result.items() if not ok]
                logger.info(
                    "Startup migration executed. migrated=%s skipped=%s",
                    ",".join(migrated) or "none",
                    ",".join(skipped) or "none",
                )
        except Exception as mig_err:
            logger.warning("Startup migration check failed: %s", mig_err)

        # HTTP session for CoC API
        self.http_session = aiohttp.ClientSession()

        # Load strict join caches per clan
        for c in self.clans:
            try:
                self.strict_join_cache[c["tag"]] = load_strict_cache(c["tag"])
            except Exception:
                self.strict_join_cache[c["tag"]] = set()

        # Load name cache (kept for compatibility)
        _ = load_json(NAME_CACHE_FILE) or {}

        # Register persistent views (survive bot restart)
        from cogs.profiles import PlayerProfileView
        from cogs.membership import JoinEmbedView
        self.add_view(PlayerProfileView())
        self.add_view(JoinEmbedView())

        # Load cog extensions
        cog_extensions = [
            "cogs.profiles",
            "cogs.membership",
            "cogs.war",
            "cogs.raids",
            "cogs.leaderboards",
            "cogs.achievements",
            "cogs.challenges",
            "cogs.donations_cog",
            "cogs.upgrades",
            "cogs.runtime_config",
            "cogs.admin",
        ]
        for ext in cog_extensions:
            try:
                await self.load_extension(ext)
                logger.info("Loaded cog: %s", ext)
            except Exception as exc:
                logger.error("Failed to load cog %s: %s", ext, exc, exc_info=True)

        # Prefix-command global cooldown check.
        async def _global_prefix_cooldown(ctx: commands.Context) -> bool:
            author = getattr(ctx, "author", None)
            if not author:
                return True
            if getattr(author, "bot", False):
                return False

            if self.maintenance_mode and not has_leadership_role(author, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID):
                raise MaintenanceModeError(self.maintenance_message)

            now = datetime.now(timezone.utc)
            mute_until = self._mute_until_by_user.get(author.id)
            if mute_until and now < mute_until:
                remaining = int((mute_until - now).total_seconds())
                raise commands.CheckFailure(f"Temporarily muted from bot commands for spam ({remaining}s left).")

            last = self._last_command_by_user.get(author.id)
            if last is not None and (now - last).total_seconds() < self._global_cooldown_seconds:
                raise commands.CommandOnCooldown(commands.Cooldown(1, self._global_cooldown_seconds), self._global_cooldown_seconds, commands.BucketType.user)

            self._last_command_by_user[author.id] = now
            return True

        self.add_check(_global_prefix_cooldown)

    async def on_ready(self):
        logger.info("Logged in as %s (id: %s)", self.user, self.user.id)

        # Sync slash commands
        try:
            synced = await self.tree.sync()
            logger.info("Synced %d slash command(s).", len(synced))
        except Exception as exc:
            logger.warning("Slash command sync failed: %s", exc)

        # Startup status embeds
        try:
            ch = await self.get_announce_channel()
            if ch:
                for c in self.clans:
                    emb = discord.Embed(
                        title=f"🔁 Startup Status — {c['name']}",
                        color=0x3498DB,
                        timestamp=datetime.now(timezone.utc),
                    )
                    emb.description = (
                        "Baseline loaded — bot will not announce existing members. "
                        "Only real joins/leaves are announced."
                    )
                    await ch.send(embed=emb)
        except Exception:
            pass

    async def on_command(self, ctx: commands.Context):
        """Track prefix command analytics and enforce anti-spam mute notices."""
        author = getattr(ctx, "author", None)
        cmd_name = (ctx.command.qualified_name if ctx.command else "unknown")
        self._record_command_usage(author, cmd_name)

        mute_until = self._mute_until_by_user.get(getattr(author, "id", 0))
        if mute_until and datetime.now(timezone.utc) < mute_until:
            try:
                await ctx.send(
                    "🚫 Command spam detected. You are muted from bot commands for 5 minutes.",
                    delete_after=15,
                )
            except Exception:
                pass

    async def on_app_command_completion(
        self,
        interaction: discord.Interaction,
        command: app_commands.Command | app_commands.ContextMenu,
    ):
        """Track slash command analytics and enforce anti-spam mutes."""
        self._record_command_usage(interaction.user, getattr(command, "qualified_name", "unknown"))

    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Global error handler — friendly messages for common failures."""
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(
                f"⏳ This command is on cooldown. Try again in **{error.retry_after:.0f}s**.",
                ephemeral=True, delete_after=10,
            )
        elif isinstance(error, commands.MissingPermissions):
            perms = ", ".join(error.missing_permissions)
            await ctx.send(f"🔒 You need: **{perms}**", ephemeral=True)
        elif isinstance(error, commands.BotMissingPermissions):
            perms = ", ".join(error.missing_permissions)
            await ctx.send(f"⚠️ Bot needs: **{perms}**", ephemeral=True)
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"❌ Bad argument: {error}", ephemeral=True)
        elif isinstance(error, commands.CommandNotFound):
            pass  # silently ignore unknown commands
        elif isinstance(error, commands.CheckFailure):
            if isinstance(error, MaintenanceModeError):
                await ctx.send(str(error), ephemeral=True)
            else:
                await ctx.send("❌ You don't have permission to run this.", ephemeral=True)
        else:
            logger.error("Unhandled command error in %s: %s", ctx.command, error, exc_info=error)
            await ctx.send("❌ Something went wrong. Please try again.", ephemeral=True)

    async def close(self):
        if self.http_session:
            try:
                await self.http_session.close()
            except Exception:
                pass
        await super().close()

    # ────────────────────────────
    # CoC API helpers (shared)
    # ────────────────────────────
    async def coc_get(self, path: str, _retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch from CoC API with caching, request deduplication, and retry on 429."""
        if not COC_API_KEY or self.http_session is None:
            return None

        path_norm = path if path.startswith("/") else "/" + path
        cache_key = f"coc:{path_norm}"

        # Determine TTL by endpoint
        ttl = CLAN_CACHE_TTL
        if "/players/" in path_norm:
            ttl = PLAYER_CACHE_TTL
        elif "/currentwar" in path_norm:
            ttl = WAR_CACHE_TTL

        # Check cache
        cached = await api_cache.get(cache_key, ttl)
        if cached is not None:
            return cached

        # Deduplicated fetch with exponential backoff on 429
        async def _fetch():
            url = f"{COC_API_BASE_URL}{path_norm}"
            headers = {"Authorization": f"Bearer {COC_API_KEY}"}
            backoff = 1.0
            for attempt in range(_retries):
                async with self.coc_semaphore:
                    try:
                        async with self.http_session.get(
                            url, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=COC_TIMEOUT),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                await api_cache.set(cache_key, data)
                                return data
                            if resp.status == 429:
                                retry_after = float(resp.headers.get("Retry-After", backoff))
                                logger.warning(
                                    "Rate limited (429) on %s — retrying in %.1fs (attempt %d/%d)",
                                    path_norm, retry_after, attempt + 1, _retries,
                                )
                                await asyncio.sleep(retry_after)
                                backoff = min(backoff * 2, 30.0)
                                continue
                            if resp.status == 503:
                                logger.warning("CoC API 503 on %s — retrying in %.1fs", path_norm, backoff)
                                await asyncio.sleep(backoff)
                                backoff = min(backoff * 2, 30.0)
                                continue
                            # 404, 403, etc. — don't retry
                            logger.debug("CoC API %d on %s", resp.status, path_norm)
                            return None
                    except asyncio.TimeoutError:
                        logger.warning("Timeout on %s (attempt %d/%d)", path_norm, attempt + 1, _retries)
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 15.0)
                    except aiohttp.ClientError as e:
                        logger.warning("Client error on %s: %s", path_norm, e)
                        return None
            return None

        return await request_deduplicator.get_or_create(cache_key, _fetch)

    async def get_player(self, tag: str) -> Optional[Dict[str, Any]]:
        return await self.coc_get(f"/players/{urllib.parse.quote(tag)}")

    async def get_clan_member_list(self, clan_tag: str) -> List[Dict[str, Any]]:
        data = await self.coc_get(f"/clans/{urllib.parse.quote(clan_tag)}")
        if not data:
            return []
        return data.get("memberList", [])

    async def get_current_war(self, clan_tag: str) -> Optional[Dict[str, Any]]:
        return await self.coc_get(f"/clans/{urllib.parse.quote(clan_tag)}/currentwar")

    async def fetch_players(
        self, tags: List[str], concurrency: int = COC_CONCURRENCY
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Fetch many players in parallel; returns ``{tag: player_json | None}``."""
        results: Dict[str, Optional[Dict[str, Any]]] = {}
        if not tags:
            return results
        sem = asyncio.Semaphore(concurrency)

        async def _fetch_one(tag: str):
            async with sem:
                try:
                    results[tag] = await self.get_player(tag)
                except Exception:
                    results[tag] = None

        tasks = [asyncio.create_task(_fetch_one(t)) for t in tags]
        await asyncio.gather(*tasks)
        return results

    # ────────────────────────────
    # Log helper (channel + stdout)
    # ────────────────────────────
    async def log(self, msg: str):
        logger.info(msg)
        try:
            from config import LOG_CHANNEL_ID
            ch = self.get_channel(LOG_CHANNEL_ID)
            if ch is None:
                ch = await self.fetch_channel(LOG_CHANNEL_ID)
            if ch:
                await ch.send(f"[LOG {datetime.now(timezone.utc).isoformat()}] {msg}")
        except Exception:
            pass


# ════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════

def main():
    setup_logging()

    if (
        not DISCORD_TOKEN or not COC_API_KEY
        or DISCORD_TOKEN.startswith("token")
        or COC_API_KEY.startswith("api")
    ):
        logger.critical("DISCORD_TOKEN and COC_API_KEY must be set (config.py / env).")
        return

    bot = CC2Bot()

    # Graceful shutdown on Ctrl+C / SIGTERM
    def _handle_signal(sig, frame):
        logger.info("Received %s — shutting down gracefully…", signal.Signals(sig).name)
        asyncio.ensure_future(bot.close())

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    bot.run(DISCORD_TOKEN, log_handler=None)  # we handle logging ourselves


if __name__ == "__main__":
    main()

