"""Hero upgrade alerts and general upgrade tracking loops (per-clan)."""
import logging
import asyncio
import re
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from discord.ext import commands

from config import UPGRADE_CHECK_INTERVAL, UPGRADE_ALERT_CHECK
from utils.helpers import truncate, build_paginated_embeds, send_paginated_embeds
from calculations import calculate_weighted_rush_score
from cogs.profiles import clan_autocomplete

logger = logging.getLogger("cc2bot.cogs.upgrades")


class UpgradesCog(commands.Cog, name="Upgrades"):
    """Background tasks that alert when heroes or troops start upgrading."""

    def __init__(self, bot):
        self.bot = bot
        self._hero_tasks: Dict[str, asyncio.Task] = {}
        self._alert_tasks: Dict[str, asyncio.Task] = {}
        self._last_upgrade_cache: Dict[str, List[str]] = {}
        self._last_hero_levels: Dict[str, Dict[str, int]] = {}
        self._last_th_levels: Dict[str, int] = {}
        self._active_hero_upgrades: Dict[str, Dict[str, datetime]] = {}

    @staticmethod
    def _parse_upgrade_seconds(raw: Any) -> int:
        """Parse upgradeTimeLeft payload into seconds.

        Supports integer seconds and loose strings like "1d 3h 20m".
        """
        if raw is None:
            return 0
        if isinstance(raw, (int, float)):
            return max(0, int(raw))

        text = str(raw).strip().lower()
        if not text:
            return 0

        try:
            return max(0, int(float(text)))
        except Exception:
            pass

        total = 0
        for value, unit in re.findall(r"(\d+)\s*([dhms])", text):
            n = int(value)
            if unit == "d":
                total += n * 86400
            elif unit == "h":
                total += n * 3600
            elif unit == "m":
                total += n * 60
            elif unit == "s":
                total += n
        return total

    def _prune_expired_active_upgrades(self, player_tag: str) -> None:
        now = datetime.now(timezone.utc)
        active = self._active_hero_upgrades.get(player_tag, {})
        if not active:
            return
        active = {name: until for name, until in active.items() if isinstance(until, datetime) and until > now}
        if active:
            self._active_hero_upgrades[player_tag] = active
        else:
            self._active_hero_upgrades.pop(player_tag, None)

    def get_active_hero_upgrades_for_tag(self, player_tag: str) -> List[str]:
        """Return currently active hero upgrade names known by tracker."""
        tag = str(player_tag or "").strip().upper()
        if not tag:
            return []
        self._prune_expired_active_upgrades(tag)
        active = self._active_hero_upgrades.get(tag, {})
        return sorted(active.keys())

    def _resolve_upgradecheck_clans(self, guild_id: int | None, clan_filter: str | None) -> List[Dict[str, str]]:
        """Resolve target clans for upgradecheck command.

        - No filter or ALL => all guild-scoped monitored clans
        - tag/name filter => matched subset
        """
        scoped = list(self.bot.get_scoped_clans(guild_id) or [])
        if not scoped:
            return []

        token = str(clan_filter or "").strip()
        if not token or token.upper() == "ALL":
            return scoped

        token_upper = token.upper()
        if token_upper and not token_upper.startswith("#") and any(ch.isdigit() for ch in token_upper):
            token_upper = f"#{token_upper}"

        matched: List[Dict[str, str]] = []
        for c in scoped:
            ctag = str(c.get("tag", "")).upper()
            cname = str(c.get("name", "")).strip().lower()
            if token_upper == ctag or token.strip().lower() == cname:
                matched.append(c)
        return matched

    def _extract_upgrading_hero_names(self, player: Dict[str, Any], player_tag: str) -> List[str]:
        """Return hero names currently upgrading using API signal + active tracker cache."""
        tag = str(player_tag or "").upper()
        self._prune_expired_active_upgrades(tag)
        active_names = set(self._active_hero_upgrades.get(tag, {}).keys())

        upgrading: List[str] = []
        for hero in player.get("heroes", []) or []:
            hero_name = str(hero.get("name") or "").strip()
            if not hero_name:
                continue
            seconds_left = self._parse_upgrade_seconds(hero.get("upgradeTimeLeft"))
            if seconds_left > 0 or hero_name in active_names:
                if hero_name not in upgrading:
                    upgrading.append(hero_name)
        return upgrading

    @commands.hybrid_command(name="upgradecheck", aliases=["uc"], description="Show players upgrading N+ heroes")
    @app_commands.describe(
        min_heroes="Minimum upgrading heroes to include (0 shows all players)",
        clan="Clan name/tag, or ALL",
    )
    @app_commands.autocomplete(clan=clan_autocomplete)
    async def upgradecheck(self, ctx: commands.Context, min_heroes: int = 1, clan: str | None = None):
        """Check who is currently upgrading heroes across monitored clans."""
        try:
            await ctx.defer()
        except Exception:
            pass

        min_required = max(0, min(int(min_heroes or 0), 6))
        guild_id = ctx.guild.id if ctx.guild else None
        target_clans = self._resolve_upgradecheck_clans(guild_id, clan)
        if not target_clans:
            return await ctx.send("❌ No matching monitored clan found for that filter.")

        rows: List[tuple[str, str, str, List[str]]] = []
        checked_players = 0
        for clan_row in target_clans:
            clan_tag = str(clan_row.get("tag") or "")
            clan_name = str(clan_row.get("name") or clan_tag)
            members = await self.bot.get_clan_member_list(clan_tag)
            tags = [str(m.get("tag") or "") for m in (members or []) if m.get("tag")]
            if not tags:
                continue

            players_map = await self.bot.fetch_players(tags)
            for tag in tags:
                player = players_map.get(tag)
                if not isinstance(player, dict):
                    continue
                checked_players += 1
                upgrading = self._extract_upgrading_hero_names(player, tag)
                if min_required == 0 or len(upgrading) >= min_required:
                    rows.append((
                        str(player.get("name") or tag),
                        tag,
                        clan_name,
                        upgrading,
                    ))

        title_suffix = "All Players" if min_required == 0 else f"{min_required}+ Heroes"
        title = f"⬆️ Upgrade Check — {title_suffix}"

        if not rows:
            emb = discord.Embed(
                title=title,
                description="No matching players found for the selected filter.",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            emb.add_field(name="Clans Checked", value=str(len(target_clans)), inline=True)
            emb.add_field(name="Players Scanned", value=str(checked_players), inline=True)
            emb.set_footer(text="CC2 Clash Bot • Upgrade Check")
            return await ctx.send(embed=emb)

        rows.sort(key=lambda r: (len(r[3]), r[0].lower()), reverse=True)
        lines: List[str] = []
        for name, tag, clan_name, upgrading in rows:
            hero_list = ", ".join(upgrading) if upgrading else "none"
            lines.append(
                f"• **{name}** `{tag}` — **{len(upgrading)}** hero(es) upgrading ({hero_list}) — *{clan_name}*"
            )

        embeds = build_paginated_embeds(
            title=title,
            lines=lines,
            color=discord.Color.blurple(),
            per_page=8,
            footer_prefix="CC2 Clash Bot • Upgrade Check",
        )
        for emb in embeds:
            emb.add_field(name="Clans Checked", value=str(len(target_clans)), inline=True)
            emb.add_field(name="Players Scanned", value=str(checked_players), inline=True)
        await send_paginated_embeds(ctx, embeds)

    async def cog_load(self):
        for clan in self.bot.get_all_monitored_clans():
            self.start_tracking(clan)

    async def cog_unload(self):
        for tag in list(self._hero_tasks):
            self.stop_tracking(tag)

    # ── public API ──
    def start_tracking(self, clan: Dict[str, str]):
        tag = clan["tag"]
        if tag not in self._hero_tasks:
            self._hero_tasks[tag] = asyncio.create_task(self._hero_upgrade_loop(clan))
            self._alert_tasks[tag] = asyncio.create_task(self._upgrade_alert_loop(clan))
            logger.info(f"Started upgrade trackers for {clan['name']} ({tag})")

    def stop_tracking(self, clan_tag: str):
        for d in (self._hero_tasks, self._alert_tasks):
            task = d.pop(clan_tag, None)
            if task:
                task.cancel()

    # ── hero upgrade loop (alerts when ≥3 heroes upgrading) ──
    async def _hero_upgrade_loop(self, clan: Dict[str, str]):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
                members = await self.bot.get_clan_member_list(clan["tag"])
                if not members:
                    await asyncio.sleep(UPGRADE_CHECK_INTERVAL)
                    continue
                for m in members:
                    tag = m.get("tag")
                    if not tag:
                        continue
                    player = await self.bot.get_player(tag)
                    if not player:
                        continue
                    upgrading = []
                    self._prune_expired_active_upgrades(tag)
                    active_map = self._active_hero_upgrades.get(tag, {})
                    for h in player.get("heroes", []) or []:
                        ut = h.get("upgradeTimeLeft")
                        hero_name = str(h.get("name") or "Unknown Hero")
                        seconds_left = self._parse_upgrade_seconds(ut)
                        if seconds_left > 0:
                            active_map[hero_name] = datetime.now(timezone.utc) + timedelta(seconds=seconds_left)
                        if hero_name in active_map:
                            upgrading.append(hero_name)
                    if active_map:
                        self._active_hero_upgrades[tag] = active_map
                    if len(upgrading) >= 3:
                        embed = discord.Embed(
                            title="⚠️ Hero Upgrade Alert",
                            description=f"**{player.get('name')}** (`{tag}`) is upgrading **{len(upgrading)} heroes**!",
                            color=0xE67E22,
                            timestamp=datetime.now(timezone.utc),
                        )
                        embed.add_field(name="Heroes", value="\n".join(upgrading) if upgrading else "—")
                        try:
                            for channel in channels:
                                await channel.send(embed=embed)
                        except Exception:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Hero upgrade check error for {clan['name']}: {e}")
            await asyncio.sleep(UPGRADE_CHECK_INTERVAL)

    # ── general upgrade alert loop (new upgrades started) ──
    async def _upgrade_alert_loop(self, clan: Dict[str, str]):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                channels = await self.bot.get_announce_channels_for_clan(clan["tag"])
                members = await self.bot.get_clan_member_list(clan["tag"])
                if not members:
                    await asyncio.sleep(UPGRADE_ALERT_CHECK)
                    continue
                for m in members:
                    tag = m.get("tag")
                    if not tag:
                        continue
                    player = await self.bot.get_player(tag)
                    if not player:
                        continue
                    upgrading: List[str] = []
                    self._prune_expired_active_upgrades(tag)
                    active_map = self._active_hero_upgrades.get(tag, {})
                    for u in player.get("heroes", []):
                        ut = u.get("upgradeTimeLeft")
                        hero_name = str(u.get("name") or "Unknown Hero")
                        seconds_left = self._parse_upgrade_seconds(ut)
                        if seconds_left > 0:
                            active_map[hero_name] = datetime.now(timezone.utc) + timedelta(seconds=seconds_left)
                        if hero_name in active_map:
                            upgrading.append(f"Hero: {hero_name} → L{(u.get('level') or 0) + 1}")
                    if active_map:
                        self._active_hero_upgrades[tag] = active_map
                    for u in player.get("pets", []):
                        ut = u.get("upgradeTimeLeft")
                        if ut is not None and ut not in (0, "0", ""):
                            upgrading.append(f"Pet: {u.get('name')} → L{(u.get('level') or 0) + 1}")
                    for u in (player.get("troops") or []) + (player.get("spells") or []):
                        ut = u.get("upgradeTimeLeft")
                        if ut is not None and ut not in (0, "0", ""):
                            upgrading.append(f"Troop/Spell: {u.get('name')} → L{(u.get('level') or 0) + 1}")

                    old = self._last_upgrade_cache.get(tag, [])
                    new_upgrades = [x for x in upgrading if x not in old]
                    if new_upgrades:
                        embed = discord.Embed(
                            title=f"⬆️ Upgrade Started — {player.get('name')}",
                            color=0x00AAFF,
                            timestamp=datetime.now(timezone.utc),
                        )
                        embed.add_field(name="New Upgrades", value=truncate("\n".join(new_upgrades)) if new_upgrades else "—")
                        embed.set_footer(text=tag)
                        try:
                            for channel in channels:
                                await channel.send(embed=embed)
                        except Exception:
                            pass

                    # Detect completed hero upgrades by level increase vs cached levels.
                    current_hero_levels: Dict[str, int] = {}
                    for hero in player.get("heroes", []) or []:
                        hero_name = str(hero.get("name") or "").strip()
                        if not hero_name:
                            continue
                        try:
                            current_hero_levels[hero_name] = int(hero.get("level", 0) or 0)
                        except Exception:
                            continue

                    previous_hero_levels = self._last_hero_levels.get(tag)
                    if previous_hero_levels:
                        completed_lines: List[str] = []
                        for hero_name, level in current_hero_levels.items():
                            prev = int(previous_hero_levels.get(hero_name, level))
                            if level > prev:
                                if tag in self._active_hero_upgrades:
                                    self._active_hero_upgrades[tag].pop(hero_name, None)
                                    if not self._active_hero_upgrades[tag]:
                                        self._active_hero_upgrades.pop(tag, None)
                                completed_lines.append(f"• {hero_name}: **{prev} → {level}**")

                        if completed_lines:
                            complete_embed = discord.Embed(
                                title=f"🎉 Hero Upgrade Complete — {player.get('name')}",
                                color=discord.Color.green(),
                                timestamp=datetime.now(timezone.utc),
                            )
                            complete_embed.add_field(name="Completed", value=truncate("\n".join(completed_lines)), inline=False)
                            complete_embed.set_footer(text=tag)
                            try:
                                for channel in channels:
                                    await channel.send(embed=complete_embed)
                            except Exception:
                                pass

                    # Detect Town Hall upgrades by level increase.
                    try:
                        current_th = int(player.get("townHallLevel", 0) or 0)
                    except Exception:
                        current_th = 0
                    previous_th = self._last_th_levels.get(tag)
                    if previous_th is not None and current_th > previous_th:
                        rush = calculate_weighted_rush_score(player)
                        rush_score = float((rush or {}).get("score", 0.0) or 0.0)
                        th_embed = discord.Embed(
                            title=f"🏰 Town Hall Upgrade — {player.get('name')}",
                            description=f"Welcome to **TH{current_th}**!",
                            color=discord.Color.gold(),
                            timestamp=datetime.now(timezone.utc),
                        )
                        th_embed.add_field(name="Player", value=f"`{tag}`", inline=True)
                        th_embed.add_field(name="Previous TH", value=f"TH{previous_th}", inline=True)
                        th_embed.add_field(name="New TH", value=f"TH{current_th}", inline=True)
                        th_embed.add_field(name="Rush Score", value=f"{rush_score:.2f}%", inline=True)
                        th_embed.set_footer(text="CC2 Clash Bot • TH Upgrade")
                        try:
                            for channel in channels:
                                await channel.send(embed=th_embed)
                        except Exception:
                            pass

                    self._last_upgrade_cache[tag] = upgrading
                    self._last_hero_levels[tag] = current_hero_levels
                    if current_th > 0:
                        self._last_th_levels[tag] = current_th
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Upgrade alert error for {clan['name']}: {e}")
            await asyncio.sleep(UPGRADE_ALERT_CHECK)


async def setup(bot):
    await bot.add_cog(UpgradesCog(bot))
