"""Leaderboard and ranking commands (/top, /myrank)."""
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from calculations import calculate_weighted_rush_score, calculate_activity_score, extract_hero_levels
from cogs.profiles import clan_autocomplete
from cogs.raids import get_latest_raid_weekend
from storage import get_linked_tag_for_user, load_monthly_leaderboard, load_leaderboard_snapshot, load_war_player_stats
from storage import load_achievements_data, load_rush_history_for_player
from utils.helpers import normalize_tag, is_valid_tag, build_paginated_embeds, ClanSelectView
from donations import get_current_month_key

logger = logging.getLogger("cc2bot.cogs.leaderboards")

VALID_CATEGORIES = {
    "donations": "Donations",
    "trophies": "Trophies",
    "war_stars": "War Stars",
    "cwl_stars": "CWL Stars",
    "top_loot": "Top Loot",
    "rush_score": "Rush Score",
    "activity_score": "Activity Score",
    "raid_loot": "Raid Loot",
    "donation_ratio": "Donation Ratio",
}

CATEGORY_FORMULAS = {
    "donations": "Sorted by current season donations (`player.donations`), highest first.",
    "trophies": "Sorted by current trophies (`player.trophies`), highest first.",
    "war_stars": "Sorted by lifetime war stars (`player.warStars`), highest first.",
    "cwl_stars": "Sorted by CWL stars from achievement `War League Legend`, highest first.",
    "top_loot": (
        "Sorted by total lifetime loot = `Gold Grab + Elixir Escapade + Heroic Heist`, highest first."
    ),
    "rush_score": "Sorted by weighted rush score (`calculate_weighted_rush_score`), highest first.",
    "activity_score": "Sorted by activity score (`calculate_activity_score`), highest first.",
    "raid_loot": "Sorted by latest raid weekend `capitalResourcesLooted`, highest first.",
    "donation_ratio": "Sorted by `donations / max(1, donationsReceived)`, highest first.",
}
async def category_autocomplete(interaction: discord.Interaction, current: str):
    cur = (current or "").lower().strip()
    out: List[app_commands.Choice[str]] = []
    for key, label in VALID_CATEGORIES.items():
        if not cur or cur in key or cur in label.lower():
            out.append(app_commands.Choice(name=f"{label} ({key})", value=key))
    return out[:25]


class LeaderboardSwitchView(discord.ui.View):
    """Interactive view to switch leaderboard category and page."""

    def __init__(
        self,
        cog: "LeaderboardsCog",
        rows: List[Dict[str, Any]],
        clan_label: str,
        initial_category: str,
        clan_names: Optional[List[str]] = None,
        author_id: Optional[int] = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.rows = rows
        self.clan_label = clan_label
        self.author_id = author_id
        self.categories = list(VALID_CATEGORIES.keys())
        self.category_index = self.categories.index(initial_category) if initial_category in self.categories else 0
        ordered_clans = [c for c in (clan_names or []) if c]
        self.clan_options = ordered_clans[:2]
        self.clan_filter = "ALL"
        self.page_index = 0
        self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
        self._sync_buttons()
        self._update_category_select()
        self._sync_clan_buttons()

    @property
    def current_category(self) -> str:
        return self.categories[self.category_index]

    @property
    def current_clan_label(self) -> str:
        if self.clan_filter == "ALL":
            return self.clan_label
        return self.clan_filter

    def _filtered_rows(self) -> List[Dict[str, Any]]:
        if self.clan_filter == "ALL":
            return self.rows
        return [r for r in self.rows if str(r.get("clan_name", "")) == self.clan_filter]

    def _sync_buttons(self) -> None:
        self.prev_page.disabled = self.page_index <= 0
        self.next_page.disabled = self.page_index >= (len(self.pages) - 1)
        self.prev_lb.label = f"◀ LB"
        self.next_lb.label = f"LB ▶"

    def _sync_clan_buttons(self) -> None:
        self.clan1.label = self.clan_options[0] if len(self.clan_options) >= 1 else "Clan 1"
        self.clan1.disabled = len(self.clan_options) < 1

        self.clan2.label = self.clan_options[1] if len(self.clan_options) >= 2 else "Clan 2"
        self.clan2.disabled = len(self.clan_options) < 2

        self.clan_all.label = "All"

        if self.clan_filter == "ALL":
            self.clan_all.style = discord.ButtonStyle.success
            self.clan1.style = discord.ButtonStyle.secondary
            self.clan2.style = discord.ButtonStyle.secondary
        else:
            self.clan_all.style = discord.ButtonStyle.secondary
            self.clan1.style = discord.ButtonStyle.success if len(self.clan_options) >= 1 and self.clan_filter == self.clan_options[0] else discord.ButtonStyle.secondary
            self.clan2.style = discord.ButtonStyle.success if len(self.clan_options) >= 2 and self.clan_filter == self.clan_options[1] else discord.ButtonStyle.secondary

    def _update_category_select(self) -> None:
        """Update category select menu to show current selection."""
        self.category_select.options = [
            discord.SelectOption(
                label=VALID_CATEGORIES[cat],
                value=cat,
                default=(cat == self.current_category)
            )
            for cat in self.categories
        ]

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is None:
            return True
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Only the command invoker can use these leaderboard buttons.", ephemeral=True)
        return False

    @discord.ui.select(placeholder="📊 Pick a leaderboard...", min_values=1, max_values=1)
    async def category_select(self, interaction: discord.Interaction, select: discord.ui.Select) -> None:
        selected = select.values[0]
        if selected in self.categories:
            self.category_index = self.categories.index(selected)
            self.page_index = 0
            self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
            self._update_category_select()
            self._sync_buttons()
            self._sync_clan_buttons()
            await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="◀ LB", style=discord.ButtonStyle.secondary)
    async def prev_lb(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.category_index = (self.category_index - 1) % len(self.categories)
        self.page_index = 0
        self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
        self._update_category_select()
        self._sync_buttons()
        self._sync_clan_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="Prev Page", style=discord.ButtonStyle.secondary)
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.page_index > 0:
            self.page_index -= 1
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="Next Page", style=discord.ButtonStyle.primary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.page_index < len(self.pages) - 1:
            self.page_index += 1
        self._sync_buttons()
        self._sync_clan_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="LB ▶", style=discord.ButtonStyle.secondary)
    async def next_lb(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.category_index = (self.category_index + 1) % len(self.categories)
        self.page_index = 0
        self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
        self._update_category_select()
        self._sync_buttons()
        self._sync_clan_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="Clan 1", style=discord.ButtonStyle.secondary, row=2)
    async def clan1(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if len(self.clan_options) < 1:
            await interaction.response.defer()
            return
        self.clan_filter = self.clan_options[0]
        self.page_index = 0
        self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
        self._sync_buttons()
        self._sync_clan_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="Clan 2", style=discord.ButtonStyle.secondary, row=2)
    async def clan2(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if len(self.clan_options) < 2:
            await interaction.response.defer()
            return
        self.clan_filter = self.clan_options[1]
        self.page_index = 0
        self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
        self._sync_buttons()
        self._sync_clan_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)

    @discord.ui.button(label="All", style=discord.ButtonStyle.success, row=2)
    async def clan_all(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        self.clan_filter = "ALL"
        self.page_index = 0
        self.pages = self.cog._build_top_pages(self._filtered_rows(), self.current_category, self.current_clan_label)
        self._sync_buttons()
        self._sync_clan_buttons()
        await interaction.response.edit_message(embed=self.pages[self.page_index], view=self)


class LeaderboardsCog(commands.Cog, name="Leaderboards"):
    """Clan-family leaderboards and personal ranking commands."""

    def __init__(self, bot):
        self.bot = bot

    @staticmethod
    def _achievement_value(player: Dict[str, Any], achievement_name: str) -> int:
        achievements = player.get("achievements", [])
        if not isinstance(achievements, list):
            return 0
        for ach in achievements:
            if not isinstance(ach, dict):
                continue
            name = str(ach.get("name", ""))
            if achievement_name.lower() in name.lower():
                try:
                    return int(ach.get("value", 0) or 0)
                except Exception:
                    return 0
        return 0

    async def _resolve_clans(
        self,
        clan: Optional[str],
        guild_id: Optional[int] = None,
        scope: str = "guild",
    ) -> Optional[List[Dict[str, str]]]:
        from cogs.admin import resolve_clans, _normalize_tag

        scope_norm = (scope or "guild").strip().lower()
        if scope_norm not in {"guild", "family"}:
            return None

        if scope_norm == "guild":
            return resolve_clans(self.bot, clan, guild_id=guild_id)

        # Family scope: resolve against the union of monitored clans.
        family_clans = self.bot.get_all_monitored_clans()
        if not clan or clan == "ALL":
            return family_clans

        tag_norm = _normalize_tag(clan)
        for c in family_clans:
            if c.get("tag", "").upper() == tag_norm:
                return [c]
        return None

    async def _build_member_context(
        self,
        clans_to_check: List[Dict[str, str]],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Return (rows, players_cache) for leaderboard calculations."""
        rows: List[Dict[str, Any]] = []
        all_tags: List[str] = []
        clan_member_map: Dict[str, List[Dict[str, Any]]] = {}

        for clan in clans_to_check:
            members = await self.bot.get_clan_member_list(clan["tag"])
            members = members or []
            clan_member_map[clan["tag"]] = members
            for m in members:
                tag = m.get("tag")
                if tag:
                    all_tags.append(tag)

        players = await self.bot.fetch_players(all_tags)

        raid_map: Dict[str, Dict[str, Any]] = {}
        war_map: Dict[str, Dict[str, Any]] = {}

        for clan in clans_to_check:
            try:
                raid = await get_latest_raid_weekend(self.bot, clan["tag"])
                if raid and isinstance(raid.get("members"), list):
                    raid_map[clan["tag"]] = {m.get("tag"): m for m in raid.get("members", []) if isinstance(m, dict) and m.get("tag")}
            except Exception:
                raid_map[clan["tag"]] = {}

            try:
                war = await self.bot.get_current_war(clan["tag"])
                if war and war.get("state") == "inWar":
                    war_members = (war.get("clan") or {}).get("members") or []
                    war_map[clan["tag"]] = {m.get("tag"): m for m in war_members if isinstance(m, dict) and m.get("tag")}
                else:
                    war_map[clan["tag"]] = {}
            except Exception:
                war_map[clan["tag"]] = {}

        monthly_data_raw = self._load_monthly_board()
        war_stats_raw = load_war_player_stats()
        if not isinstance(war_stats_raw, dict):
            war_stats_raw = {}
        achievements_raw = load_achievements_data()
        if not isinstance(achievements_raw, dict):
            achievements_raw = {}
        current_month = get_current_month_key()

        for clan in clans_to_check:
            clan_tag = clan["tag"]
            monthly_members = (((monthly_data_raw.get(clan_tag, {}) if isinstance(monthly_data_raw, dict) else {})
                               .get(current_month, {}) if current_month else {}) or {}).get("members", {})
            for m in clan_member_map.get(clan_tag, []):
                tag = m.get("tag")
                if not tag:
                    continue
                player = players.get(tag)
                if not player:
                    continue

                raid_member = (raid_map.get(clan_tag) or {}).get(tag)
                war_member = (war_map.get(clan_tag) or {}).get(tag)

                raid_rate = None
                raid_loot = 0
                if raid_member:
                    used = int(raid_member.get("attacksUsed", 0) or 0)
                    limit = int(raid_member.get("attacksLimit", 6) or 6)
                    raid_rate = (used / limit) * 100 if limit > 0 else 0.0
                    raid_loot = int(raid_member.get("capitalResourcesLooted", 0) or 0)

                war_rate = None
                if war_member:
                    used_war = len(war_member.get("attacks", []) or [])
                    war_rate = min(100.0, (used_war / 2.0) * 100.0)

                activity = calculate_activity_score(player, war_attack_rate_pct=war_rate, raid_completion_rate_pct=raid_rate)
                hero_levels = extract_hero_levels(player)
                rush = calculate_weighted_rush_score(player)
                war_stats_row = (((war_stats_raw.get(clan_tag) or {}) if isinstance(war_stats_raw.get(clan_tag), dict) else {})
                                 .get(tag, {}))
                if not isinstance(war_stats_row, dict):
                    war_stats_row = {}
                attacks_used_total = int(war_stats_row.get("attacks_used", 0) or 0)
                attacks_possible_total = int(war_stats_row.get("total_possible_attacks", 0) or 0)
                war_participation_rate = (
                    (float(attacks_used_total) / float(attacks_possible_total)) * 100.0
                    if attacks_possible_total > 0 else 0.0
                )

                donations = int(player.get("donations", 0) or 0)
                received = int(player.get("donationsReceived", 0) or 0)
                donation_ratio = float(donations) / float(max(1, received))
                cwl_stars = self._achievement_value(player, "War League Legend")
                top_loot = (
                    self._achievement_value(player, "Gold Grab")
                    + self._achievement_value(player, "Elixir Escapade")
                    + self._achievement_value(player, "Heroic Heist")
                )

                rows.append({
                    "name": player.get("name", m.get("name", "Unknown")),
                    "tag": tag,
                    "clan_name": clan.get("name", clan_tag),
                    "donations": donations,
                    "received": received,
                    "war_stars": int(player.get("warStars", 0) or 0),
                    "trophies": int(player.get("trophies", 0) or 0),
                    "cwl_stars": int(cwl_stars),
                    "top_loot": int(top_loot),
                    "rush_score": float(rush.get("score", 0.0) if rush else 0.0),
                    "activity_score": float(activity.get("score", 0.0)),
                    "raid_loot": raid_loot,
                    "hero_levels": sum(hero_levels.values()),
                    "badge_count": len(((achievements_raw.get(tag) or {}) if isinstance(achievements_raw.get(tag), dict) else {}).get("badges", [])),
                    "rush_improvement": self._get_monthly_rush_improvement(tag, current_month),
                    "monthly_donations": int((monthly_members.get(tag) or {}).get("donations", 0) or 0),
                    "monthly_war_stars": int((monthly_members.get(tag) or {}).get("war_stars", 0) or 0),
                    "monthly_activity": float((monthly_members.get(tag) or {}).get("activity_score", 0.0) or 0.0),
                    "war_participation_rate": float(round(war_participation_rate, 2)),
                    "donation_ratio": float(round(donation_ratio, 3)),
                })

        return rows, players

    def _sort_rows(self, rows: List[Dict[str, Any]], category: str) -> List[Dict[str, Any]]:
        reverse = True
        return sorted(rows, key=lambda x: x.get(category, 0), reverse=reverse)

    @staticmethod
    def _format_value_for_category(category: str, value: Any) -> str:
        if category == "rush_improvement":
            return f"{float(value):+.2f}"
        if category == "war_participation_rate":
            return f"{float(value):.2f}%"
        if category == "donation_ratio":
            return f"{float(value):.2f}x"
        if isinstance(value, float):
            return f"{value:.2f}"
        return f"{value:,}"

    def _build_top_pages(self, rows: List[Dict[str, Any]], category: str, clan_label: str) -> List[discord.Embed]:
        sorted_rows = self._sort_rows(rows, category)
        lines: List[str] = []
        for idx, row in enumerate(sorted_rows, 1):
            value = row.get(category, 0)
            value_txt = self._format_value_for_category(category, value)
            lines.append(
                f"{idx}. **{row['name']}** `{row['tag']}` • {row['clan_name']} • **{value_txt}**"
            )

        pages = build_paginated_embeds(
            title=f"🏆 Top {VALID_CATEGORIES[category]} — {clan_label}",
            lines=lines,
            color=discord.Color.gold(),
            per_page=10,
            footer_prefix="CC2 Clash Bot • Leaderboard",
        )

        for page in pages:
            page.description = (page.description or "") + "\n\nUse controls: `Pick a leaderboard` to jump category, `LB` to cycle category, `Page` to paginate, and `Clan 1 / Clan 2 / All` to filter clan."

        if pages and category == "war_participation_rate":
            pages[0].description = (
                (pages[0].description or "")
                + "\nFormula: `attacks_used / total_possible_attacks * 100`"
            )
        if pages and category == "donation_ratio":
            pages[0].description = (
                (pages[0].description or "")
                + "\nFormula: `donations / max(1, donationsReceived)`"
            )
        if pages:
            formula = CATEGORY_FORMULAS.get(category)
            if formula:
                pages[0].description = (
                    (pages[0].description or "")
                    + f"\n\nRanking logic: {formula}"
                )
        return pages

    @staticmethod
    def _parse_history_timestamp(raw: str) -> Optional[datetime]:
        try:
            value = str(raw or "").replace("Z", "+00:00")
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _compute_rush_improvement_from_rows(rows: List[Dict[str, Any]], month_key: str) -> float:
        """Return rush score reduction within month (positive means improved)."""
        try:
            month_start = datetime.strptime(month_key, "%Y-%m").replace(tzinfo=timezone.utc)
        except Exception:
            return 0.0

        if month_start.month == 12:
            month_end = month_start.replace(year=month_start.year + 1, month=1)
        else:
            month_end = month_start.replace(month=month_start.month + 1)

        parsed: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = LeaderboardsCog._parse_history_timestamp(str(row.get("created_at", "")))
            if ts is None:
                continue
            try:
                score = float(row.get("score", 0.0) or 0.0)
            except Exception:
                continue
            parsed.append({"ts": ts, "score": score})

        if not parsed:
            return 0.0

        parsed.sort(key=lambda x: x["ts"])
        in_month = [r for r in parsed if month_start <= r["ts"] < month_end]
        if not in_month:
            return 0.0

        start_score = float(in_month[0]["score"])
        end_score = float(in_month[-1]["score"])

        if len(in_month) == 1:
            prior = [r for r in parsed if r["ts"] < month_start]
            if prior:
                start_score = float(prior[-1]["score"])

        return round(start_score - end_score, 2)

    def _get_monthly_rush_improvement(self, player_tag: str, month_key: str) -> float:
        rows = load_rush_history_for_player(player_tag, limit=250)
        if not isinstance(rows, list) or not rows:
            return 0.0
        return self._compute_rush_improvement_from_rows(rows, month_key)

    @staticmethod
    def _load_monthly_board() -> Dict[str, Any]:
        data = load_monthly_leaderboard()
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _previous_month_key(month_key: str) -> Optional[str]:
        try:
            dt = datetime.strptime(month_key, "%Y-%m")
        except Exception:
            return None
        year = dt.year
        month = dt.month - 1
        if month == 0:
            year -= 1
            month = 12
        return f"{year:04d}-{month:02d}"

    def _build_monthly_rows_for_month(self, clans_to_check: List[Dict[str, str]], month_key: str) -> List[Dict[str, Any]]:
        data = self._load_monthly_board()
        rows: List[Dict[str, Any]] = []
        for clan in clans_to_check:
            clan_tag = clan.get("tag", "")
            snap = load_leaderboard_snapshot(clan_tag, month_key)
            if not isinstance(snap, dict):
                snap = ((data.get(clan_tag, {}) if isinstance(data.get(clan_tag, {}), dict) else {})
                        .get(month_key, {}) if month_key else {}) or {}
            members = snap.get("members", {}) if isinstance(snap, dict) else {}
            if not isinstance(members, dict):
                continue
            for tag, row in members.items():
                if not isinstance(row, dict):
                    continue
                rows.append({
                    "name": row.get("name", "Unknown"),
                    "tag": tag,
                    "clan_name": clan.get("name", clan_tag),
                    "monthly_donations": int(row.get("donations", 0) or 0),
                    "monthly_war_stars": int(row.get("war_stars", 0) or 0),
                    "monthly_activity": float(row.get("activity_score", 0.0) or 0.0),
                })
        return rows

    @commands.hybrid_command(name="top", aliases=["lb"], description="Show clan leaderboard by category")
    @app_commands.checks.cooldown(1, 15.0)
    @app_commands.describe(
        category="Leaderboard category",
        clan="Clan to rank (optional, default = all monitored clans)",
        scope="guild or family",
    )
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    @app_commands.autocomplete(category=category_autocomplete, clan=clan_autocomplete)
    async def top(self, ctx: commands.Context, category: str = "donations", clan: Optional[str] = None, scope: str = "guild"):
        await ctx.defer()
        cat = (category or "").strip().lower()
        if cat not in VALID_CATEGORIES:
            return await ctx.send(f"❌ Invalid category. Choose one of: {', '.join(VALID_CATEGORIES.keys())}")

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.")

        if not clan:
            scoped = await self._resolve_clans(None, guild_id=(ctx.guild.id if ctx.guild else None), scope=scope_val)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for leaderboard",
                    include_all=True,
                )
                await ctx.send("Select a clan for leaderboard:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.")
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.")
                clan = view.selected_tag

        clans_to_check = await self._resolve_clans(clan, guild_id=(ctx.guild.id if ctx.guild else None), scope=scope_val)
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.")

        rows, _ = await self._build_member_context(clans_to_check)
        if not rows:
            return await ctx.send("No data available for leaderboard.")

        if not clan or clan == "ALL":
            clan_label = "ALL FAMILY" if scope_val == "family" else "ALL CLANS"
        else:
            clan_label = clans_to_check[0]["name"]

        pages = self._build_top_pages(rows, cat, clan_label)
        author_id = getattr(getattr(ctx, "author", None), "id", None)
        clan_names = [str(c.get("name", "")).strip() for c in clans_to_check if str(c.get("name", "")).strip()]
        view = LeaderboardSwitchView(self, rows, clan_label, cat, clan_names=clan_names, author_id=author_id)
        await ctx.send(embed=pages[0], view=view)

    @commands.hybrid_command(name="myrank", aliases=["mr"], description="Show your personal rank in a category")
    @app_commands.checks.cooldown(1, 8.0)
    @app_commands.describe(
        category="Ranking category",
        clan="Clan to rank against (optional, default = all monitored clans)",
        tag="Optional player tag override",
        scope="guild or family",
    )
    @app_commands.choices(
        scope=[
            app_commands.Choice(name="This Guild", value="guild"),
            app_commands.Choice(name="All Family", value="family"),
        ]
    )
    @app_commands.autocomplete(category=category_autocomplete, clan=clan_autocomplete)
    async def myrank(
        self,
        ctx: commands.Context,
        category: str = "donations",
        clan: Optional[str] = None,
        tag: Optional[str] = None,
        scope: str = "guild",
    ):
        await ctx.defer(ephemeral=True)
        cat = (category or "").strip().lower()
        if cat not in VALID_CATEGORIES:
            return await ctx.send(f"❌ Invalid category. Choose one of: {', '.join(VALID_CATEGORIES.keys())}", ephemeral=True)

        scope_val = scope.lower() if isinstance(scope, str) else scope.value.lower()
        if scope_val not in {"guild", "family"}:
            return await ctx.send("❌ Scope must be `guild` or `family`.", ephemeral=True)

        if not clan:
            scoped = await self._resolve_clans(None, guild_id=(ctx.guild.id if ctx.guild else None), scope=scope_val)
            if scoped:
                view = ClanSelectView(
                    scoped,
                    author_id=getattr(ctx.author, "id", None),
                    placeholder="Choose clan for myrank",
                    include_all=True,
                )
                await ctx.send("Select a clan for myrank:", view=view)
                timed_out = await view.wait()
                if timed_out or not view.selected_tag:
                    return await ctx.send("⏱️ Clan selection timed out.", ephemeral=True)
                if view.selected_tag == "NONE":
                    return await ctx.send("❌ No monitored clans available.", ephemeral=True)
                clan = view.selected_tag

        if tag:
            target_tag = normalize_tag(tag)
            if not is_valid_tag(target_tag):
                return await ctx.send("❌ Invalid tag format. Use like #2PQUE2J.", ephemeral=True)
        else:
            linked = get_linked_tag_for_user(ctx.author.id)
            if not linked:
                return await ctx.send("❌ No linked account. Use /link or pass tag.", ephemeral=True)
            target_tag = normalize_tag(linked)

        clans_to_check = await self._resolve_clans(clan, guild_id=(ctx.guild.id if ctx.guild else None), scope=scope_val)
        if clans_to_check is None:
            return await ctx.send("❌ Clan not found.", ephemeral=True)

        rows, _ = await self._build_member_context(clans_to_check)
        if not rows:
            return await ctx.send("No ranking data available.", ephemeral=True)

        sorted_rows = self._sort_rows(rows, cat)
        total = len(sorted_rows)
        rank = next((i + 1 for i, r in enumerate(sorted_rows) if r.get("tag", "").upper() == target_tag.upper()), None)
        if rank is None:
            return await ctx.send("❌ Player not found in selected clan scope.", ephemeral=True)

        row = next(r for r in sorted_rows if r.get("tag", "").upper() == target_tag.upper())
        value = row.get(cat, 0)
        value_txt = self._format_value_for_category(cat, value)

        embed = discord.Embed(
            title=f"📌 My Rank — {VALID_CATEGORIES[cat]}",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Player", value=f"**{row['name']}** `{row['tag']}`", inline=False)
        embed.add_field(name="Rank", value=f"**#{rank}** out of **{total}**", inline=True)
        embed.add_field(name="Value", value=f"**{value_txt}**", inline=True)
        formula = CATEGORY_FORMULAS.get(cat)
        if formula:
            embed.add_field(name="Ranking Logic", value=formula, inline=False)
        if not clan or clan == "ALL":
            scope_label = "ALL FAMILY" if scope_val == "family" else "ALL CLANS"
        else:
            scope_label = clans_to_check[0]["name"]
        embed.add_field(name="Clan Scope", value=scope_label, inline=False)

        # Rank delta for monthly categories using previous-month stored leaderboard data.
        if cat in {"monthly_donations", "monthly_war_stars", "monthly_activity"}:
            cur_month = get_current_month_key()
            prev_month = self._previous_month_key(cur_month)
            if prev_month:
                prev_rows = self._build_monthly_rows_for_month(clans_to_check, prev_month)
                if prev_rows:
                    prev_sorted = self._sort_rows(prev_rows, cat)
                    prev_rank = next((i + 1 for i, r in enumerate(prev_sorted) if r.get("tag", "").upper() == target_tag.upper()), None)
                    if prev_rank is not None:
                        delta = prev_rank - rank
                        if delta > 0:
                            delta_text = f"↑{delta}"
                        elif delta < 0:
                            delta_text = f"↓{abs(delta)}"
                        else:
                            delta_text = "—"
                        embed.add_field(name="Rank Delta (vs last month)", value=f"{delta_text} (#{prev_rank} → #{rank})", inline=False)
                    else:
                        embed.add_field(name="Rank Delta (vs last month)", value="New entrant (no prior month rank)", inline=False)
        embed.set_footer(text="CC2 Clash Bot • My Rank")
        await ctx.send(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(LeaderboardsCog(bot))
