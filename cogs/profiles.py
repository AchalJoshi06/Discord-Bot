"""Player profile commands, upgrade analysis, and persistent profile view."""
import logging
import re
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from embeds import build_info_embed, format_value, build_compare_embed
from calculations import (
    extract_hero_levels, calculate_hero_rush, calculate_lab_rush,
    extract_lab_total, calculate_weighted_rush_score, calculate_activity_score,
    calculate_player_streaks,
)
from config import HERO_CAPS, LAB_CAPS
from storage import (
    get_linked_tag_for_user,
    get_linked_tags_for_user,
    create_rush_history_entry,
    load_rush_history_for_player,
)
from utils.helpers import normalize_tag, is_valid_tag, ClanSelectView, build_error_embed

logger = logging.getLogger("cc2bot.cogs.profiles")


# ────────────────────────────────────────────
# Persistent View (survives bot restarts)
# ────────────────────────────────────────────

class PlayerProfileView(discord.ui.View):
    """Quick-action buttons for player profile embeds.

    `timeout=None` + hard-coded `custom_id` ensures buttons stay
    functional even after a bot restart.
    """

    def __init__(self, author_id: Optional[int] = None):
        super().__init__(timeout=None)
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is None:
            return True
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message(
            "Only the command invoker can use these profile buttons.",
            ephemeral=True,
        )
        return False

    @discord.ui.button(
        label="📱 Compact",
        style=discord.ButtonStyle.blurple,
        custom_id="persistent:profile_compact",
    )
    async def compact_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_layout(interaction, "compact")

    @discord.ui.button(
        label="🖥️ Detailed",
        style=discord.ButtonStyle.gray,
        custom_id="persistent:profile_detailed",
    )
    async def detailed_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_layout(interaction, "detailed")

    @discord.ui.button(
        label="🔄 Next Account",
        style=discord.ButtonStyle.secondary,
        custom_id="persistent:profile_next_linked",
    )
    async def next_account_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        embed = interaction.message.embeds[0] if interaction.message and interaction.message.embeds else None
        if not embed:
            return

        current_tag = self._extract_tag(embed)
        if not current_tag:
            return

        linked_tags = get_linked_tags_for_user(interaction.user.id)
        if len(linked_tags) <= 1:
            return await interaction.followup.send(
                "ℹ️ You only have one linked account. Link more accounts to switch.",
                ephemeral=True,
            )

        try:
            idx = linked_tags.index(current_tag)
            next_tag = linked_tags[(idx + 1) % len(linked_tags)]
        except ValueError:
            next_tag = linked_tags[0]

        bot = interaction.client
        player = await bot.get_player(next_tag)
        if not player:
            return await interaction.followup.send(
                f"❌ Could not fetch linked account `{next_tag}`.",
                ephemeral=True,
            )

        _detect_minion_prince(player)
        layout = self._current_layout(embed)
        new_embed = _build_profile_embed(player, next_tag, layout=layout, bot=bot)
        self._update_button_styles(layout)
        await interaction.edit_original_response(embed=new_embed, view=self)

    async def _switch_layout(self, interaction: discord.Interaction, layout: str):
        """Defer immediately, fetch fresh data, rebuild embed, edit in place."""
        await interaction.response.defer()

        embed = interaction.message.embeds[0] if interaction.message and interaction.message.embeds else None
        if not embed:
            return

        tag = self._extract_tag(embed)
        if not tag:
            return

        bot = interaction.client
        player = await bot.get_player(tag)
        if not player:
            return

        _detect_minion_prince(player)
        new_embed = _build_profile_embed(player, tag, layout=layout, bot=bot)
        self._update_button_styles(layout)
        await interaction.edit_original_response(embed=new_embed, view=self)

    # ── helpers ──
    def _extract_tag(self, embed: discord.Embed) -> Optional[str]:
        """Try to pull the player tag from embed fields, footer, or title."""
        # 1. Check fields for a "Tag" field
        for field in embed.fields:
            if "tag" in (field.name or "").lower():
                raw = (field.value or "").strip().strip("`")
                if raw.startswith("#"):
                    return raw
        # 2. Check footer (primary — tag appended after •)
        if embed.footer and embed.footer.text:
            match = re.search(r"#[A-Z0-9]+", embed.footer.text)
            if match:
                return match.group(0)
        # 3. Fallback: title (e.g. "PlayerName  #2PQUE2J")
        if embed.title:
            match = re.search(r"#[A-Z0-9]+", embed.title)
            if match:
                return match.group(0)
        return None

    def _update_button_styles(self, active: str):
        """Set the active button to blurple and the other to gray."""
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if "compact" in (item.custom_id or ""):
                    item.style = discord.ButtonStyle.blurple if active == "compact" else discord.ButtonStyle.gray
                elif "detailed" in (item.custom_id or ""):
                    item.style = discord.ButtonStyle.blurple if active == "detailed" else discord.ButtonStyle.gray

    def _current_layout(self, embed: discord.Embed) -> str:
        text = (embed.footer.text if embed.footer else "") or ""
        if "(Detailed)" in text:
            return "detailed"
        return "compact"


# ────────────────────────────────────────────
# Helper: Minion Prince detection
# ────────────────────────────────────────────

def _build_profile_embed(
    player: Dict[str, Any], tag: str, layout: str = "compact", bot: Optional[commands.Bot] = None,
) -> discord.Embed:
    """Build a full profile embed with pets field.

    Used by both the initial ``/info`` send and the Compact/Detailed
    button toggle so the embed is always identical.
    """
    streaks = None
    if layout == "detailed":
        clan_tags = None
        if bot is not None and hasattr(bot, "get_all_monitored_clans"):
            clan_tags = [c.get("tag") for c in bot.get_all_monitored_clans() if isinstance(c, dict)]
        streaks = calculate_player_streaks(tag, clan_tags=clan_tags)

    embed = build_info_embed(player, tag, layout=layout, streaks=streaks)
    # Temporarily hidden while activity scoring is being reworked.
    # activity = calculate_activity_score(player)
    # embed.add_field(name="📈 Activity", value=f"**{activity.get('score', 0):.2f}/100**", inline=True)

    pets = player.get("pets", []) or []
    if pets:
        pet_lines = [f"{p.get('name')} L{p.get('level', '?')}" for p in pets[:10]]
        embed.add_field(name="🐾 Pets", value="\n".join(pet_lines), inline=True)

    return embed


def _detect_minion_prince(player: Dict[str, Any]):
    """Return (mp_level, cleaned_player) with Minion Prince removed from 'heroes'."""
    mp_level = None
    for h in player.get("heroes", []) or []:
        if "minion prince" in (h.get("name") or "").lower():
            try:
                mp_level = int(h.get("level") or 0)
            except Exception:
                mp_level = h.get("level") or "?"
            break
    if mp_level is None:
        pets_list = player.get("pets", []) or []
        for p in list(pets_list):
            if "minion prince" in (p.get("name") or "").lower():
                try:
                    mp_level = int(p.get("level") or 0)
                except Exception:
                    mp_level = p.get("level") or "?"
                pets_list.remove(p)
                player["pets"] = pets_list
                break
    return mp_level


def _exclude_minion_prince(player: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallow copy with Minion Prince removed from 'heroes'."""
    p = dict(player)
    if isinstance(p.get("heroes"), list):
        p["heroes"] = [
            h for h in p["heroes"]
            if "minion prince" not in (h.get("name") or "").lower()
        ]
    return p


def _rush_status_band(score: float) -> tuple[str, str]:
    value = max(0.0, float(score or 0.0))
    if value <= 10.0:
        return "Very Clean", "🟢"
    if value <= 20.0:
        return "Stable", "🟡"
    if value <= 30.0:
        return "At Risk", "🟠"
    return "Rushed", "🔴"


def _rush_trend_outlook(net_change: float) -> str:
    delta = float(net_change or 0.0)
    if delta <= -5.0:
        return "Strong improvement trend"
    if delta < -0.5:
        return "Improving trend"
    if delta <= 0.5:
        return "Mostly flat trend"
    if delta < 5.0:
        return "Worsening trend"
    return "Sharp deterioration trend"


def _rush_action_hint(latest_score: float, net_change: float) -> str:
    score = max(0.0, float(latest_score or 0.0))
    delta = float(net_change or 0.0)
    if score <= 10.0 and delta <= 0.5:
        return "Maintain current pace. Use `cc2 upgradepriority` weekly to stay efficient."
    if score <= 20.0 and delta <= 0.0:
        return "Good direction. Keep hero uptime high and close top 2 priority gaps first."
    if delta > 0.5 and score <= 30.0:
        return "Trend is rising. Re-focus builders on heroes/lab and avoid early TH jumps."
    if score > 30.0:
        return "High rush risk. Run `cc2 upgradepriority` now and prioritize offense heroes before side upgrades."
    return "Steady progress. Review priorities after each snapshot to prevent backslide."


def record_rush_history_for_player(player: Dict[str, Any], clan_tag: Optional[str] = None) -> bool:
    """Persist one rush-history point from a full player payload."""
    if not isinstance(player, dict):
        return False

    player_tag = str(player.get("tag") or "").strip().upper()
    if not player_tag:
        return False

    rush = calculate_weighted_rush_score(player)
    if not isinstance(rush, dict):
        return False

    score = float(rush.get("score", 0.0) or 0.0)
    payload = {
        "name": player.get("name", "Unknown"),
        "town_hall": int(player.get("townHallLevel", 0) or 0),
        "rush": rush,
    }
    resolved_clan_tag = clan_tag
    if not resolved_clan_tag:
        cobj = player.get("clan") if isinstance(player.get("clan"), dict) else {}
        resolved_clan_tag = cobj.get("tag")

    return create_rush_history_entry(
        player_tag=player_tag,
        score=score,
        payload=payload,
        clan_tag=resolved_clan_tag,
        created_at_iso=datetime.now(timezone.utc).isoformat(),
    )


# ────────────────────────────────────────────
# Upgrade Priority Analysis
# ────────────────────────────────────────────

def get_upgrade_priority(player: Dict[str, Any]) -> Dict[str, Any]:
    """Rank hero + lab upgrades by practical priority score.

    Priority scoring blends:
    - Relative gap percent (how far behind this hero is)
    - Absolute gap pressure (missing levels)
    - Role weight (offense impact by hero)
    """
    th = player.get("townHallLevel")
    if th is None:
        return {"priority": [], "hero_gaps": {}, "lab_progress": 0}

    # Use previous-TH caps as a realistic baseline target right after TH jump.
    target_th = max(1, int(th) - 1)
    hero_caps = HERO_CAPS.get(target_th, {})
    # LAB_CAPS is Dict[int, int] (simple int value per TH)
    lab_cap_raw = LAB_CAPS.get(target_th, 0)
    lab_cap = lab_cap_raw if isinstance(lab_cap_raw, int) else (lab_cap_raw.get("total", 0) if isinstance(lab_cap_raw, dict) else 0)
    hero_levels = extract_hero_levels(player)

    hero_names = {
        "BK": "Barbarian King", "AQ": "Archer Queen",
        "GW": "Grand Warden", "RC": "Royal Champion",
        "MP": "Minion Prince",
    }

    hero_role_weight = {
        "AQ": 1.35,
        "GW": 1.30,
        "RC": 1.25,
        "MP": 1.15,
        "BK": 1.10,
    }

    hero_gaps = []
    for code, name in hero_names.items():
        current = hero_levels.get(code, 0)
        required = hero_caps.get(code, 0)
        if required > 0:
            gap = max(0, required - current)
            if gap > 0:
                pct = round((current / required) * 100, 1) if required else 0.0
                role_w = float(hero_role_weight.get(code, 1.0))
                relative_gap = (gap / required) * 100.0 if required else 0.0
                absolute_pressure = min(40.0, float(gap) * 4.0)
                score = round((0.60 * relative_gap + 0.40 * absolute_pressure) * role_w, 2)
                hero_gaps.append({
                    "code": code,
                    "name": name,
                    "current": current,
                    "required": required,
                    "gap": gap,
                    "percent": pct,
                    "score": score,
                    "relative_gap": round(relative_gap, 2),
                })
    hero_gaps.sort(key=lambda x: x.get("score", 0.0), reverse=True)

    current_lab = extract_lab_total(player)
    lab_gap = max(0, lab_cap - current_lab)
    lab_progress = round((current_lab / lab_cap) * 100, 1) if lab_cap else 0

    priority: list = []
    for hero in hero_gaps[:3]:
        reason = f"{hero['relative_gap']:.1f}% behind target"
        if hero.get("code") in {"AQ", "GW", "RC"}:
            reason += ", high offense impact"
        priority.append({
            "type": "hero",
            "icon": "🦸",
            "name": hero["name"],
            "current": hero["current"],
            "required": hero["required"],
            "gap": hero["gap"],
            "progress": hero["percent"],
            "score": hero["score"],
            "reason": reason,
        })
    if lab_gap > 0:
        # Keep lab visible, but do not overshadow core hero progression.
        lab_score = round(min(100.0, (lab_gap / max(1, lab_cap)) * 100.0) * 0.85, 2)
        priority.append({
            "type": "lab",
            "icon": "🧪",
            "name": "Lab Units",
            "current": current_lab,
            "required": lab_cap,
            "gap": lab_gap,
            "progress": lab_progress,
            "score": lab_score,
            "reason": f"{(lab_gap / max(1, lab_cap)) * 100.0:.1f}% behind target",
        })

    return {
        "priority": priority, "hero_gaps": hero_gaps,
        "lab_current": current_lab, "lab_total": lab_cap,
        "lab_progress": lab_progress,
        "target_th": target_th,
    }


# ────────────────────────────────────────────
# Clan autocomplete helper
# ────────────────────────────────────────────

async def clan_autocomplete(interaction: discord.Interaction, current: str):
    bot = interaction.client
    cur = (current or "").lower()
    opts: List[app_commands.Choice[str]] = []
    guild_id = getattr(interaction, "guild_id", None)
    scoped_clans = bot.get_scoped_clans(guild_id)
    if not cur or "all".startswith(cur):
        opts.append(app_commands.Choice(name="ALL CLANS", value="ALL"))
    for c in scoped_clans:
        label = f"{c['name']} ({c['tag']})"
        if cur in label.lower():
            opts.append(app_commands.Choice(name=label, value=c["tag"]))
    return opts[:25]


# ────────────────────────────────────────────
# Cog
# ────────────────────────────────────────────

class ProfilesCog(commands.Cog, name="Profiles"):
    """Player info, rush analysis, upgrade priority."""

    def __init__(self, bot):
        self.bot = bot

    # ── shared logic ──
    async def _resolve_tag(self, ctx: commands.Context, raw: Optional[str]) -> Optional[str]:
        """Resolve a tag from arg, mention, or linked account."""
        if raw:
            raw = raw.strip()
            # mention?
            match = re.match(r"<@!?(\d+)>", raw)
            if match:
                uid = int(match.group(1))
                linked = get_linked_tag_for_user(uid)
                if not linked:
                    await ctx.send(
                        embed=build_error_embed(
                            "E-TAG-NOLINK",
                            "That user has no linked player tag.",
                            "Ask them to run `/link #TAG` first, or provide a direct tag.",
                        ),
                        ephemeral=True,
                    )
                    return None
                tag = normalize_tag(linked)
                if not is_valid_tag(tag):
                    await ctx.send(
                        embed=build_error_embed(
                            "E-TAG-BADLINK",
                            "Linked player tag is invalid.",
                            "Re-link the account with `/link #TAG`.",
                            context=f"resolved_tag={tag}",
                        ),
                        ephemeral=True,
                    )
                    return None
                return tag
            tag = normalize_tag(raw)
            if not is_valid_tag(tag):
                await ctx.send(
                    embed=build_error_embed(
                        "E-TAG-FORMAT",
                        "Invalid player tag format.",
                        "Use format like `#2PQUE2J`.",
                        context=f"input={raw}",
                    ),
                    ephemeral=True,
                )
                return None
            return tag
        linked = get_linked_tag_for_user(ctx.author.id)
        if not linked:
            await ctx.send(
                embed=build_error_embed(
                    "E-TAG-MISSING",
                    "No tag provided and no linked account found.",
                    "Use `/link #TAG` first, or provide a tag argument.",
                ),
                ephemeral=True,
            )
            return None
        tag = normalize_tag(linked)
        if not is_valid_tag(tag):
            await ctx.send(
                embed=build_error_embed(
                    "E-TAG-LINKINVALID",
                    "Your linked player tag is invalid.",
                    "Re-link with `/link #TAG`.",
                    context=f"linked_tag={tag}",
                ),
                ephemeral=True,
            )
            return None
        return tag

    async def _send_player_profile(self, ctx: commands.Context, tag: str, layout: str = "compact"):
        """Fetch player data and send profile embed with persistent view.
        
        Args:
            ctx: Command context
            tag: Player tag (resolved)
            layout: "compact" (default) or "detailed"
        """
        player = await self.bot.get_player(tag)
        if not player:
            return await ctx.send(
                embed=build_error_embed(
                    "E-API-PLAYER",
                    "Could not fetch player data from the Clash API.",
                    "Retry in a few seconds, or confirm the tag is valid.",
                    context=f"tag={tag}",
                )
            )
        _detect_minion_prince(player)
        record_rush_history_for_player(player)

        embed = _build_profile_embed(player, tag, layout=layout, bot=self.bot)
        view = PlayerProfileView(author_id=ctx.author.id)
        await ctx.send(embed=embed, view=view)

    # ═══════════════════════════════════
    # /info  +  cc2 info
    # ═══════════════════════════════════
    @commands.hybrid_command(name="info", aliases=["i"], description="Get detailed player info + rush analysis")
    @app_commands.describe(tag="Player tag (e.g. #2PQUE2J) — optional; uses linked tag if omitted")
    async def info(self, ctx: commands.Context, *, tag: Optional[str] = None):
        await ctx.defer()
        resolved = await self._resolve_tag(ctx, tag)
        if not resolved:
            return
        await self._send_player_profile(ctx, resolved)

    @commands.hybrid_command(name="profile", aliases=["pf"], description="Alias of /info")
    @app_commands.describe(tag="Player tag (e.g. #2PQUE2J) — optional; uses linked tag if omitted")
    async def profile(self, ctx: commands.Context, *, tag: Optional[str] = None):
        await ctx.defer()
        resolved = await self._resolve_tag(ctx, tag)
        if not resolved:
            return
        await self._send_player_profile(ctx, resolved)

    # ═══════════════════════════════════
    # /p  +  cc2 p c/d [tag]
    # ═══════════════════════════════════
    @commands.command(name="p", description="Quick profile lookup: cc2 p [c|d] [tag]")
    async def p(self, ctx: commands.Context, *, args: Optional[str] = None):
        """Profile shortcut with optional layout: c=compact, d=detailed.
        
        Usage:
          cc2 p              — compact, linked tag
          cc2 p c            — compact, linked tag
          cc2 p d            — detailed, linked tag
          cc2 p #TAG        — compact, specific tag
          cc2 p c #TAG      — compact, specific tag
          cc2 p d #TAG      — detailed, specific tag
        """
        await ctx.defer()
        layout = "compact"
        tag = None
        
        if args:
            parts = args.split(maxsplit=1)
            if parts[0].lower() in ("c", "d"):
                layout = "detailed" if parts[0].lower() == "d" else "compact"
                tag = parts[1] if len(parts) > 1 else None
            else:
                tag = args
        
        resolved = await self._resolve_tag(ctx, tag)
        if not resolved:
            return
        await self._send_player_profile(ctx, resolved, layout=layout)

    # ═══════════════════════════════════
    # /upgradepriority  +  cc2 upgradepriority
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="upgradepriority", aliases=["upg"],
        description="Show upgrade priorities for a player (heroes + lab)",
    )
    @app_commands.describe(tag="Player tag — optional; uses linked tag if omitted")
    async def upgradepriority(self, ctx: commands.Context, *, tag: Optional[str] = None):
        await ctx.defer(ephemeral=True)
        resolved = await self._resolve_tag(ctx, tag)
        if not resolved:
            return

        player = await self.bot.get_player(resolved)
        if not player:
            return await ctx.send(f"❌ Could not fetch player `{resolved}`.")

        analysis = get_upgrade_priority(player)
        priority = analysis["priority"]
        player_name = player.get("name", "Unknown")
        th_level = player.get("townHallLevel", "?")

        embed = discord.Embed(
            title=f"📈 Upgrade Priority — {player_name}",
            description=f"TH{th_level} — Smart priority queue for next upgrades",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="🆔 Player Tag", value=f"`{resolved}`", inline=True)
        embed.add_field(name="🏰 Town Hall", value=f"**TH {th_level}**", inline=True)
        embed.add_field(name="🎯 Baseline", value=f"TH{analysis.get('target_th', th_level)} cap target", inline=True)

        if priority:
            lines = []
            for idx, u in enumerate(priority[:5], 1):
                bar_w = 10
                filled = int((u["progress"] / 100) * bar_w)
                bar = "█" * filled + "░" * (bar_w - filled)
                lines.append(
                    f"{idx}. {u['icon']} **{u['name']}**\n"
                    f"   `{u['current']}/{u['required']}` — **{u['progress']}%** [{bar}]\n"
                    f"   📌 Missing: **{u['gap']}** levels | Priority: **{float(u.get('score', 0.0)):.1f}**\n"
                    f"   💡 {u.get('reason', 'Needs upgrades') }"
                )
            embed.add_field(name="⭐ Priority Upgrades", value="\n".join(lines), inline=False)
        else:
            embed.add_field(
                name="✅ All Caught Up!",
                value="Maxed heroes and lab for this TH level!",
                inline=False,
            )

        summary = []
        if analysis["hero_gaps"]:
            summary.append(f"🦸 Heroes: **{sum(h['gap'] for h in analysis['hero_gaps'])}** levels behind")
        lab_gap = analysis["lab_total"] - analysis["lab_current"]
        if lab_gap > 0:
            summary.append(f"🧪 Lab: **{lab_gap}** levels behind")
        if summary:
            embed.add_field(name="📊 Total Gap", value=" | ".join(summary), inline=False)
        embed.set_footer(text="CC2 Clash Bot • Upgrade Priority (weighted by role + gap severity)")
        await ctx.send(embed=embed)

    # ═══════════════════════════════════
    # /compare
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="compare", aliases=["cmp"],
        description="Compare two players side-by-side",
    )
    @app_commands.describe(
        tag_a="First player tag",
        tag_b="Second player tag",
    )
    async def compare(self, ctx: commands.Context, tag_a: str, tag_b: str):
        await ctx.defer()
        ta = normalize_tag(tag_a)
        tb = normalize_tag(tag_b)
        if not is_valid_tag(ta) or not is_valid_tag(tb):
            return await ctx.send("❌ Invalid tag format. Use tags like #2PQUE2J.")
        if ta.upper() == tb.upper():
            return await ctx.send("❌ Please provide two different player tags.")

        pa = await self.bot.get_player(ta)
        pb = await self.bot.get_player(tb)
        if not pa or not pb:
            return await ctx.send("❌ Could not fetch one or both players from the API.")

        emb = build_compare_embed(pa, ta, pb, tb)
        await ctx.send(embed=emb)

    # ═══════════════════════════════════
    # /rushhistory
    # ═══════════════════════════════════
    @commands.hybrid_command(
        name="rushhistory", aliases=["rhs"],
        description="Show rush score trend history for a player",
    )
    @app_commands.describe(
        tag="Player tag (optional; defaults to linked tag)",
        limit="Number of entries to show (default 10, max 30)",
    )
    async def rushhistory(self, ctx: commands.Context, *, tag: Optional[str] = None, limit: int = 10):
        await ctx.defer()
        resolved = await self._resolve_tag(ctx, tag)
        if not resolved:
            return

        lim = max(3, min(limit, 30))
        rows = load_rush_history_for_player(resolved, limit=lim)
        if not rows:
            return await ctx.send("No rush history tracked yet. Run `/info` or `cc2 takesnapshot` first.")

        ordered = list(reversed(rows))
        first_score = float(ordered[0].get("score", 0.0) or 0.0)
        latest_score = float(ordered[-1].get("score", 0.0) or 0.0)
        net_change = latest_score - first_score
        status_label, status_icon = _rush_status_band(latest_score)
        trend_outlook = _rush_trend_outlook(net_change)
        action_hint = _rush_action_hint(latest_score, net_change)

        step_changes: List[float] = []
        prev_for_avg: Optional[float] = None
        for row in ordered:
            s = float(row.get("score", 0.0) or 0.0)
            if prev_for_avg is not None:
                step_changes.append(s - prev_for_avg)
            prev_for_avg = s
        avg_step = (sum(step_changes) / float(len(step_changes))) if step_changes else 0.0

        latest_payload = ordered[-1].get("payload", {}) if isinstance(ordered[-1].get("payload", {}), dict) else {}
        player_name = latest_payload.get("name") or ordered[-1].get("player_tag", resolved)

        emb = discord.Embed(
            title=f"📉 Rush History — {player_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Player Tag", value=f"`{resolved}`", inline=True)
        emb.add_field(name="Latest Rush Score", value=f"**{latest_score:.2f}%**", inline=True)
        emb.add_field(
            name="Net Change",
            value=(f"📉 **{net_change:.2f}%**" if net_change < 0 else f"📈 **+{net_change:.2f}%**"),
            inline=True,
        )
        emb.add_field(name="Status", value=f"{status_icon} **{status_label}**", inline=True)
        emb.add_field(name="Trend Outlook", value=f"**{trend_outlook}**", inline=True)
        emb.add_field(
            name="Avg Step Change",
            value=(f"📉 **{avg_step:.2f}% / entry**" if avg_step < 0 else f"📈 **+{avg_step:.2f}% / entry**"),
            inline=True,
        )
        emb.add_field(name="Recommended Next Step", value=action_hint, inline=False)

        lines: List[str] = []
        prev_score: Optional[float] = None
        for row in ordered:
            score = float(row.get("score", 0.0) or 0.0)
            created_raw = str(row.get("created_at", ""))
            ts = created_raw.replace("T", " ").replace("+00:00", " UTC")
            delta = ""
            if prev_score is not None:
                diff = score - prev_score
                if diff < 0:
                    delta = f" (↓{abs(diff):.2f})"
                elif diff > 0:
                    delta = f" (↑{diff:.2f})"
                else:
                    delta = " (→0.00)"
            lines.append(f"• **{score:.2f}%**{delta} — {ts}")
            prev_score = score

        emb.add_field(name=f"Timeline ({len(ordered)} entries)", value="\n".join(lines[:20]), inline=False)
        emb.set_footer(text="Lower rush score is better")
        await ctx.send(embed=emb)

async def setup(bot):
    await bot.add_cog(ProfilesCog(bot))
