"""Weekly challenge automation and challenge status commands."""
import logging
from typing import Dict, Any, List
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks

from storage import load_challenges_data, save_challenges_data

logger = logging.getLogger("cc2bot.cogs.challenges")

CHALLENGE_TEMPLATES: List[Dict[str, Any]] = [
    {
        "id": "donation_push",
        "type": "donations",
        "goal": 10000,
        "title": "Donation Drive",
        "description": "This week's challenge: donate 10,000 troops as a clan family.",
    },
    {
        "id": "war_star_push",
        "type": "war_stars",
        "goal": 200,
        "title": "War Star Push",
        "description": "This week's challenge: earn 200 war stars across tracked clans.",
    },
    {
        "id": "trophy_push",
        "type": "trophies",
        "goal": 50000,
        "title": "Trophy Climb",
        "description": "This week's challenge: reach a combined 50,000 trophies.",
    },
]


_METRIC_LABELS = {
    "donations": "Donations",
    "war_stars": "War Stars",
    "trophies": "Trophies",
}


def _metric_label(metric: str) -> str:
    return _METRIC_LABELS.get(str(metric or "").lower(), str(metric or "Unknown"))


def _progress_bar(progress: int, goal: int, width: int = 12) -> str:
    if goal <= 0:
        return "░" * width
    ratio = max(0.0, min(float(progress) / float(goal), 1.0))
    filled = int(round(ratio * width))
    return ("█" * filled) + ("░" * (width - filled))


def _days_left_in_week(now: datetime) -> int:
    # Monday=0, Sunday=6; include today so this stays actionable.
    return max(1, 7 - int(now.weekday()))


def _next_steps(metric: str, remaining: int) -> str:
    if remaining <= 0:
        return "✅ Goal reached. Keep contributing to build margin for the weekly result post."

    metric = str(metric or "").lower()
    if metric == "donations":
        return (
            "Ask members to fill requests quickly and keep queues active during peak hours. "
            "Encourage high-capacity donors to chain support requests."
        )
    if metric == "war_stars":
        return (
            "Prioritize planned attacks and cleanup hits to maximize stars. "
            "Remind unused attackers before war close."
        )
    if metric == "trophies":
        return (
            "Push active players to maintain win streaks and avoid risky trophy losses. "
            "Coordinate shield/guard usage for stable gains."
        )
    return "Coordinate the clan to close the remaining gap before week end."


def _week_key(now: datetime) -> str:
    year, week_num, _ = now.isocalendar()
    return f"{year}-W{week_num:02d}"


def _load_challenges() -> Dict[str, Any]:
    data = load_challenges_data()
    return data if isinstance(data, dict) else {}


def _save_challenges(data: Dict[str, Any]) -> None:
    save_challenges_data(data)


class ChallengesCog(commands.Cog, name="Challenges"):
    """Posts weekly challenges and progress updates automatically."""

    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        self.challenge_loop.start()

    async def cog_unload(self):
        self.challenge_loop.cancel()

    def _ensure_week_challenge(self, now: datetime) -> Dict[str, Any]:
        data = _load_challenges()
        wk = _week_key(now)
        if wk in data:
            return data[wk]

        idx = abs(hash(wk)) % len(CHALLENGE_TEMPLATES)
        tpl = dict(CHALLENGE_TEMPLATES[idx])
        tpl.update(
            {
                "week": wk,
                "created_at": now.isoformat(),
                "posted": False,
                "friday_update_sent": False,
                "sunday_result_sent": False,
            }
        )
        data[wk] = tpl
        _save_challenges(data)
        return tpl

    async def _calculate_progress(self, challenge: Dict[str, Any]) -> int:
        metric = challenge.get("type")
        total = 0
        for c in self.bot.get_all_monitored_clans():
            members = await self.bot.get_clan_member_list(c["tag"])
            if not members:
                continue
            tags = [m.get("tag") for m in members if m.get("tag")]
            players = await self.bot.fetch_players(tags)
            for p in players.values():
                if not p:
                    continue
                if metric == "donations":
                    total += int(p.get("donations", 0) or 0)
                elif metric == "war_stars":
                    total += int(p.get("warStars", 0) or 0)
                elif metric == "trophies":
                    total += int(p.get("trophies", 0) or 0)
        return total

    async def _send_challenge_embed(self, channel, challenge: Dict[str, Any], phase: str, progress: int):
        goal = int(challenge.get("goal", 0) or 0)
        pct = (progress / goal * 100.0) if goal > 0 else 0.0
        pct = min(999.0, pct)
        remaining = max(0, goal - progress)
        metric = str(challenge.get("type", "N/A"))
        metric_name = _metric_label(metric)
        now = datetime.now(timezone.utc)
        days_left = _days_left_in_week(now)
        pace_needed = (remaining / float(days_left)) if remaining > 0 else 0.0
        bar = _progress_bar(progress, goal)

        if phase == "start":
            title = f"🎯 Weekly Challenge — {challenge.get('title', 'Challenge')}"
            desc = (
                f"{challenge.get('description', '')}\n\n"
                "Start of week plan is live. Track progress daily and adjust participation early."
            )
            color = discord.Color.blurple()
        elif phase == "update":
            title = f"📣 Weekly Challenge Update — {challenge.get('title', 'Challenge')}"
            desc = "Mid-week checkpoint. Focus effort on the remaining gap to stay on pace."
            color = discord.Color.orange()
        else:
            title = f"🏁 Weekly Challenge Result — {challenge.get('title', 'Challenge')}"
            success = goal > 0 and progress >= goal
            desc = "Challenge completed! Great work. ✅" if success else "Challenge not completed this week."
            color = discord.Color.green() if success else discord.Color.red()

        emb = discord.Embed(title=title, description=desc, color=color, timestamp=now)
        emb.add_field(name="Week", value=challenge.get("week", "N/A"), inline=True)
        emb.add_field(name="Metric", value=metric_name, inline=True)
        emb.add_field(name="Goal", value=f"{goal:,}", inline=True)
        emb.add_field(name="Current", value=f"{progress:,}", inline=True)
        emb.add_field(name="Remaining", value=f"{remaining:,}", inline=True)
        emb.add_field(name="Progress", value=f"{bar}  {pct:.1f}%", inline=False)
        if remaining > 0:
            emb.add_field(
                name="Weekly Pace Needed",
                value=f"~{pace_needed:,.0f} {metric_name.lower()} per day for the next {days_left} day(s)",
                inline=False,
            )
        emb.add_field(name="What To Do Now", value=_next_steps(metric, remaining), inline=False)
        emb.set_footer(text="CC2 Clash Bot • Weekly Challenge")
        await channel.send(embed=emb)

    @tasks.loop(minutes=30)
    async def challenge_loop(self):
        now = datetime.now(timezone.utc)
        challenge = self._ensure_week_challenge(now)

        channels = await self.bot.get_all_announce_channels()
        if not channels:
            return

        data = _load_challenges()
        wk = challenge.get("week")
        if not wk or wk not in data:
            return

        row = data[wk]
        progress = await self._calculate_progress(row)

        # Monday post
        if now.weekday() == 0 and not row.get("posted", False):
            for channel in channels:
                await self._send_challenge_embed(channel, row, "start", progress)
            row["posted"] = True

        # Friday update
        if now.weekday() == 4 and not row.get("friday_update_sent", False):
            for channel in channels:
                await self._send_challenge_embed(channel, row, "update", progress)
            row["friday_update_sent"] = True

        # Sunday result (after 18:00 UTC)
        if now.weekday() == 6 and now.hour >= 18 and not row.get("sunday_result_sent", False):
            for channel in channels:
                await self._send_challenge_embed(channel, row, "result", progress)
            row["sunday_result_sent"] = True

        data[wk] = row
        _save_challenges(data)

    @challenge_loop.before_loop
    async def before_loop(self):
        await self.bot.wait_until_ready()

    @commands.hybrid_command(name="challenge", aliases=["ch"], description="Show current weekly challenge status")
    async def challenge(self, ctx: commands.Context):
        await ctx.defer()
        now = datetime.now(timezone.utc)
        challenge = self._ensure_week_challenge(now)
        progress = await self._calculate_progress(challenge)

        goal = int(challenge.get("goal", 0) or 0)
        pct = (progress / goal * 100.0) if goal > 0 else 0.0
        remaining = max(0, goal - progress)
        metric = str(challenge.get("type", "N/A"))
        metric_name = _metric_label(metric)
        days_left = _days_left_in_week(now)
        pace_needed = (remaining / float(days_left)) if remaining > 0 else 0.0
        bar = _progress_bar(progress, goal)
        status = "✅ On Track" if remaining == 0 else ("⚠️ Needs Push" if pct < 70 else "🟡 In Progress")

        emb = discord.Embed(
            title=f"🎯 Weekly Challenge — {challenge.get('title', 'Challenge')}",
            description=challenge.get("description", ""),
            color=discord.Color.blurple(),
            timestamp=now,
        )
        emb.add_field(name="Week", value=challenge.get("week", "N/A"), inline=True)
        emb.add_field(name="Metric", value=metric_name, inline=True)
        emb.add_field(name="Status", value=status, inline=True)
        emb.add_field(name="Goal", value=f"{goal:,}", inline=True)
        emb.add_field(name="Current", value=f"{progress:,}", inline=True)
        emb.add_field(name="Remaining", value=f"{remaining:,}", inline=True)
        emb.add_field(name="Progress", value=f"{bar}  {pct:.1f}%", inline=False)
        if remaining > 0:
            emb.add_field(
                name="Weekly Pace Needed",
                value=f"~{pace_needed:,.0f} {metric_name.lower()} per day for {days_left} day(s)",
                inline=False,
            )
        emb.add_field(name="What To Do Now", value=_next_steps(metric, remaining), inline=False)
        emb.set_footer(text="CC2 Clash Bot • Challenge")
        await ctx.send(embed=emb)


async def setup(bot):
    await bot.add_cog(ChallengesCog(bot))
