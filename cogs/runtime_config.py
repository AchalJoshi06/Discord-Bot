"""Runtime configuration commands backed by settings persistence."""
import logging
from typing import Dict, Any

import discord
import requests
from discord import app_commands
from discord.ext import commands

from config import LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID
from storage import load_settings, save_settings, load_guild_settings, save_guild_settings, get_effective_setting
from utils.helpers import has_leadership_role

logger = logging.getLogger("cc2bot.cogs.runtime_config")

_ALLOWED_KEYS = {
    "raid_reminder_enabled": "bool",
    "raid_dm_reminder_enabled": "bool",
    "war_reminder_enabled": "bool",
    "onboarding_dm_enabled": "bool",
    "kick_review_day": "int",
    "monthly_snapshot_day": "int",
    "inactive_days_threshold": "int",
    "announce_channel_id": "channel",
}


async def config_key_autocomplete(interaction: discord.Interaction, current: str):
    cur = (current or "").strip().lower()
    out = []
    for key in sorted(_ALLOWED_KEYS.keys()):
        if not cur or cur in key:
            out.append(app_commands.Choice(name=key, value=key))
    return out[:25]


async def config_scope_autocomplete(interaction: discord.Interaction, current: str):
    cur = (current or "").strip().lower()
    scopes = ["guild", "global", "effective"]
    out = [s for s in scopes if (not cur or cur in s)]
    return [app_commands.Choice(name=s, value=s) for s in out[:3]]


def _to_bool(text: str):
    val = (text or "").strip().lower()
    if val in {"1", "true", "yes", "on", "enable", "enabled"}:
        return True
    if val in {"0", "false", "no", "off", "disable", "disabled"}:
        return False
    return None


class RuntimeConfigCog(commands.Cog, name="RuntimeConfig"):
    """Manage runtime settings with persistent storage."""

    def __init__(self, bot):
        self.bot = bot

    def _is_leadership(self, member) -> bool:
        return has_leadership_role(member, LEADERSHIP_ROLE_ID, BOT_ADMIN_ROLE_ID)

    @commands.hybrid_group(name="config", aliases=["cfg"], description="View and update bot runtime settings", invoke_without_command=True)
    async def config(self, ctx: commands.Context):
        if not self._is_leadership(ctx.author):
            return await ctx.send("❌ Leadership role required for this command.")

        settings = load_settings()
        guild_settings = load_guild_settings(ctx.guild.id) if ctx.guild else {}
        emb = discord.Embed(
            title="⚙️ Runtime Settings",
            color=discord.Color.purple(),
            description="Use `cc2 config set <key> <value> [scope]` where scope is `guild` or `global`.",
        )
        for key in _ALLOWED_KEYS:
            gval = guild_settings.get(key, "(unset)") if ctx.guild else "(n/a)"
            val = settings.get(key, "(unset)")
            emb.add_field(name=key, value=f"global={val} | guild={gval}", inline=False)
        await ctx.send(embed=emb)

    @config.command(name="set", aliases=["cset"])
    @app_commands.describe(key="Setting key", value="New value", scope="Scope: guild or global")
    @app_commands.autocomplete(key=config_key_autocomplete, scope=config_scope_autocomplete)
    async def config_set(self, ctx: commands.Context, key: str, value: str, scope: str = "guild"):
        if not self._is_leadership(ctx.author):
            return await ctx.send("❌ Leadership role required for this command.")

        key = (key or "").strip().lower()
        scope = (scope or "guild").strip().lower()
        kind = _ALLOWED_KEYS.get(key)
        if not kind:
            allowed = ", ".join(sorted(_ALLOWED_KEYS.keys()))
            return await ctx.send(f"❌ Unknown key. Allowed: {allowed}")
        if scope not in {"guild", "global"}:
            return await ctx.send("❌ Scope must be `guild` or `global`.")
        if scope == "guild" and not ctx.guild:
            return await ctx.send("❌ Guild scope can only be used in a server channel.")

        parsed: Any
        if kind == "bool":
            parsed = _to_bool(value)
            if parsed is None:
                return await ctx.send("❌ Invalid boolean. Use true/false, on/off, yes/no, 1/0.")
        elif kind == "int":
            try:
                parsed = int(value)
            except Exception:
                return await ctx.send("❌ Value must be an integer.")
            if key == "monthly_snapshot_day":
                parsed = max(1, min(28, parsed))
            if key == "inactive_days_threshold":
                parsed = max(1, min(90, parsed))
            if key == "kick_review_day":
                parsed = max(0, min(6, parsed))
        else:  # channel
            v = (value or "").strip()
            if v.startswith("<#") and v.endswith(">"):
                v = v[2:-1]
            try:
                parsed = int(v)
            except Exception:
                return await ctx.send("❌ Provide a channel mention like #general or a channel ID.")

        if scope == "global":
            settings: Dict[str, Any] = load_settings()
            settings[key] = parsed
            ok = save_settings(settings)
        else:
            ok = save_guild_settings(ctx.guild.id, {key: parsed}, merge=True)
        if not ok:
            return await ctx.send("❌ Failed to save setting.")

        # Apply runtime-sensitive global settings immediately when possible.
        if scope == "global" and key == "raid_reminder_enabled":
            self.bot.raid_reminder_enabled = bool(parsed)
        elif scope == "global" and key == "war_reminder_enabled":
            self.bot.war_reminder_enabled = bool(parsed)

        await ctx.send(f"✅ Saved `{key}` = `{parsed}` ({scope})")

    @config.command(name="get", aliases=["cget"])
    @app_commands.describe(key="Setting key", scope="Scope: guild, global, or effective")
    @app_commands.autocomplete(key=config_key_autocomplete, scope=config_scope_autocomplete)
    async def config_get(self, ctx: commands.Context, key: str, scope: str = "effective"):
        if not self._is_leadership(ctx.author):
            return await ctx.send("❌ Leadership role required for this command.")

        key = (key or "").strip().lower()
        scope = (scope or "effective").strip().lower()
        if key not in _ALLOWED_KEYS:
            allowed = ", ".join(sorted(_ALLOWED_KEYS.keys()))
            return await ctx.send(f"❌ Unknown key. Allowed: {allowed}")
        if scope not in {"guild", "global", "effective"}:
            return await ctx.send("❌ Scope must be `guild`, `global`, or `effective`.")
        if scope == "guild" and not ctx.guild:
            return await ctx.send("❌ Guild scope can only be used in a server channel.")

        settings = load_settings()
        guild_settings = load_guild_settings(ctx.guild.id) if ctx.guild else {}
        if scope == "global":
            val = settings.get(key, "(unset)")
        elif scope == "guild":
            val = guild_settings.get(key, "(unset)")
        else:
            val = get_effective_setting(key, default="(unset)", guild_id=(ctx.guild.id if ctx.guild else None))
        await ctx.send(f"⚙️ `{key}` = `{val}` ({scope})")

    @commands.command(name="botip")
    async def check_bot_ip(self, ctx: commands.Context):
        """Check the public IP address of the bot (for CoC API whitelisting)."""
        try:
            async with ctx.typing():
                response = requests.get("https://api.ipify.org?format=json", timeout=5)
                if response.status_code == 200:
                    ip_data = response.json()
                    public_ip = ip_data.get("ip", "Unable to determine")
                    embed = discord.Embed(
                        title="🌐 Bot Public IP Address",
                        description=f"```\n{public_ip}\n```",
                        color=discord.Color.blue()
                    )
                    embed.add_field(
                        name="Use This For:",
                        value="Whitelist in Clash of Clans API settings at https://developer.clashofclans.com",
                        inline=False
                    )
                    embed.set_footer(text="CoC API Token → Edit → Whitelist this IP")
                    await ctx.send(embed=embed)
                else:
                    await ctx.send("❌ Failed to retrieve IP address. Try again later.")
        except Exception as e:
            logger.error(f"Error checking bot IP: {e}")
            await ctx.send(f"❌ Error: {str(e)}")


async def setup(bot):
    await bot.add_cog(RuntimeConfigCog(bot))
