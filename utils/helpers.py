"""Centralized utility functions shared across cogs.

Consolidates: tag normalization, mention parsing, embed helpers,
time formatting, and common validation patterns.
"""
import math
import re
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import discord

logger = logging.getLogger("cc2bot.utils.helpers")


# ════════════════════════════════════════════
# Tag & Input Normalization
# ════════════════════════════════════════════

def normalize_tag(tag: str) -> str:
    """Normalize a Clash of Clans player/clan tag.

    - Strips whitespace
    - Uppercases
    - Ensures leading '#'
    - Handles '##' double-hash edge case
    """
    tag = (tag or "").strip().upper()
    # Remove all leading # then add exactly one
    tag = tag.lstrip("#")
    if tag:
        tag = "#" + tag
    return tag


def is_valid_tag(tag: str) -> bool:
    """Check if a tag looks like a valid CoC tag (# + alphanumeric)."""
    return bool(re.match(r"^#[0-9A-Z]{3,12}$", tag.upper()))


# ════════════════════════════════════════════
# Mention Parsing
# ════════════════════════════════════════════

_MENTION_RE = re.compile(r"<@!?(\d+)>")


def extract_mention_id(text: str) -> Optional[int]:
    """Extract Discord user ID from a mention string like <@123> or <@!123>.

    Returns None if the text is not a valid mention.
    """
    match = _MENTION_RE.match(text.strip())
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def is_mention(text: str) -> bool:
    """Check if text is a Discord mention."""
    return _MENTION_RE.match(text.strip()) is not None


# ════════════════════════════════════════════
# Tag Extraction from Embeds
# ════════════════════════════════════════════

_TAG_RE = re.compile(r"(#[0-9A-Za-z]{3,12})")


def extract_tag_from_embed(embed: discord.Embed) -> Optional[str]:
    """Extract a player tag from an embed using 3-step fallback.

    1. Check embed fields for one named 'Tag'
    2. Search footer text for #TAG pattern
    3. Search title text for #TAG pattern (fallback for older embeds)

    Returns the first valid tag found, or None.
    """
    # Step 1: explicit "Tag" field
    if embed.fields:
        for field in embed.fields:
            if field.name and "tag" in field.name.lower():
                match = _TAG_RE.search(field.value or "")
                if match:
                    return match.group(1).upper()

    # Step 2: footer text
    if embed.footer and embed.footer.text:
        match = _TAG_RE.search(embed.footer.text)
        if match:
            return match.group(1).upper()

    # Step 3: title text (fallback)
    if embed.title:
        match = _TAG_RE.search(embed.title)
        if match:
            return match.group(1).upper()

    return None


# ════════════════════════════════════════════
# Embed Helpers
# ════════════════════════════════════════════

SEPARATOR = "`─────────────────`"

# Discord limits
EMBED_FIELD_VALUE_LIMIT = 1024
EMBED_DESCRIPTION_LIMIT = 4096
EMBED_TITLE_LIMIT = 256


def truncate(text: str, max_len: int = EMBED_FIELD_VALUE_LIMIT, suffix: str = "…") -> str:
    """Safely truncate text to fit within Discord embed limits.

    If text exceeds max_len, cuts at the last newline before the limit
    (to avoid cutting mid-line) and appends the suffix.
    """
    if not text or len(text) <= max_len:
        return text or ""
    cutoff = max_len - len(suffix)
    # Try to cut at a line boundary
    last_nl = text.rfind("\n", 0, cutoff)
    if last_nl > cutoff // 2:
        return text[:last_nl] + suffix
    return text[:cutoff] + suffix


def add_separator(embed: discord.Embed) -> discord.Embed:
    """Add a visual separator field to an embed."""
    embed.add_field(name="\u200b", value=SEPARATOR, inline=False)
    return embed


def format_value(value: int) -> str:
    """Format large numbers with K/M suffixes for readability.

    Examples: 1500 → '1.5K', 2500000 → '2.5M', 500 → '500'
    """
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return str(value)


def make_progress_bar(current: int, maximum: int, length: int = 10) -> str:
    """Create a text progress bar like [████░░░░░░] 40%."""
    if maximum <= 0:
        return f"[{'░' * length}] 0%"
    pct = min(current / maximum, 1.0)
    filled = int(pct * length)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {pct * 100:.0f}%"


def build_error_embed(
    code: str,
    problem: str,
    recovery: str,
    context: Optional[str] = None,
) -> discord.Embed:
    """Build a consistent typed error embed with a recovery hint."""
    emb = discord.Embed(
        title=f"❌ {str(code or 'ERROR').strip().upper()}",
        description=str(problem or "An error occurred."),
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )
    emb.add_field(name="How to fix", value=str(recovery or "Try again."), inline=False)
    if context:
        emb.add_field(name="Context", value=truncate(str(context), 900), inline=False)
    emb.set_footer(text="CC2 Clash Bot • Error")
    return emb


# ════════════════════════════════════════════
# Safe Discord Sending
# ════════════════════════════════════════════

MESSAGE_CONTENT_LIMIT = 2000


async def safe_send(ctx_or_channel: Any, text: str, **kwargs) -> Any:
    """Send a message, splitting into <=2000-char chunks if needed.

    Splits on newlines to avoid cutting mid-line.
    """
    if len(text) <= MESSAGE_CONTENT_LIMIT:
        return await ctx_or_channel.send(text, **kwargs)

    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > MESSAGE_CONTENT_LIMIT:
            if current:
                chunks.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line
    if current:
        chunks.append(current)

    for chunk in chunks:
        await ctx_or_channel.send(chunk, **kwargs)


class PaginatedEmbedView(discord.ui.View):
    """Simple Previous/Next pagination for a list of embeds."""

    def __init__(self, embeds: List[discord.Embed], author_id: Optional[int] = None):
        super().__init__(timeout=180)
        self.embeds = embeds
        self.author_id = author_id
        self.index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        self.prev_button.disabled = self.index <= 0
        self.next_button.disabled = self.index >= (len(self.embeds) - 1)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is None:
            return True
        if interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Only the command invoker can use these pagination buttons.", ephemeral=True)
        return False

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.index > 0:
            self.index -= 1
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.index], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if self.index < len(self.embeds) - 1:
            self.index += 1
        self._sync_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.index], view=self)


class _ClanSelect(discord.ui.Select):
    def __init__(self, options: List[discord.SelectOption], placeholder: str = "Select a clan"):
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, ClanSelectView):
            return
        if view.author_id is not None and interaction.user.id != view.author_id:
            await interaction.response.send_message("Only the command invoker can use this selector.", ephemeral=True)
            return

        view.selected_tag = self.values[0]
        for child in view.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=f"Selected clan: **{view.selected_tag}**",
            view=view,
        )
        view.stop()


class ClanSelectView(discord.ui.View):
    """Reusable clan dropdown selector for commands with optional clan arguments."""

    def __init__(
        self,
        clans: List[Dict[str, str]],
        author_id: Optional[int] = None,
        placeholder: str = "Select a clan",
        include_all: bool = False,
    ):
        super().__init__(timeout=60)
        self.author_id = author_id
        self.selected_tag: Optional[str] = None

        options: List[discord.SelectOption] = []
        seen: set[str] = set()
        if include_all:
            options.append(discord.SelectOption(label="All Clans", value="ALL", description="Run command across all clans"))
            seen.add("ALL")

        for c in clans or []:
            if not isinstance(c, dict):
                continue
            tag = normalize_tag(str(c.get("tag", "")))
            if not tag or tag in seen:
                continue
            seen.add(tag)
            name = str(c.get("name", tag))
            options.append(discord.SelectOption(label=name[:100], value=tag, description=tag))

        if not options:
            options.append(discord.SelectOption(label="No clans available", value="NONE", description="No monitored clans found"))

        self.add_item(_ClanSelect(options, placeholder=placeholder))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.author_id is None or interaction.user.id == self.author_id:
            return True
        await interaction.response.send_message("Only the command invoker can use this selector.", ephemeral=True)
        return False


def build_paginated_embeds(
    title: str,
    lines: List[str],
    color: discord.Color = discord.Color.blurple(),
    per_page: int = 15,
    footer_prefix: str = "CC2 Clash Bot",
) -> List[discord.Embed]:
    """Build paginated embeds from line items."""
    if not lines:
        return [discord.Embed(title=title, description="No data.", color=color)]

    pages: List[discord.Embed] = []
    total_pages = max(1, math.ceil(len(lines) / per_page))
    for i in range(total_pages):
        chunk = lines[i * per_page:(i + 1) * per_page]
        emb = discord.Embed(title=title, description="\n".join(chunk), color=color)
        emb.set_footer(text=f"{footer_prefix} • Page {i + 1}/{total_pages}")
        pages.append(emb)
    return pages


async def send_paginated_embeds(ctx: Any, embeds: List[discord.Embed]) -> Any:
    """Send either a single embed or interactive paginated view."""
    if not embeds:
        return await ctx.send("No data.")
    if len(embeds) == 1:
        return await ctx.send(embed=embeds[0])

    author_id = getattr(getattr(ctx, "author", None), "id", None)
    view = PaginatedEmbedView(embeds, author_id=author_id)
    return await ctx.send(embed=embeds[0], view=view)


def has_leadership_role(member: Any, leadership_role_id: int = 0, bot_admin_role_id: int = 0) -> bool:
    """Check if a member is guild admin or has configured leadership/bot-admin role."""
    if member is None:
        return False

    perms = getattr(member, "guild_permissions", None)
    if perms and (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
        return True

    role_ids = {getattr(r, "id", 0) for r in getattr(member, "roles", [])}
    configured = {rid for rid in (leadership_role_id, bot_admin_role_id) if int(rid or 0) > 0}
    return bool(role_ids.intersection(configured))


def has_admin_role(member: Any, bot_admin_role_id: int = 0) -> bool:
    """Check if a member is guild admin or has configured bot-admin role."""
    if member is None:
        return False

    perms = getattr(member, "guild_permissions", None)
    if perms and (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
        return True

    role_ids = {getattr(r, "id", 0) for r in getattr(member, "roles", [])}
    if int(bot_admin_role_id or 0) > 0:
        return bot_admin_role_id in role_ids
    return False


async def audit_log(
    bot: Any,
    action: str,
    actor: Any,
    details: str,
    audit_channel_id: int,
) -> None:
    """Send an audit embed to the configured audit channel if available."""
    if not bot or int(audit_channel_id or 0) <= 0:
        return
    try:
        channel = bot.get_channel(audit_channel_id) or await bot.fetch_channel(audit_channel_id)
        if not channel:
            return
        actor_name = getattr(actor, "display_name", None) or getattr(actor, "name", "Unknown")
        actor_id = getattr(actor, "id", "N/A")

        emb = discord.Embed(
            title="🧾 Bot Audit Log",
            color=discord.Color.purple(),
            timestamp=utc_now(),
        )
        emb.add_field(name="Action", value=action, inline=False)
        emb.add_field(name="Actor", value=f"{actor_name} (`{actor_id}`)", inline=False)
        emb.add_field(name="Details", value=truncate(details, max_len=1024), inline=False)
        emb.set_footer(text="CC2 Clash Bot • Audit")
        await channel.send(embed=emb)
    except Exception as e:
        logger.debug("Audit log failed for action %s: %s", action, e)


# ════════════════════════════════════════════
# Time Formatting
# ════════════════════════════════════════════

def utc_now() -> datetime:
    """Get current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration.

    Examples: 3661 → '1h 1m', 86400 → '1d 0h', 45 → '45s'
    """
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    remaining_min = minutes % 60
    if hours < 24:
        return f"{hours}h {remaining_min}m"
    days = hours // 24
    remaining_hours = hours % 24
    return f"{days}d {remaining_hours}h"


# ════════════════════════════════════════════
# Resolve Clans (moved here to avoid circular imports in future)
# ════════════════════════════════════════════

def resolve_clans(bot: Any, clan_arg: Optional[str]) -> Optional[List[Dict[str, str]]]:
    """Return list of clan dicts matching *clan_arg*, or all clans if None/'ALL'.

    Returns ``None`` when a specific tag was given but not found.
    """
    if not clan_arg or clan_arg.upper() == "ALL":
        return list(bot.clans)
    tag_norm = normalize_tag(clan_arg)
    for c in bot.clans:
        if c["tag"].upper() == tag_norm:
            return [c]
    return None
