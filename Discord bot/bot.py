"""
Refactored Discord bot for Clash of Clans clan management.

Improvements:
- Modular structure (config, api, storage, calculations, embeds, trackers)
- API caching with TTL to reduce calls
- Request deduplication to prevent concurrent duplicate requests
- Optimized API calls (batch where possible)
- Better error handling and async practices
- Improved decision logic with explanations
- Environment variable support for secrets
"""
import asyncio
import io
import csv
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import discord
from discord import app_commands
import aiohttp

from config import (
    DISCORD_TOKEN, COC_API_KEY, CHANNEL_ID, LOG_CHANNEL_ID, ANNOUNCE_CHANNEL_ID,
    TH_COLORS, BASE_TYPES
)
from storage import (
    load_clans, save_clans, load_links, save_links, get_linked_tag_for_user,
    load_bases, save_bases, load_strict_cache
)
from coc_api import COCAPI
from calculations import calculate_hero_rush, calculate_lab_rush, analyze_player_for_kick
from embeds import build_join_embed, build_info_embed, build_leave_embed, _bold_upper
from trackers import ClanTracker, fixed_time_reminder_loop
from donations import (
    extract_lifetime_donations, create_donation_snapshot, save_monthly_snapshot,
    calculate_monthly_donations, get_donation_history, get_player_donation_stats,
    get_current_month_key
)


# ============================
# DISCORD CLIENT
# ============================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True


class ClashBot(discord.Client):
    """Main Discord bot client."""
    
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.coc_api: Optional[COCAPI] = None
        self.clan_trackers: Dict[str, ClanTracker] = {}
        self.clans: List[Dict[str, str]] = load_clans()
        self._tasks_started = False
    
    async def setup_hook(self):
        """Called when bot is starting up."""
        self.http_session = aiohttp.ClientSession()
        self.coc_api = COCAPI(self.http_session)
    
    async def close(self):
        """Cleanup on shutdown."""
        if self.http_session:
            await self.http_session.close()
        await super().close()
    
    async def log(self, msg: str):
        """Log message to console and Discord channel."""
        print(msg)
        if LOG_CHANNEL_ID:
            try:
                ch = self.get_channel(LOG_CHANNEL_ID) or await self.fetch_channel(LOG_CHANNEL_ID)
                await ch.send(f"[LOG {datetime.now().isoformat()}] {msg}")
            except Exception:
                pass


client = ClashBot(intents=intents)


# ============================
# CLAN MANAGEMENT
# ============================
def get_clan_by_tag(tag: str) -> Optional[Dict[str, str]]:
    """Find clan by tag in monitored list."""
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm
    for c in client.clans:
        if c["tag"].upper() == tag_norm:
            return c
    return None


def start_clan_tracking(clan: Dict[str, str]):
    """Start tracking tasks for a clan."""
    clan_tag = clan["tag"]
    if clan_tag in client.clan_trackers:
        return  # Already tracking
    
    tracker = ClanTracker(client, client.coc_api, clan)
    client.clan_trackers[clan_tag] = tracker
    
    # Start all tracking tasks
    asyncio.create_task(tracker.track_joins_leaves())
    asyncio.create_task(tracker.track_war())
    asyncio.create_task(tracker.check_hero_upgrades())
    asyncio.create_task(tracker.track_upgrades())


def stop_clan_tracking(clan_tag: str):
    """Stop tracking tasks for a clan."""
    tracker = client.clan_trackers.get(clan_tag)
    if tracker:
        tracker.stop()
        del client.clan_trackers[clan_tag]


# ============================
# AUTocomplete
# ============================
async def clan_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocomplete for clan selection."""
    current_lower = current.lower()
    options: List[app_commands.Choice[str]] = []
    for c in client.clans:
        label = f"{c['name']} ({c['tag']})"
        if current_lower in label.lower():
            options.append(app_commands.Choice(name=label, value=c["tag"]))
    return options[:25]


# ============================
# SLASH COMMANDS
# ============================
@client.tree.command(name="link", description="Link your Discord account to a Clash player tag.")
@app_commands.describe(tag="Your player tag (example: #2PQUE2J)")
async def link(interaction: discord.Interaction, tag: str):
    """Link Discord account to Clash player tag."""
    await interaction.response.send_message("🔗 Linking your tag...", ephemeral=True)
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm
    
    links = load_links()
    links[tag_norm] = str(interaction.user.id)
    save_links(links)
    
    emb = discord.Embed(
        title="Account Linked ✅",
        color=0x2ecc71,
        timestamp=datetime.now(timezone.utc)
    )
    emb.add_field(name="Discord User", value=f"{interaction.user.mention}", inline=True)
    emb.add_field(name="Player Tag", value=f"`{tag_norm}`", inline=True)
    emb.set_footer(text="Use /info to get player info anytime.")
    
    await interaction.edit_original_response(content="🔗 Linked!", embed=emb)


@client.tree.command(name="info", description="Get detailed player info + rush analysis")
@app_commands.describe(tag="Player tag (example: #2PQUE2J)")
async def info(interaction: discord.Interaction, tag: str):
    """Get detailed player information."""
    await interaction.response.send_message("🔎 Fetching player info...", ephemeral=True)
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm
    
    player = await client.coc_api.get_player(tag_norm)
    if not player:
        await interaction.edit_original_response(content="❌ Could not fetch player. Check tag or API.")
        return
    
    embed = build_info_embed(player, tag_norm)
    # Exclude Minion Prince from rush calculations for backward compatibility
    player_for_rush = dict(player)
    if isinstance(player_for_rush.get('heroes'), list):
        player_for_rush['heroes'] = [h for h in player_for_rush['heroes'] if 'minion prince' not in (h.get('name') or '').lower()]
    hero_res = calculate_hero_rush(player_for_rush)
    lab_res = calculate_lab_rush(player)
    
    # Add compact Rush Status (minimal, two lines)
    rush_info = []
    if hero_res:
        status = "Rushed" if hero_res['counted'] else "OK"
        rush_info.append(f"Hero Rush: {hero_res['percent']:.2f}% ({status})")

    if lab_res:
        status = "Rushed" if lab_res['counted'] else "OK"
        rush_info.append(f"Lab Rush: {lab_res['percent']:.2f}% ({status})")

    if rush_info:
        # spacer before rush status (exactly one blank line between sections)
        embed.add_field(name="\u200b", value="\u200b", inline=False)
        embed.add_field(
            name=f"⚡ {_bold_upper('RUSH STATUS')}",
            value="\n".join(rush_info),
            inline=False
        )
    
    await interaction.edit_original_response(content="✅ Done — player info below.")
    await interaction.followup.send(embed=embed)


@client.tree.command(name="roster", description="Export clan roster CSV")
@app_commands.describe(clan="Clan to export")
@app_commands.autocomplete(clan=clan_autocomplete)
async def roster(interaction: discord.Interaction, clan: str):
    """Export clan roster as CSV."""
    await interaction.response.send_message("📤 Building roster...", ephemeral=True)
    clan_obj = get_clan_by_tag(clan)
    if not clan_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return
    
    members = await client.coc_api.get_clan_members(clan_obj["tag"])
    if not members:
        await interaction.edit_original_response(content="❌ Could not fetch clan or clan is empty.")
        return
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["name", "tag", "townHall", "expLevel", "trophies", "role"])
    for m in members:
        writer.writerow([
            m.get("name"),
            m.get("tag"),
            m.get("townHallLevel"),
            m.get("expLevel"),
            m.get("trophies"),
            m.get("role")
        ])
    
    output.seek(0)
    bio = io.BytesIO(output.getvalue().encode())
    bio.name = f"roster_{clan_obj['tag'].replace('#', '')}.csv"
    
    await interaction.edit_original_response(content="✅ Roster ready — check attachment.")
    await interaction.followup.send(file=discord.File(bio, filename=bio.name), ephemeral=True)


@client.tree.command(name="status", description="Show bot status and stats")
async def status(interaction: discord.Interaction):
    """Show bot status."""
    await interaction.response.send_message("⏳ Gathering status...", ephemeral=True)
    u = client.user
    guilds = len(client.guilds)
    now = datetime.now(timezone.utc).isoformat()
    
    from cache import api_cache
    cache_stats = api_cache.get_stats()
    
    # Count registered commands
    try:
        synced_commands = await client.tree.fetch_commands()
        command_count = len(synced_commands)
    except Exception:
        command_count = "Unknown"
    
    text = (
        f"**Bot:** {u}\n"
        f"**Guilds:** {guilds}\n"
        f"**Time:** {now}\n"
        f"**Commands registered:** {command_count}\n"
        f"**Monitored clans:** {len(client.clans)}\n"
        f"**Active trackers:** {len(client.clan_trackers)}\n"
        f"**Cache entries:** {cache_stats.get('total_keys', 0)}\n"
        f"**Clans:** {', '.join([c['name'] for c in client.clans])}"
    )
    await interaction.edit_original_response(content=text)


@client.tree.command(name="synccommands", description="Force sync slash commands with Discord (Elder)")
async def synccommands(interaction: discord.Interaction):
    """Force sync slash commands."""
    await interaction.response.send_message("🔄 Syncing commands with Discord...", ephemeral=True)
    
    try:
        synced = await client.tree.sync()
        command_names = [cmd.name for cmd in synced]
        
        embed = discord.Embed(
            title="✅ Commands Synced",
            color=0x2ecc71,
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(
            name="Synced Commands",
            value=f"**{len(synced)}** commands registered",
            inline=False
        )
        embed.add_field(
            name="Command List",
            value="\n".join([f"• `/{name}`" for name in sorted(command_names)])[:1024],
            inline=False
        )
        embed.set_footer(text="Commands may take 1-2 minutes to appear in Discord")
        
        await interaction.edit_original_response(content="✅ Commands synced:", embed=embed)
        print(f"[SYNC] Manually synced {len(synced)} commands")
    except Exception as e:
        await interaction.edit_original_response(
            content=f"❌ Failed to sync commands:\n```\n{e}\n```"
        )
        print(f"[SYNC ERROR] {e}")


@client.tree.command(name="clearbot", description="Delete recent bot messages in this channel")
@app_commands.describe(limit="How many recent messages to check (max 500)")
async def clearbot(interaction: discord.Interaction, limit: int = 200):
    """Delete bot messages from channel."""
    await interaction.response.send_message("🧹 Cleaning bot messages…", ephemeral=True)
    
    if limit > 500:
        limit = 500
    
    channel = interaction.channel
    deleted = 0
    
    try:
        async for msg in channel.history(limit=limit):
            if msg.author.id == client.user.id:
                try:
                    await msg.delete()
                    deleted += 1
                except Exception:
                    pass
        
        await interaction.edit_original_response(
            content=f"🧹 Deleted **{deleted}** bot messages in <#{channel.id}>."
        )
    except Exception as e:
        await interaction.edit_original_response(
            content=f"❌ Error while deleting messages:\n```\n{e}\n```"
        )


@client.tree.command(name="whohavenotattacked", description="Show players who haven't attacked in current war")
@app_commands.describe(clan="(Optional) Select a clan; if empty, checks all")
@app_commands.autocomplete(clan=clan_autocomplete)
async def whohavenotattacked(interaction: discord.Interaction, clan: Optional[str] = None):
    """Show players who haven't used war attacks."""
    await interaction.response.send_message("🔎 Checking war status...", ephemeral=True)
    out_lines: List[str] = []
    
    clans_to_check = client.clans
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return
    
    for c in clans_to_check:
        war = await client.coc_api.get_current_war(c["tag"])
        if not war or war.get("state") != "inWar":
            continue
        
        members = (war.get("clan") or {}).get("members") or []
        pending = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]
        
        if pending:
            out_lines.append(f"**{c['name']}** — {len(pending)} pending")
            out_lines += [f"• {p.get('name')} `{p.get('tag')}`" for p in pending[:50]]
    
    if not out_lines:
        await interaction.edit_original_response(content="✅ No ongoing war or everyone attacked.")
    else:
        text = "\n".join(out_lines)
        await interaction.edit_original_response(content="📋 Results ready (ephemeral).")
        await interaction.followup.send(text, ephemeral=True)


@client.tree.command(
    name="kicksuggestions",
    description="Show players who might be candidates for kicking (with detailed analysis)"
)
@app_commands.describe(clan="(Optional) clan to check; default = all")
@app_commands.autocomplete(clan=clan_autocomplete)
async def kicksuggestions(interaction: discord.Interaction, clan: Optional[str] = None):
    """Show kick suggestions with detailed analysis."""
    await interaction.response.send_message("🔎 Building kick suggestions...", ephemeral=True)
    output: List[str] = []
    
    clans_to_check = client.clans
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return
    
    for c in clans_to_check:
        clan_name = c["name"]
        clan_tag = c["tag"]
        
        # Fetch war data once per clan (optimization)
        war = await client.coc_api.get_current_war(clan_tag)
        
        # Fetch all members
        members = await client.coc_api.get_clan_members(clan_tag)
        if not members:
            continue
        
        # Analyze each member
        suggestions: List[Dict[str, Any]] = []
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            
            player = await client.coc_api.get_player(tag)
            if not player:
                continue
            
            # Use improved analysis function
            analysis = analyze_player_for_kick(player, war, clan_tag)
            
            if analysis["should_kick"]:
                suggestions.append({
                    "name": player.get("name", tag),
                    "tag": tag,
                    "analysis": analysis
                })
        
        # Sort by score (most problematic first)
        suggestions.sort(key=lambda x: x["analysis"]["score"], reverse=True)
        
        if suggestions:
            lines = [f"**{clan_name}:** ({len(suggestions)} suggestions)"]
            for s in suggestions[:20]:  # Limit to top 20
                reasons = ", ".join(s["analysis"]["reasons"])
                score = s["analysis"]["score"]
                lines.append(f"• {s['name']} `{s['tag']}` — {reasons} (score: {score})")
            output.append("\n".join(lines))
    
    if not output:
        await interaction.edit_original_response(content="✅ No kick suggestions. Clan looks good!")
    else:
        await interaction.edit_original_response(content="📋 Kick suggestions ready (ephemeral).")
        await interaction.followup.send("\n\n".join(output), ephemeral=True)


@client.tree.command(name="raidsleft", description="Show players who did NOT finish capital raid attacks")
@app_commands.describe(clan="(Optional) clan to check; default = all")
@app_commands.autocomplete(clan=clan_autocomplete)
async def raidsleft(interaction: discord.Interaction, clan: Optional[str] = None):
    """Show players with incomplete capital raid attacks."""
    await interaction.response.send_message("🔎 Checking capital raid status...", ephemeral=True)
    out: List[str] = []
    
    clans_to_check = client.clans
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return
    
    for c in clans_to_check:
        raid = await client.coc_api.get_capital_raid_season(c["tag"])
        if not raid:
            continue
        
        members = raid.get("members", [])
        not_used = [
            m for m in members
            if (m.get("attacksUsed", 0) < (m.get("attacksLimit") or 6))
        ]
        
        if not_used:
            out.append(f"**{c['name']} — Missing Attacks:**")
            for p in not_used:
                out.append(
                    f"• {p.get('name')} `{p.get('tag')}` — "
                    f"{p.get('attacksUsed', 0)}/{p.get('attacksLimit', 6)}"
                )
    
    if not out:
        await interaction.edit_original_response(content="✅ Everyone completed raid attacks!")
    else:
        await interaction.edit_original_response(content="🔔 Raid report ready (ephemeral).")
        await interaction.followup.send("\n".join(out), ephemeral=True)


@client.tree.command(
    name="upgradecheck",
    description="Show players upgrading at least N heroes in a clan (or all clans)."
)
@app_commands.describe(
    min_heroes="Minimum heroes upgrading (0 = diagnostic / everyone)",
    clan="(Optional) clan to check; default = all"
)
@app_commands.autocomplete(clan=clan_autocomplete)
async def upgradecheck(
    interaction: discord.Interaction,
    min_heroes: int = 1,
    clan: Optional[str] = None
):
    """Check for players upgrading heroes."""
    await interaction.response.send_message("🔎 Scanning hero upgrades...", ephemeral=True)
    
    if min_heroes < 0:
        min_heroes = 0
    
    clans_to_check = client.clans
    if clan:
        c_obj = get_clan_by_tag(clan)
        if c_obj:
            clans_to_check = [c_obj]
        else:
            await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
            return
    
    lines: List[str] = []
    diag_lines: List[str] = []
    total_checked = 0
    total_hits = 0
    
    for c in clans_to_check:
        members = await client.coc_api.get_clan_members(c["tag"])
        if not members:
            continue
        
        for m in members:
            tag = m.get("tag")
            if not tag:
                continue
            
            player = await client.coc_api.get_player(tag)
            if not player:
                continue
            
            total_checked += 1
            
            # Count upgrading heroes
            upgrading_count = 0
            upgrading_names: List[str] = []
            for h in player.get("heroes", []) or []:
                ut = h.get("upgradeTimeLeft")
                if ut is not None and ut not in (0, "0", ""):
                    upgrading_count += 1
                    next_level = (h.get("level") or 0) + 1
                    upgrading_names.append(f"{h.get('name')} → L{next_level}")
            
            diag_lines.append(
                f"{c['name']} • {player.get('name')} ({tag}) — "
                f"detected {upgrading_count} upgrading hero(oes)"
            )
            
            if upgrading_count >= min_heroes:
                total_hits += 1
                if upgrading_names:
                    details = "\n    " + "\n    ".join(upgrading_names)
                else:
                    details = ""
                lines.append(
                    f"**{c['name']}** — {player.get('name')} `{tag}` — "
                    f"{upgrading_count} hero(oes) upgrading{details}"
                )
    
    if min_heroes == 0:
        if not diag_lines:
            await interaction.edit_original_response(
                content="✅ Diagnostic complete: no members checked (empty clans / API failure)."
            )
            return
        msg = (
            f"✅ Diagnostic — hero upgrade detection (0+ heroes)\n"
            f"Checked {total_checked} members.\n\n"
            + "\n".join(diag_lines[:50])
        )
        await interaction.edit_original_response(content=msg)
        return
    
    if not lines:
        await interaction.edit_original_response(
            content=(
                f"✅ No players upgrading **{min_heroes}** or more heroes "
                f"(checked {total_checked} members across {len(clans_to_check)} clan(s))."
            )
        )
    else:
        header = (
            f"⬆️ Upgrade Check — **{min_heroes}+ Heroes**\n"
            f"Matched **{total_hits}** players (checked {total_checked} members)."
        )
        body = "\n\n".join(lines[:50])
        await interaction.edit_original_response(content="📋 Upgrade check ready (ephemeral).")
        await interaction.followup.send(f"{header}\n\n{body}", ephemeral=True)


# Base commands
def _normalize_tag(tag: str) -> str:
    """Normalize player tag format."""
    tag = (tag or "").strip().upper()
    if tag and not tag.startswith("#"):
        tag = "#" + tag
    return tag


@client.tree.command(name="setbase", description="Save a base link for your account")
@app_commands.describe(
    base_type="Type of base (war, legend, anti2, blizzard).",
    link="Clash of Clans base link.",
    name="Name/label for this base (e.g. Anti 2 Ring #1).",
    tag="(Optional) Player tag, if you are not linked with /link."
)
@app_commands.choices(
    base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
    ]
)
async def setbase(
    interaction: discord.Interaction,
    base_type: app_commands.Choice[str],
    link: str,
    name: str,
    tag: str | None = None
):
    """Save a base link."""
    await interaction.response.send_message("📥 Saving base...", ephemeral=True)
    
    if tag:
        tag_norm = _normalize_tag(tag)
    else:
        tag_norm = get_linked_tag_for_user(interaction.user.id)
        if not tag_norm:
            await interaction.edit_original_response(
                content="❌ You are not linked yet. Use `/link` or provide a `tag:` in this command."
            )
            return
    
    link = link.strip()
    if not link:
        await interaction.edit_original_response(content="❌ Please provide a valid base link.")
        return
    
    bases = load_bases()
    player_bases = bases.get(tag_norm, {})
    t = base_type.value
    
    entry_list = player_bases.get(t, [])
    if not isinstance(entry_list, list):
        entry_list = []
    
    entry = {
        "name": name,
        "link": link,
        "addedBy": str(interaction.user.id),
        "addedAt": datetime.now(timezone.utc).isoformat(),
    }
    entry_list.append(entry)
    player_bases[t] = entry_list
    bases[tag_norm] = player_bases
    save_bases(bases)
    
    emb = discord.Embed(
        title="✅ Base Saved",
        color=0x2ecc71,
        timestamp=datetime.now(timezone.utc)
    )
    emb.add_field(name="Player Tag", value=tag_norm, inline=True)
    emb.add_field(name="Type", value=t, inline=True)
    emb.add_field(name="Name", value=name, inline=False)
    emb.add_field(name="Link", value=link, inline=False)
    emb.set_footer(text="Use /getbase to retrieve it later.")
    
    await interaction.edit_original_response(content="✅ Base saved!", embed=emb)


@client.tree.command(name="getbase", description="Get a base link (latest) for a given type.")
@app_commands.describe(
    base_type="Type of base (war, legend, anti2, blizzard).",
    tag="(Optional) Player tag; if omitted, uses your linked account."
)
@app_commands.choices(
    base_type=[
        app_commands.Choice(name="War", value="war"),
        app_commands.Choice(name="Legend", value="legend"),
        app_commands.Choice(name="Anti-2", value="anti2"),
        app_commands.Choice(name="Blizzard", value="blizzard"),
    ]
)
async def getbase(
    interaction: discord.Interaction,
    base_type: app_commands.Choice[str],
    tag: str | None = None
):
    """Get a saved base link."""
    await interaction.response.send_message("📤 Fetching base...", ephemeral=True)
    
    if tag:
        tag_norm = _normalize_tag(tag)
    else:
        tag_norm = get_linked_tag_for_user(interaction.user.id)
        if not tag_norm:
            await interaction.edit_original_response(
                content="❌ You are not linked yet. Use `/link` or provide a `tag:` in this command."
            )
            return
    
    bases = load_bases()
    player_bases = bases.get(tag_norm, {})
    t = base_type.value
    entries = player_bases.get(t, [])
    
    if not entries:
        await interaction.edit_original_response(
            content=f"⚠️ No `{t}` bases saved for `{tag_norm}`."
        )
        return
    
    entry = entries[-1]
    emb = discord.Embed(
        title=f"🏰 {t.capitalize()} Base — {entry.get('name', 'Unnamed')}",
        color=0x3498db,
        timestamp=datetime.now(timezone.utc)
    )
    emb.add_field(name="Player Tag", value=tag_norm, inline=True)
    emb.add_field(name="Base Name", value=entry.get("name", "Unnamed"), inline=False)
    emb.add_field(name="Link", value=entry.get("link", "(missing)"), inline=False)
    emb.set_footer(text=f"{len(entries)} {t} bases saved; showing latest.")
    
    await interaction.edit_original_response(content="✅ Base fetched:", embed=emb)


@client.tree.command(name="basebook", description="Show all saved bases for your account")
@app_commands.describe(tag="(Optional) Player tag; if omitted, uses your linked account.")
async def basebook(interaction: discord.Interaction, tag: str | None = None):
    """Show all saved bases."""
    await interaction.response.send_message("📚 Building base book...", ephemeral=True)
    
    if tag:
        tag_norm = _normalize_tag(tag)
    else:
        tag_norm = get_linked_tag_for_user(interaction.user.id)
        if not tag_norm:
            await interaction.edit_original_response(
                content="❌ You are not linked yet. Use `/link` or provide a `tag:` in this command."
            )
            return
    
    bases = load_bases()
    player_bases = bases.get(tag_norm, {})
    
    if not player_bases:
        await interaction.edit_original_response(
            content=f"⚠️ No bases saved for `{tag_norm}`."
        )
        return
    
    lines = []
    for t, entries in player_bases.items():
        if not isinstance(entries, list) or not entries:
            continue
        lines.append(f"**{t.capitalize()} Bases:**")
        for e in entries[:10]:
            nm = e.get("name", "Unnamed")
            lk = e.get("link", "(missing link)")
            lines.append(f"• **{nm}** → {lk}")
        lines.append("")
    
    if not lines:
        await interaction.edit_original_response(
            content=f"⚠️ No bases saved for `{tag_norm}`."
        )
        return
    
    text = "\n".join(lines)
    await interaction.edit_original_response(
        content=f"📚 **Base Book for `{tag_norm}`**\n\n{text}"
    )


@client.tree.command(name="syncroles", description="Sync TH roles (TH1–TH18) for all linked players.")
@app_commands.choices(
    clan_tag=[
        app_commands.Choice(name=clan["name"], value=clan["tag"])
        for clan in load_clans()
    ] + [app_commands.Choice(name="ALL CLANS", value="ALL")]
)
@app_commands.describe(clan_tag="Choose a clan or ALL.")
async def syncroles(interaction: discord.Interaction, clan_tag: app_commands.Choice[str]):
    """Sync Town Hall roles for linked players."""
    if interaction.guild is None:
        await interaction.response.send_message(
            "❌ Use this inside a server, not in DMs.",
            ephemeral=True
        )
        return
    
    await interaction.response.send_message("🔄 Syncing TH roles…", ephemeral=True)
    
    guild = interaction.guild
    links = load_links()
    
    if clan_tag.value == "ALL":
        target_clans = [c["tag"] for c in client.clans]
    else:
        target_clans = [clan_tag.value]
    
    updated_count = 0
    created_count = 0
    
    for ctag in target_clans:
        members = await client.coc_api.get_clan_members(ctag)
        if not members:
            continue
        
        for m in members:
            clash_tag = _normalize_tag(m.get("tag", ""))
            th = m.get("townHallLevel")
            if not clash_tag or not th:
                continue
            
            discord_id = links.get(clash_tag)
            if not discord_id:
                continue
            
            member = guild.get_member(int(discord_id))
            if not member:
                continue
            
            role_name = f"TH{th}"
            desired_role = discord.utils.get(guild.roles, name=role_name)
            
            if desired_role is None:
                color_value = TH_COLORS.get(th, 0x95A5A6)
                try:
                    desired_role = await guild.create_role(
                        name=role_name,
                        color=discord.Color(color_value),
                        reason="CC2 Clash Bot auto-create TH Role"
                    )
                    created_count += 1
                except Exception:
                    continue
            
            if desired_role not in member.roles:
                try:
                    await member.add_roles(desired_role, reason="TH Sync update")
                    updated_count += 1
                except Exception:
                    pass
    
    msg = (
        f"🏰 **TH Role Sync Complete**\n"
        f"👤 Updated: **{updated_count}** members\n"
        f"🆕 Created: **{created_count}** new TH roles\n"
        f"📌 Multi-account support enabled\n"
    )
    await interaction.edit_original_response(content=msg)


@client.tree.command(name="addclan", description="Add a new clan to the monitored list.")
@app_commands.describe(name="Clan name (any label you want)", tag="Clan tag (example: #PQUCURCQ)")
async def addclan(interaction: discord.Interaction, name: str, tag: str):
    """Add a clan to monitoring."""
    await interaction.response.send_message("➕ Adding clan...", ephemeral=True)
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm
    
    if get_clan_by_tag(tag_norm):
        await interaction.edit_original_response(
            content=f"❌ Clan with tag `{tag_norm}` is already in the list."
        )
        return
    
    # Validate clan exists
    clan_data = await client.coc_api.get_clan(tag_norm)
    if not clan_data:
        await interaction.edit_original_response(
            content=f"❌ Could not validate clan tag `{tag_norm}` via API."
        )
        return
    
    display_name = name.strip() or clan_data.get("name") or "Unnamed Clan"
    
    new_clan = {"name": display_name, "tag": tag_norm}
    client.clans.append(new_clan)
    save_clans(client.clans)
    
    # Start tracking
    start_clan_tracking(new_clan)
    
    await interaction.edit_original_response(
        content=f"✅ Added clan **{display_name}** (`{tag_norm}`) and started tracking."
    )


@client.tree.command(name="removeclan", description="Remove a clan from monitored list")
@app_commands.describe(clan="Select the clan to remove")
@app_commands.autocomplete(clan=clan_autocomplete)
async def removeclan(interaction: discord.Interaction, clan: str):
    """Remove a clan from monitoring."""
    await interaction.response.send_message("➖ Removing clan...", ephemeral=True)
    c_obj = get_clan_by_tag(clan)
    if not c_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return
    
    tag_norm = c_obj["tag"]
    name = c_obj["name"]
    
    # Remove from list
    client.clans = [c for c in client.clans if c["tag"].upper() != tag_norm.upper()]
    save_clans(client.clans)
    
    # Stop tracking
    stop_clan_tracking(tag_norm)
    
    await interaction.edit_original_response(
        content=f"✅ Removed clan **{name}** (`{tag_norm}`) from monitored list and stopped tracking."
    )


# ============================
# DONATION TRACKING COMMANDS
# ============================

@client.tree.command(name="donations", description="View donation statistics for a player")
@app_commands.describe(tag="Player tag (example: #2PQUE2J)")
async def donations(interaction: discord.Interaction, tag: str):
    """View comprehensive donation statistics."""
    await interaction.response.send_message("💝 Fetching donation stats...", ephemeral=True)
    tag_norm = tag.strip().upper()
    if not tag_norm.startswith("#"):
        tag_norm = "#" + tag_norm
    
    player = await client.coc_api.get_player(tag_norm)
    if not player:
        await interaction.edit_original_response(content="❌ Could not fetch player. Check tag or API.")
        return
    
    # Get lifetime donations from achievements
    lifetime = extract_lifetime_donations(player)
    seasonal = player.get("donations", 0)
    received = player.get("donationsReceived", 0)
    
    embed = discord.Embed(
        title=f"💝 Donation Stats — {player.get('name', 'Unknown')}",
        color=0x2ecc71,
        timestamp=datetime.now(timezone.utc)
    )
    
    embed.add_field(name="🆔 Tag", value=f"`{tag_norm}`", inline=True)
    
    # Lifetime donations (from achievements)
    embed.add_field(
        name="📊 Lifetime Donations",
        value=(
            f"Troops: **{lifetime['troops_donated']:,}**\n"
            f"Spells: **{lifetime['spells_donated']:,}**\n"
            f"Siege: **{lifetime['siege_donated']:,}**\n"
            f"**Total: {lifetime['total_donated']:,}**"
        ),
        inline=False
    )
    
    # Seasonal donations
    embed.add_field(
        name="📅 Current Season",
        value=f"Sent: **{seasonal:,}**\nReceived: **{received:,}**",
        inline=True
    )
    
    # Try to get tracked stats if player is in a monitored clan
    for clan in client.clans:
        stats = get_player_donation_stats(tag_norm, clan["tag"])
        if stats:
            embed.add_field(
                name="📈 Tracked Stats",
                value=(
                    f"Tracking since: **{stats.get('tracked_from', 'N/A')}**\n"
                    f"Last snapshot: **{stats.get('snapshot_date', 'N/A')}**"
                ),
                inline=True
            )
            break
    
    embed.set_footer(text="Lifetime stats from achievements • Seasonal from current season")
    await interaction.edit_original_response(content="✅ Donation stats:", embed=embed)


@client.tree.command(name="donationhistory", description="View monthly donation history for a clan")
@app_commands.describe(
    clan="Clan to check",
    months="Number of months to show (default: 6, max: 24)"
)
@app_commands.autocomplete(clan=clan_autocomplete)
async def donationhistory(interaction: discord.Interaction, clan: str, months: int = 6):
    """View monthly donation history."""
    await interaction.response.send_message("📊 Building donation history...", ephemeral=True)
    
    if months < 1:
        months = 1
    if months > 24:
        months = 24
    
    clan_obj = get_clan_by_tag(clan)
    if not clan_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return
    
    history = get_donation_history(clan_obj["tag"], limit=months)
    
    if not history:
        await interaction.edit_original_response(
            content=f"⚠️ No donation history found for **{clan_obj['name']}**. "
                   f"Snapshots will be created automatically on the 1st of each month."
        )
        return
    
    embed = discord.Embed(
        title=f"📊 Donation History — {clan_obj['name']}",
        color=0x3498db,
        timestamp=datetime.now(timezone.utc)
    )
    
    # Build summary
    lines = []
    total_all_months = 0
    
    for month_data in history[:months]:
        month = month_data.get("month", "Unknown")
        total_monthly = month_data.get("total_monthly", 0)
        total_all_months += total_monthly
        member_count = len(month_data.get("members", {}))
        
        lines.append(f"**{month}**: {total_monthly:,} donations ({member_count} members)")
    
    if lines:
        embed.description = "\n".join(lines)
        embed.add_field(
            name="📈 Summary",
            value=f"Total tracked: **{total_all_months:,}** donations\nMonths shown: **{len(history)}**",
            inline=False
        )
    else:
        embed.description = "No donation data available yet."
    
    embed.set_footer(text="Monthly snapshots taken on the 1st of each month")
    await interaction.edit_original_response(content="✅ Donation history:", embed=embed)
    
    # Send detailed breakdown if requested
    if len(history) > 0 and months <= 3:
        detailed_lines = []
        for month_data in history:
            month = month_data.get("month", "Unknown")
            members = month_data.get("members", {})
            
            # Sort by monthly donations (descending)
            sorted_members = sorted(
                members.items(),
                key=lambda x: x[1].get("monthly", 0),
                reverse=True
            )[:10]  # Top 10 per month
            
            detailed_lines.append(f"\n**{month}** (Top 10):")
            for tag, data in sorted_members:
                name = data.get("name", "Unknown")
                monthly = data.get("monthly", 0)
                detailed_lines.append(f"• {name}: {monthly:,}")
        
        if detailed_lines:
            text = "\n".join(detailed_lines)
            if len(text) > 2000:
                text = text[:1950] + "\n... (truncated)"
            await interaction.followup.send(f"📋 **Detailed Breakdown:**\n{text}", ephemeral=True)


@client.tree.command(name="takesnapshot", description="Manually take a donation snapshot for a clan (Elder)")
@app_commands.describe(clan="Clan to snapshot")
@app_commands.autocomplete(clan=clan_autocomplete)
async def takesnapshot(interaction: discord.Interaction, clan: str):
    """Manually create a donation snapshot."""
    await interaction.response.send_message("📸 Taking snapshot...", ephemeral=True)
    
    clan_obj = get_clan_by_tag(clan)
    if not clan_obj:
        await interaction.edit_original_response(content="❌ Clan not found in monitored list.")
        return
    
    # Fetch all members
    members = await client.coc_api.get_clan_members(clan_obj["tag"])
    if not members:
        await interaction.edit_original_response(content="❌ Could not fetch clan members.")
        return
    
    # Fetch player data for all members (with caching)
    player_cache = {}
    fetched = 0
    for member in members:
        tag = member.get("tag")
        if tag:
            player = await client.coc_api.get_player(tag)
            if player:
                player_cache[tag] = player
                fetched += 1
    
    if not player_cache:
        await interaction.edit_original_response(content="❌ Could not fetch any player data.")
        return
    
    # Create snapshot
    snapshot = create_donation_snapshot(clan_obj["tag"], members, player_cache)
    success = save_monthly_snapshot(clan_obj["tag"], snapshot)
    
    if success:
        month = snapshot["date"]
        member_count = len(snapshot.get("members", {}))
        
        embed = discord.Embed(
            title="✅ Snapshot Created",
            color=0x2ecc71,
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Clan", value=clan_obj["name"], inline=True)
        embed.add_field(name="Month", value=month, inline=True)
        embed.add_field(name="Members", value=str(member_count), inline=True)
        embed.add_field(name="Players Fetched", value=str(fetched), inline=True)
        
        await interaction.edit_original_response(content="✅ Snapshot created:", embed=embed)
    else:
        await interaction.edit_original_response(content="❌ Failed to save snapshot.")


# ============================
# STARTUP
# ============================
@client.event
async def on_ready():
    """Called when bot is ready."""
    print(f"[READY] {client.user} (id: {client.user.id})")
    
    try:
        synced = await client.tree.sync()
        print(f"[INFO] Slash commands synced. {len(synced)} commands registered.")
        for cmd in synced:
            print(f"  - /{cmd.name}")
    except Exception as e:
        await client.log(f"[WARN] Slash sync failed: {e}")
        print(f"[ERROR] Failed to sync commands: {e}")
    
    # Start background tasks
    if not client._tasks_started:
        client._tasks_started = True
        
        # Load member caches
        for c in client.clans:
            try:
                load_strict_cache(c["tag"])
            except Exception:
                pass
        
        # Send startup status
        try:
            ch = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
            if ch:
                for c in client.clans:
                    emb = discord.Embed(
                        title=f"Startup Status — {c['name']}",
                        color=0x3498db,
                        timestamp=datetime.now(timezone.utc)
                    )
                    emb.description = (
                        "Baseline loaded — bot will not announce existing members. "
                        "Only real joins/leaves are announced."
                    )
                    await ch.send(embed=emb)
        except Exception:
            pass
        
        # Start tracking for all clans
        for c in client.clans:
            start_clan_tracking(c)
        
        # Start reminder loop
        asyncio.create_task(fixed_time_reminder_loop(client, client.coc_api, client.clans))
        
        # Start monthly donation snapshot loop
        from trackers import monthly_donation_snapshot_loop
        asyncio.create_task(monthly_donation_snapshot_loop(client, client.coc_api, client.clans))


# ============================
# RUN
# ============================
if __name__ == "__main__":
    if not DISCORD_TOKEN or not COC_API_KEY:
        print("[FATAL] Set DISCORD_TOKEN and COC_API_KEY environment variables.")
        print("You can also set them in config.py for backward compatibility.")
    else:
        try:
            client.run(DISCORD_TOKEN)
        except KeyboardInterrupt:
            print("[INFO] Shutting down...")
        except Exception as e:
            print(f"[FATAL] Error running bot: {e}")


