"""Background tracking tasks for clan monitoring."""
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import discord

from config import (
    CHECK_INTERVAL, WAR_POLL_INTERVAL, UPGRADE_CHECK_INTERVAL,
    UPGRADE_ALERT_CHECK, ANNOUNCE_CHANNEL_ID, MONTHLY_SNAPSHOT_DAY
)
from storage import load_strict_cache, save_strict_cache
from coc_api import COCAPI
from embeds import build_join_embed, build_leave_embed
from calculations import extract_hero_levels
from donations import (
    create_donation_snapshot, save_monthly_snapshot, get_current_month_key
)


class ClanTracker:
    """Manages tracking for a single clan."""
    
    def __init__(self, client: discord.Client, coc_api: COCAPI, clan: Dict[str, str]):
        self.client = client
        self.coc_api = coc_api
        self.clan = clan
        self.clan_tag = clan["tag"]
        self.clan_name = clan["name"]
        self.member_cache: set = load_strict_cache(self.clan_tag)
        self.running = False
    
    async def track_joins_leaves(self):
        """Track clan member joins and leaves."""
        await self.client.wait_until_ready()
        channel = self.client.get_channel(ANNOUNCE_CHANNEL_ID) or await self.client.fetch_channel(ANNOUNCE_CHANNEL_ID)
        
        print(f"[TRACK] Started tracker for {self.clan_name} ({self.clan_tag})")
        self.running = True
        
        while not self.client.is_closed() and self.running:
            try:
                await asyncio.sleep(CHECK_INTERVAL)
                member_list = await self.coc_api.get_clan_members(self.clan_tag)
                if not member_list:
                    continue
                
                current_tags = {m["tag"]: m.get("name") for m in member_list if m.get("tag")}
                prev_tags = self.member_cache
                
                # Detect joins
                joins = [tag for tag in current_tags if tag not in prev_tags]
                for tag in joins:
                    try:
                        player = await self.coc_api.get_player(tag)
                        if player:
                            emb = build_join_embed(player, tag, self.clan_name)
                            await channel.send(embed=emb)
                        else:
                            name = current_tags.get(tag, tag)
                            emb = discord.Embed(
                                title=f"🟢 PLAYER JOINED — {name}",
                                description=f"`{tag}` joined **{self.clan_name}**",
                                color=0x2ecc71,
                                timestamp=datetime.now(timezone.utc)
                            )
                            emb.add_field(name="Player Tag", value=f"`{tag}`", inline=True)
                            await channel.send(embed=emb)
                        await asyncio.sleep(0.15)  # Rate limit protection
                    except Exception as e:
                        print(f"[TRACK] Error processing join for {tag}: {e}")
                    
                    self.member_cache.add(tag)
                
                if joins:
                    save_strict_cache(self.clan_tag, self.member_cache)
                
                # Detect leaves
                leaves = [tag for tag in prev_tags if tag not in current_tags]
                for tag in leaves:
                    try:
                        name = current_tags.get(tag) or tag
                        emb = build_leave_embed(tag, name)
                        await channel.send(embed=emb)
                        await asyncio.sleep(0.15)
                    except Exception as e:
                        print(f"[TRACK] Error processing leave for {tag}: {e}")
                    
                    if tag in self.member_cache:
                        self.member_cache.remove(tag)
                        save_strict_cache(self.clan_tag, self.member_cache)
            
            except Exception as e:
                print(f"[TRACK] Error in tracker loop for {self.clan_name}: {e}")
                await asyncio.sleep(CHECK_INTERVAL)
    
    async def track_war(self):
        """Track war attacks."""
        await self.client.wait_until_ready()
        channel = self.client.get_channel(ANNOUNCE_CHANNEL_ID) or await self.client.fetch_channel(ANNOUNCE_CHANNEL_ID)
        
        print(f"[WAR] Started war tracker for {self.clan_name} ({self.clan_tag})")
        war_baseline: Dict[str, List[Dict[str, Any]]] = {}
        
        while not self.client.is_closed() and self.running:
            try:
                await asyncio.sleep(WAR_POLL_INTERVAL)
                war = await self.coc_api.get_current_war(self.clan_tag)
                
                if not war or war.get("state") != "inWar":
                    continue
                
                clan_data = war.get("clan") or {}
                members = clan_data.get("members") or []
                current_map: Dict[str, List[Dict[str, Any]]] = {}
                
                for member in members:
                    if not isinstance(member, dict):
                        continue
                    tag = member.get("tag")
                    if not tag:
                        continue
                    attacks = member.get("attacks", []) or []
                    current_map[tag] = attacks
                
                prev_map = war_baseline
                
                for tag, attacks in current_map.items():
                    prev_attacks = prev_map.get(tag, [])
                    if len(attacks) > len(prev_attacks):
                        name = next((m.get("name") for m in members if m.get("tag") == tag), tag)
                        new_attacks = attacks[len(prev_attacks):]
                        for atk in new_attacks:
                            stars = atk.get("stars", "?")
                            desc = atk.get("destructionPercentage", atk.get("destructionPercent", "?"))
                            try:
                                await channel.send(
                                    f"⚔️ **WAR HIT:** {name} ({tag}) — {stars}★ • {desc}%"
                                )
                                await asyncio.sleep(0.12)
                            except Exception:
                                pass
                
                war_baseline = current_map
            
            except Exception as e:
                print(f"[WAR] Error in war tracker for {self.clan_name}: {e}")
                await asyncio.sleep(WAR_POLL_INTERVAL)
    
    async def check_hero_upgrades(self):
        """Check for players upgrading 3+ heroes."""
        await self.client.wait_until_ready()
        channel = self.client.get_channel(ANNOUNCE_CHANNEL_ID) or await self.client.fetch_channel(ANNOUNCE_CHANNEL_ID)
        
        while not self.client.is_closed() and self.running:
            try:
                await asyncio.sleep(UPGRADE_CHECK_INTERVAL)
                members = await self.coc_api.get_clan_members(self.clan_tag)
                if not members:
                    continue
                
                for m in members:
                    tag = m.get("tag")
                    if not tag:
                        continue
                    
                    player = await self.coc_api.get_player(tag)
                    if not player:
                        continue
                    
                    upgrading = []
                    if isinstance(player.get("heroes"), list):
                        for h in player["heroes"]:
                            ut = h.get("upgradeTimeLeft")
                            if ut is not None and ut not in (0, "0", ""):
                                upgrading.append(h.get("name") or "Unknown Hero")
                    
                    if len(upgrading) >= 3:
                        embed = discord.Embed(
                            title="⚠️ Hero Upgrade Alert",
                            description=f"**{player.get('name')}** (`{tag}`) is upgrading **{len(upgrading)} heroes**!",
                            color=0xe67e22,
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.add_field(name="Heroes", value="\n".join(upgrading) if upgrading else "—")
                        try:
                            await channel.send(embed=embed)
                        except Exception:
                            pass
            
            except Exception as e:
                print(f"[UPGRADE] Error in hero upgrade check for {self.clan_name}: {e}")
                await asyncio.sleep(UPGRADE_CHECK_INTERVAL)
    
    async def track_upgrades(self):
        """Track all upgrades (heroes, troops, spells, pets)."""
        await self.client.wait_until_ready()
        channel = self.client.get_channel(ANNOUNCE_CHANNEL_ID) or await self.client.fetch_channel(ANNOUNCE_CHANNEL_ID)
        last_upgrade_cache: Dict[str, List[str]] = {}
        
        while not self.client.is_closed() and self.running:
            try:
                await asyncio.sleep(UPGRADE_ALERT_CHECK)
                members = await self.coc_api.get_clan_members(self.clan_tag)
                if not members:
                    continue
                
                for m in members:
                    tag = m.get("tag")
                    if not tag:
                        continue
                    
                    player = await self.coc_api.get_player(tag)
                    if not player:
                        continue
                    
                    upgrading: List[str] = []
                    for u in player.get("heroes", []):
                        ut = u.get("upgradeTimeLeft")
                        if ut is not None and ut not in (0, "0", ""):
                            upgrading.append(f"Hero: {u.get('name')} → L{(u.get('level') or 0) + 1}")
                    
                    for u in player.get("pets", []):
                        ut = u.get("upgradeTimeLeft")
                        if ut is not None and ut not in (0, "0", ""):
                            upgrading.append(f"Pet: {u.get('name')} → L{(u.get('level') or 0) + 1}")
                    
                    for u in (player.get("troops") or []) + (player.get("spells") or []):
                        ut = u.get("upgradeTimeLeft")
                        if ut is not None and ut not in (0, "0", ""):
                            upgrading.append(f"Troop/Spell: {u.get('name')} → L{(u.get('level') or 0) + 1}")
                    
                    old = last_upgrade_cache.get(tag, [])
                    new_upgrades = [x for x in upgrading if x not in old]
                    
                    if new_upgrades:
                        embed = discord.Embed(
                            title=f"⬆️ Upgrade Started — {player.get('name')}",
                            color=0x00aaff,
                            timestamp=datetime.now(timezone.utc)
                        )
                        embed.add_field(name="New Upgrades", value="\n".join(new_upgrades) if new_upgrades else "—")
                        embed.set_footer(text=tag)
                        try:
                            await channel.send(embed=embed)
                        except Exception:
                            pass
                    
                    last_upgrade_cache[tag] = upgrading
            
            except Exception as e:
                print(f"[UPGRADE] Error in upgrade tracker for {self.clan_name}: {e}")
                await asyncio.sleep(UPGRADE_ALERT_CHECK)
    
    def stop(self):
        """Stop all tracking tasks."""
        self.running = False


async def fixed_time_reminder_loop(client: discord.Client, coc_api: COCAPI, clans: List[Dict[str, str]]):
    """Send war reminders at fixed times (every even hour)."""
    await client.wait_until_ready()
    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
    print("[REMINDER] Fixed-time (every even hour) reminder loop started")
    
    from storage import load_links
    
    sent_today_hours = set()
    
    while not client.is_closed():
        now = datetime.now()
        hour = now.hour
        minute = now.minute
        
        if hour == 0 and minute == 0:
            sent_today_hours.clear()
        
        if minute == 0 and hour % 2 == 0 and hour not in sent_today_hours:
            sent_today_hours.add(hour)
            
            out_lines = []
            pending_total = 0
            dm_sent = 0
            dm_failed = 0
            
            for clan in clans:
                war = await coc_api.get_current_war(clan["tag"])
                if not war or war.get("state") != "inWar":
                    continue
                
                members = (war.get("clan") or {}).get("members") or []
                pending = [m for m in members if isinstance(m, dict) and len((m.get("attacks") or [])) == 0]
                
                if pending:
                    pending_total += len(pending)
                    out_lines.append(f"**{clan['name']}** — {len(pending)} pending")
                    out_lines += [f"• {p.get('name')} `{p.get('tag')}`" for p in pending[:40]]
                    
                    # DM sending
                    links = load_links()
                    for p in pending:
                        tag_norm = (p.get("tag") or "").upper()
                        discord_id = links.get(tag_norm)
                        if discord_id:
                            try:
                                user = await client.fetch_user(int(discord_id))
                                await user.send(
                                    f"⚠️ **WAR REMINDER**\nYou have **0 attacks used** in war for **{clan['name']}**.\nPlease attack ASAP! 💥"
                                )
                                dm_sent += 1
                                await asyncio.sleep(0.25)
                            except Exception as e:
                                dm_failed += 1
                                print(f"[DM FAIL] {tag_norm} ({discord_id}) → {e}")
            
            if out_lines:
                try:
                    msg = "⏰ **WAR REMINDER — Every 2 Hours (Even Hours)**\n" + "\n".join(out_lines)
                    await channel.send(msg + f"\n\n📨 **DM sent:** {dm_sent} | ❌ **Failed:** {dm_failed}")
                except Exception as e:
                    print(f"[REMINDER FIXED] send failed: {e}")
        
        await asyncio.sleep(30)


async def monthly_donation_snapshot_loop(client: discord.Client, coc_api: COCAPI, clans: List[Dict[str, str]]):
    """
    Take monthly donation snapshots on the configured day of each month.
    Runs once per day to check if it's time to take a snapshot.
    """
    await client.wait_until_ready()
    print("[SNAPSHOT] Monthly donation snapshot loop started")
    
    last_snapshot_month = {}  # Track last snapshot month per clan
    
    while not client.is_closed():
        try:
            now = datetime.now(timezone.utc)
            current_day = now.day
            current_month_key = get_current_month_key()
            
            # Check if it's time to take snapshots
            if current_day == MONTHLY_SNAPSHOT_DAY:
                for clan in clans:
                    clan_tag = clan["tag"]
                    clan_name = clan["name"]
                    
                    # Skip if we already took a snapshot this month
                    if last_snapshot_month.get(clan_tag) == current_month_key:
                        continue
                    
                    try:
                        print(f"[SNAPSHOT] Taking snapshot for {clan_name} ({clan_tag})")
                        
                        # Fetch all members
                        members = await coc_api.get_clan_members(clan_tag)
                        if not members:
                            continue
                        
                        # Fetch player data for all members
                        player_cache = {}
                        for member in members:
                            tag = member.get("tag")
                            if tag:
                                player = await coc_api.get_player(tag)
                                if player:
                                    player_cache[tag] = player
                                await asyncio.sleep(0.1)  # Rate limit protection
                        
                        if player_cache:
                            # Create and save snapshot
                            snapshot = create_donation_snapshot(clan_tag, members, player_cache)
                            success = save_monthly_snapshot(clan_tag, snapshot)
                            
                            if success:
                                last_snapshot_month[clan_tag] = current_month_key
                                member_count = len(snapshot.get("members", {}))
                                print(f"[SNAPSHOT] Successfully saved snapshot for {clan_name}: {member_count} members")
                                
                                # Send notification
                                try:
                                    channel = client.get_channel(ANNOUNCE_CHANNEL_ID) or await client.fetch_channel(ANNOUNCE_CHANNEL_ID)
                                    if channel:
                                        embed = discord.Embed(
                                            title=f"📸 Monthly Donation Snapshot — {clan_name}",
                                            color=0x3498db,
                                            timestamp=datetime.now(timezone.utc)
                                        )
                                        embed.add_field(name="Month", value=current_month_key, inline=True)
                                        embed.add_field(name="Members", value=str(member_count), inline=True)
                                        embed.description = "Donation snapshot taken successfully. Use `/donationhistory` to view."
                                        await channel.send(embed=embed)
                                except Exception as e:
                                    print(f"[SNAPSHOT] Failed to send notification: {e}")
                            else:
                                print(f"[SNAPSHOT] Failed to save snapshot for {clan_name}")
                    
                    except Exception as e:
                        print(f"[SNAPSHOT] Error taking snapshot for {clan_name}: {e}")
            
            # Wait 1 hour before checking again
            await asyncio.sleep(3600)
        
        except Exception as e:
            print(f"[SNAPSHOT] Error in snapshot loop: {e}")
            await asyncio.sleep(3600)


