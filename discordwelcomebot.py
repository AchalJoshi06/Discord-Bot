# Configuration from environment variables (keeps secrets out of code)
DISCORD_TOKEN ="DISCORD_API"
COC_API_KEY ="COC_API"
CHANNEL_ID = 1439346726048633053  # Discord channel ID to send welcome messages
CHECK_INTERVAL =5

CLANS = [
    {"name": "CC2 Academy", "tag": "#PQUCURCQ"},
    {"name": "CC2 Dominion", "tag": "#2JJJCCRQR"},
]

import discord
import requests
import asyncio
import urllib.parse
from discord import app_commands

# ==========================
# DISCORD CLIENT WITH APP COMMANDS
# ==========================
intents = discord.Intents.all()

class MyBot(discord.Client):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

client = MyBot(intents=intents)

previous_members = {clan["tag"]: set() for clan in CLANS}

# ==========================
# COC API CALL
# ==========================
def get_clan_members(clan_tag):
    url = f"https://api.clashofclans.com/v1/clans/{urllib.parse.quote(clan_tag)}"
    headers = {"Authorization": f"Bearer {COC_API_KEY}"}

    try:
        r = requests.get(url, headers=headers, timeout=15)
    except Exception as e:
        print(f"[ERROR] Request failed for {clan_tag}: {e}")
        return []

    if r.status_code != 200:
        print(f"[ERROR] Fetching clan ({clan_tag}) returned {r.status_code}: {r.text}")
        return []

    return r.json().get("memberList", [])


# ==========================
# TRACK JOINERS
# ==========================
async def track_clan(clan):
    clan_name = clan["name"]
    clan_tag = clan["tag"]

    await client.wait_until_ready()

    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        try:
            channel = await client.fetch_channel(CHANNEL_ID)
        except Exception as e:
            print(f"[ERROR] Cannot access channel {CHANNEL_ID}: {e}")
            return

    # Load initial members
    members = get_clan_members(clan_tag)
    previous_members[clan_tag] = {m["tag"] for m in members}

    print(f"[INFO] Tracking started for {clan_name} ({clan_tag}). {len(previous_members[clan_tag])} members loaded.")

    while not client.is_closed():
        await asyncio.sleep(CHECK_INTERVAL)

        members = get_clan_members(clan_tag)
        current_tags = {m["tag"] for m in members}

        new_members = current_tags - previous_members.get(clan_tag, set())

        for tag in new_members:
            joined = next((m for m in members if m["tag"] == tag), None)

            if joined:
                msg = (
                    f"🎉 **{joined['name']}** ({joined['tag']}) "
                    f"has joined **{clan_name}** ({clan_tag})!"
                )
                print("[NEW MEMBER]", msg)

                try:
                    await channel.send(msg)
                except Exception as e:
                    print(f"[ERROR] Could not send message: {e}")

        previous_members[clan_tag] = current_tags


# ==========================
# SLASH COMMAND: /clashperk
# ==========================
@client.tree.command(name="clashperk", description="Get player info using a player tag.")
@app_commands.describe(tag="Player tag, example: #P2JQ02C")
async def clashperk(interaction: discord.Interaction, tag: str):
    await interaction.response.defer()  # Shows loading

    url = f"https://api.clashofclans.com/v1/players/{urllib.parse.quote(tag)}"
    headers = {"Authorization": f"Bearer {COC_API_KEY}"}

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        await interaction.followup.send(f"❌ Invalid tag or API error.\n`{r.text}`")
        return

    data = r.json()

    name = data.get("name", "Unknown")
    exp = data.get("expLevel", "?")
    th = data.get("townHallLevel", "?")
    trophies = data.get("trophies", "?")

    msg = (
        f"🏆 **Player Information**\n"
        f"👤 Name: **{name}**\n"
        f"🔖 Tag: `{tag}`\n"
        f"🏯 TownHall: **{th}**\n"
        f"⭐ Level: **{exp}**\n"
        f"🥇 Trophies: **{trophies}**"
    )

    await interaction.followup.send(msg)


# ==========================
# BOT READY
# ==========================
@client.event
async def on_ready():
    print(f"[READY] Bot logged in as {client.user} (id: {client.user.id})")

    channel = client.get_channel(CHANNEL_ID)
    if channel:
        await channel.send("✅ Bot is online and slash commands are synced!")

    # Sync slash commands globally
    await client.tree.sync()
    print("[INFO] Slash commands synced.")

    # Start clan tracking
    for clan in CLANS:
        asyncio.create_task(track_clan(clan))

    print("[INFO] Tracking enabled for both clans.")


# ==========================
# START BOT
# ==========================
def main():
    print("Starting bot...")
    client.run(DISCORD_TOKEN)

if __name__ == "__main__":
    main()
