# Command Sync Guide

## Why Don't I See New Commands?

When you add new commands to a Discord bot, Discord needs to sync them. This can take 1-2 minutes, or you may need to force a sync.

## Solutions

### Solution 1: Wait and Restart Bot (Easiest)

1. **Restart your bot** (stop and start `bot.py`)
2. The bot automatically syncs commands on startup
3. Wait 1-2 minutes for Discord to update
4. Type `/` in Discord to see new commands

### Solution 2: Use the Sync Command

If the bot is running, use the new sync command:

```
/synccommands
```

This will:
- Force sync all commands with Discord
- Show you which commands are registered
- Usually takes effect within 1-2 minutes

### Solution 3: Manual Sync (Advanced)

If you have access to the bot's code, you can add this temporary code to force sync:

```python
@client.event
async def on_ready():
    # ... existing code ...
    try:
        synced = await client.tree.sync()
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print(f"Sync failed: {e}")
```

## New Donation Commands

After syncing, you should see these new commands:

1. **`/donations <tag>`** - View donation statistics
2. **`/donationhistory <clan> [months]`** - View monthly history
3. **`/takesnapshot <clan>`** - Create manual snapshot
4. **`/synccommands`** - Force sync commands

## Troubleshooting

### Commands Still Not Appearing?

1. **Check bot permissions**: Bot needs `applications.commands` scope
2. **Reinvite bot**: Use this link with proper scopes:
   ```
   https://discord.com/api/oauth2/authorize?client_id=YOUR_BOT_ID&permissions=8&scope=bot%20applications.commands
   ```
3. **Check console**: Look for sync errors in bot output
4. **Wait longer**: Discord can take up to 5 minutes to update

### Verify Commands Are Registered

Use `/status` command to see how many commands are registered.

### Check Bot Logs

When bot starts, you should see:
```
[INFO] Slash commands synced. X commands registered.
  - /link
  - /info
  - /donations
  - /donationhistory
  ...
```

If you don't see the donation commands listed, there's a sync issue.

## Quick Checklist

- [ ] Bot is running (`python bot.py`)
- [ ] Bot shows "READY" in console
- [ ] Commands synced message appears
- [ ] Waited 1-2 minutes after sync
- [ ] Typed `/` in Discord to check
- [ ] Used `/synccommands` if needed
- [ ] Checked `/status` for command count

## Still Having Issues?

1. Restart the bot completely
2. Check for errors in console
3. Verify bot has proper permissions
4. Try `/synccommands` command
5. Wait 5 minutes and check again

Discord command syncing can be slow sometimes, but it should work within a few minutes!

