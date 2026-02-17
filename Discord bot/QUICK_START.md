# Quick Start - Using the New Bot

## ⚠️ IMPORTANT: You Must Run `bot.py`, NOT `discordwelcomebot.py`

The new refactored bot is in `bot.py`. The old file `discordwelcomebot.py` does NOT have the new donation features.

## Step 1: Stop the Old Bot

If you're currently running the bot:
1. Press `Ctrl+C` in the terminal to stop it
2. Make sure it's completely stopped

## Step 2: Start the New Bot

Run the NEW bot file:
```bash
python bot.py
```

**NOT** `python discordwelcomebot.py` ❌

## Step 3: Verify It's Running

You should see in the console:
```
[READY] YourBotName (id: 123456789)
[INFO] Slash commands synced. X commands registered.
  - /link
  - /info
  - /donations          ← Should see this!
  - /donationhistory    ← Should see this!
  - /takesnapshot       ← Should see this!
  - /synccommands       ← Should see this!
```

## Step 4: Wait for Discord to Sync

1. Wait 1-2 minutes after bot starts
2. Type `/` in Discord
3. You should see the new commands

## Step 5: Test the Features

### Test `/info` command:
```
/info tag:#YLV9JYJC2
```

You should now see a **"💝 Lifetime Donations"** section showing:
- Troops donated (lifetime)
- Spells donated (lifetime)
- Siege donated (lifetime)
- Total lifetime donations

### Test `/donations` command:
```
/donations tag:#YLV9JYJC2
```

This should show comprehensive donation stats.

## Troubleshooting

### Still don't see new commands?

1. **Check which file is running:**
   - Look at the console output
   - Should say `bot.py` is running
   - If it says `discordwelcomebot.py`, you're running the wrong file!

2. **Force sync commands:**
   ```
   /synccommands
   ```

3. **Check bot status:**
   ```
   /status
   ```
   Should show the number of registered commands

### Still don't see lifetime donations in `/info`?

1. **Verify you're running `bot.py`** (not the old file)
2. **Check console for errors** when running `/info`
3. **The player might have 0 lifetime donations** (now it will show 0 instead of hiding)

## File Comparison

- ✅ **`bot.py`** - NEW refactored bot with donation tracking
- ❌ **`discordwelcomebot.py`** - OLD bot without new features

Make sure you're running `bot.py`!

