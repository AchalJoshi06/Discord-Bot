# Quick Start

## 1) Start the bot

Run the active entrypoint:

```bash
python discordwelcomebot.py
```

## 2) Verify startup

You should see ready/sync logs in console, including command registration.

## 3) Validate key commands in Discord

Test these commands first:

```text
/info tag:#YLV9JYJC2
/donations tag:#YLV9JYJC2
/donationhistory clan:ALL months:3
/status
```

## 4) Common issues

- Commands missing after restart:
   - Wait 1-2 minutes for sync completion.
   - Restart once if command metadata was changed.

- Player lookup fails:
   - Confirm the tag format starts with `#`.
   - Check that `COC_API_KEY` is valid.

- Bot does not start:
   - Ensure `DISCORD_TOKEN` and `COC_API_KEY` are set in environment variables.

## 5) Current documentation status

- Roadmap items in `bot_improvemtns.txt` are marked complete.
- Main references: `README.md`, `BOT_DOCUMENTATION.md`, and this file.

