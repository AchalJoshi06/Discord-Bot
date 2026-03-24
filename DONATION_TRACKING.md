# Donation Tracking System

## Overview

The bot implements a comprehensive donation tracking system that combines:
1. **Lifetime donations** from Clash of Clans achievements (never reset)
2. **Monthly snapshots** to track donation trends over time

This dual approach ensures accurate lifetime statistics while providing monthly donation history and trends.

## How It Works

### Lifetime Donations (from Achievements)

The Clash of Clans API provides lifetime donation statistics through player achievements:
- **Friend in Need**: Lifetime troops donated
- **Sharing is Caring**: Lifetime spells donated  
- **Siege Sharer**: Lifetime siege machines donated

These achievement values are **lifetime totals** that never reset, providing accurate historical data similar to ClashPerk.

### Monthly Snapshots

Since the API doesn't provide historical or monthly breakdowns, the bot implements a monthly snapshot system:

1. **Automatic Snapshots**: On the 1st of each month (configurable via `MONTHLY_SNAPSHOT_DAY`), the bot:
   - Fetches all clan members
   - Records each player's current seasonal donation count
   - Extracts lifetime donations from achievements
   - Stores this data in `donation_snapshots.json`

2. **Monthly Calculation**: By comparing consecutive monthly snapshots, the bot calculates:
   - Donations made during that specific month
   - Monthly trends and patterns
   - Tracked lifetime totals (starting from when tracking began)

## Data Storage

Snapshots are stored in `donation_snapshots.json` with the following structure:

```json
{
  "#CLANTAG": [
    {
      "date": "2024-01",
      "timestamp": "2024-01-01T00:00:00Z",
      "members": {
        "#PLAYERTAG": {
          "name": "PlayerName",
          "seasonal": 5000,
          "lifetime": {
            "troops_donated": 100000,
            "spells_donated": 50000,
            "siege_donated": 10000,
            "total_donated": 160000
          }
        }
      }
    }
  ]
}
```

The bot keeps the last 24 months of snapshots automatically.

## Commands

### `/donations <tag>`
View comprehensive donation statistics for a player:
- Lifetime donations (from achievements)
- Current season donations
- Tracked statistics (if player is in a monitored clan)

### `/donationhistory <clan> [months]`
View monthly donation history for a clan:
- Shows last N months (default: 6, max: 24)
- Monthly totals and member counts
- Summary statistics
- Detailed breakdown for recent months (top 10 donors per month)

### `/takesnapshot <clan>`
Manually create a donation snapshot (useful for testing or catching up):
- Fetches all clan members
- Records current donation stats
- Saves snapshot for the current month

### Updated `/info <tag>`
The existing `/info` command now also displays:
- Lifetime donations from achievements
- Breakdown by type (troops, spells, siege)

## Configuration

### Environment Variables

- `MONTHLY_SNAPSHOT_DAY=1` - Day of month to take snapshots (default: 1st)

### File Location

- `donation_snapshots.json` - Stores all monthly snapshots

## How Monthly Donations Are Calculated

When you request monthly donation history:

1. The bot finds the snapshot for that month
2. Compares it with the previous month's snapshot
3. Calculates the difference in seasonal donation counts
4. This difference represents donations made during that month

**Example:**
- January snapshot: Player has 5,000 seasonal donations
- February snapshot: Player has 8,000 seasonal donations
- **February monthly donations = 8,000 - 5,000 = 3,000**

**Note**: For the first snapshot, monthly donations equal the seasonal count (no previous data to compare).

## Important Notes

### Lifetime vs Tracked

- **Lifetime donations** (from achievements): Total donations since account creation (never reset)
- **Tracked donations** (from snapshots): Only tracked from when the bot started taking snapshots

### Tracking Start Date

The bot tracks donations starting from the first snapshot taken. Historical data before bot installation is not available, but lifetime achievement totals are always accurate.

### Seasonal Resets

Seasonal donation counts reset monthly in Clash of Clans. The bot's snapshots capture these values before they reset, allowing accurate monthly tracking.

## Benefits

1. **Accurate Lifetime Stats**: Achievement-based totals never reset
2. **Monthly Trends**: Track donation patterns over time
3. **No API Violations**: Uses official API data only
4. **Automatic**: Snapshots taken automatically each month
5. **Historical Data**: Keeps 24 months of history

## Troubleshooting

### No donation history showing
- Snapshots are taken automatically on the 1st of each month
- Use `/takesnapshot` to manually create the first snapshot
- Wait until next month for automatic snapshots

### Incorrect monthly totals
- Monthly donations are calculated from differences between snapshots
- If a player leaves and rejoins, their seasonal count resets
- First snapshot for a player uses their current seasonal count

### Missing lifetime stats
- Lifetime stats come from achievements
- If achievements aren't available, lifetime stats will be 0
- This is rare but can happen with very new accounts

## Future Enhancements

Potential improvements:
- Export donation history to CSV/Excel
- Donation leaderboards
- Donation goals and milestones
- Integration with other clan management features
- Donation ratio tracking over time

