# Clash of Clans API Endpoints Used by CC2 Bot

Base URL:
- `https://api.clashofclans.com/v1`

Notes:
- This list is from active runtime bot code (`discordwelcomebot.py` and `cogs/*.py`).
- Legacy fallback endpoints are included where intentionally used.
- Backup/archive/test files are excluded from this reference.

## Core Clan & Player

- `GET /players/{playerTag}`
  - Used for player profile fetches and player-level command features.

- `GET /clans/{clanTag}`
  - Used for clan profile data, member list extraction, clan dashboard, capital stats, and opponent lookup.

- `GET /clans/{clanTag}/currentwar`
  - Used for active war status/map/war-related commands.

- `GET /clans/{clanTag}/warlog?limit={limit}`
  - Used for war history command (live war log source).

- `GET /clans/{clanTag}/currentwar/leaguegroup`
  - Used for CWL league group status and round discovery.

- `GET /clanwarleagues/wars/{warTag}`
  - Used for CWL round war summaries.

## Raid Weekend & Capital

- `GET /clans/{clanTag}/capitalraidseasons?limit={limit}`
  - Primary endpoint for raid weekend data (season summary, members, logs).

- `GET /clans/{clanTag}/capitalraidseason?limit={limit}`
  - Legacy compatibility fallback when plural endpoint does not return usable items.

- `GET /locations/{locationId}/rankings/capitals?limit={limit}`
  - Used for capital ranking command (`locationId=32000000` for global).

- `GET /capitalleagues`
  - Used to list all capital leagues.

- `GET /capitalleagues/{leagueId}`
  - Used to fetch details for a specific capital league.

## Rankings & Leaderboards

- `GET /locations/{locationId}/rankings/clans`
  - Used for clan trophy rankings command (`locationId=32000000` for global).

- `GET /locations/{locationId}/rankings/players`
  - Used for player trophy rankings command (`locationId=32000000` for global).

## Labels & Categorization

- `GET /labels/clans`
  - Used for clan label discovery and filtering support.

- `GET /labels/players`
  - Used for player label discovery and filtering support.

## Locations & Geography

- `GET /locations`
  - Used for location discovery (countries and regions for rankings).

- `GET /locations/{locationId}`
  - Used for location detail lookup (resolve location ID to name).

## CWL Leagues Reference

- `GET /warleagues`
  - Used for discovering all available CWL leagues.

- `GET /warleagues/{leagueId}`
  - Used for fetching details for a specific CWL league.

## Builder Base Rankings

- `GET /locations/{locationId}/rankings/clans-versus`
  - Used for builder base clan trophy rankings by location.

- `GET /locations/{locationId}/rankings/players-versus`
  - Used for builder base player trophy rankings by location.

## Home Village Leagues Reference

- `GET /leagues`
  - Used for discovering all available home village leagues.

- `GET /leagues/{leagueId}`
  - Used for fetching details for a specific home village league.

## Builder Base Leagues Reference

- `GET /builderbaseleagues`
  - Used for discovering all available builder base leagues.

- `GET /builderbaseleagues/{leagueId}`
  - Used for fetching details for a specific builder base league.

## League Seasons Reference

- `GET /leagues/{leagueId}/seasons`
  - Used for discovering all seasons for a specific home village league.

- `GET /leagues/{leagueId}/seasons/{seasonId}`
  - Used for fetching details for a specific season in a league.

## Command Mapping (Quick Reference)

- War:
  - `warhistory` -> `/clans/{clanTag}/warlog`
  - War status/map style commands -> `/clans/{clanTag}/currentwar`
  - `cwlgroup` -> `/clans/{clanTag}/currentwar/leaguegroup`
  - `cwlround` -> `/clanwarleagues/wars/{warTag}` (war tags from leaguegroup)
  - `rankings` -> `/locations/{locationId}/rankings/clans` or `/locations/{locationId}/rankings/players`
  - `bbclanrank` -> `/locations/{locationId}/rankings/clans-versus` (builder base)
  - `bbplayerrank` -> `/locations/{locationId}/rankings/players-versus` (builder base)
  - `labels` -> `/labels/clans` or `/labels/players`
  - `locations` -> `/locations` (with optional search/filter)
  - `warleagues` -> `/warleagues` (list all CWL leagues)
  - `warleague` -> `/warleagues/{leagueId}` (CWL league details)
  - `leagues` -> `/leagues` (list all home village leagues)
  - `league` -> `/leagues/{leagueId}` (home village league details)
  - `builderbaseleagues` -> `/builderbaseleagues` (list all builder base leagues)
  - `builderbaseleague` -> `/builderbaseleagues/{leagueId}` (builder base league details)
  - `leagueseasons <id>` -> `/leagues/{leagueId}/seasons` (list seasons for a league)
  - `leagueseason <id> <season>` -> `/leagues/{leagueId}/seasons/{seasonId}` (season details)

- Raid:
  - `raidstatus`, `raidreport`, `raidhistory`, `raidtrends`, `raidsleft` -> `/clans/{clanTag}/capitalraidseasons` (with legacy fallback)

- Capital:
  - `capitalstatus` -> `/clans/{clanTag}` + latest `capitalraidseasons`
  - `capitalrank` -> `/locations/{locationId}/rankings/capitals`
  - `capitalleagues` -> `/capitalleagues`, `/capitalleagues/{leagueId}`

- General clan/player lookups:
  - Clan dashboard and related utilities -> `/clans/{clanTag}`
  - Player profile utilities -> `/players/{playerTag}`

## Useful Endpoints Not Yet Used (Recommended)

These endpoints can be evaluated for future additions.

### High Priority

- None at this time (all commonly-used endpoints have been implemented in this session).

### Medium Priority

- `GET /goldpass/seasons/current`
  - Useful for season timing displays/reminders, but not core to clan performance features.

### Low Priority

- `GET /clans` (Search)
  - Can be used for clan discovery/search, but most of this bot already operates from known clan tags.

## Implementation Notes

**This Session (March 23, 2026):**
- Implemented 8 endpoint groups with 15 new commands
- Coverage includes rankings, labels, locations, CWL leagues, builder base rankings, home village leagues, builder base leagues, and league seasons
- All implementations follow consistent patterns: helper functions → paginated commands → documentation → testing
- No regressions: all 21 existing tests continue to pass
