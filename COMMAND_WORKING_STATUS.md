# CC2 Command Working Status (Code-Aligned)

Purpose:
- Track whether each active command is working in live testing.
- Keep this file in sync with `commands_reference.txt` and active cogs.

Status options:
- WORKING
- FAILING
- NOT_TESTED

Last sync baseline date:
- 2026-03-24

## Command Checklist

| Command | Type | Aliases | Access | Works | Checked On | Notes |
|---|---|---|---|---|---|---|
| info | Hybrid | i | Everyone | NOT_TESTED |  |  |
| profile | Hybrid | pf | Everyone | NOT_TESTED |  |  |
| p | Text-only | - | Everyone | NOT_TESTED |  |  |
| upgradepriority | Hybrid | upg | Everyone | NOT_TESTED |  |  |
| upgradecheck | Hybrid | uc | Everyone | NOT_TESTED |  |  |
| compare | Hybrid | cmp | Everyone | NOT_TESTED |  |  |
| rushhistory | Hybrid | rhs | Everyone | NOT_TESTED |  |  |
| whohavenotattacked | Hybrid | wna | Everyone | NOT_TESTED |  |  |
| warmap | Hybrid | wm | Everyone | NOT_TESTED |  |  |
| opponentlineup | Hybrid | ol | Everyone | NOT_TESTED |  |  |
| warreminder | Hybrid | wr | Everyone | NOT_TESTED |  |  |
| warhistory | Hybrid | wh | Everyone | NOT_TESTED |  |  |
| warperformance | Hybrid | wp | Everyone | NOT_TESTED |  |  |
| wartrends | Hybrid | wt | Everyone | NOT_TESTED |  |  |
| misstreak | Hybrid | ms2 | Leadership | NOT_TESTED |  |  |
| attacklog | Hybrid | atklog | Everyone | NOT_TESTED |  |  |
| warrating | Hybrid | wrate | Leadership | NOT_TESTED |  |  |
| warpreview | Hybrid | wpv | Everyone | NOT_TESTED |  |  |
| rankings | Hybrid | rank | Everyone | NOT_TESTED |  | New Mar 24 |
| labels | Hybrid | lbl | Everyone | NOT_TESTED |  | New Mar 24 |
| locations | Hybrid | loc | Everyone | NOT_TESTED |  | New Mar 24 |
| raidstatus | Hybrid | rs | Everyone | NOT_TESTED |  |  |
| raidreport | Hybrid | rrpt | Everyone | NOT_TESTED |  |  |
| raidhistory | Hybrid | rh | Everyone | NOT_TESTED |  |  |
| raidtrends | Hybrid | rt | Everyone | NOT_TESTED |  |  |
| raidsleft | Hybrid | rl | Everyone | NOT_TESTED |  |  |
| raidreminder | Hybrid | rr | Everyone | NOT_TESTED |  |  |
| donations | Hybrid | don | Everyone | NOT_TESTED |  |  |
| donationhistory | Hybrid | dh | Everyone | NOT_TESTED |  |  |
| takesnapshot | Hybrid | ts | Leadership | NOT_TESTED |  |  |
| top | Hybrid | lb | Everyone | NOT_TESTED |  |  |
| myrank | Hybrid | mr | Everyone | NOT_TESTED |  |  |
| achievements | Hybrid | ach | Everyone | NOT_TESTED |  |  |
| milestone | Hybrid | ms | Everyone | NOT_TESTED |  |  |
| scanachievements | Hybrid | scanach | Leadership | NOT_TESTED |  |  |
| addachievement | Hybrid | addach | Leadership | NOT_TESTED |  |  |
| challenge | Hybrid | ch | Everyone | NOT_TESTED |  |  |
| config | Hybrid Group | cfg | Leadership | NOT_TESTED |  |  |
| config set | Group Subcommand | cset | Leadership | NOT_TESTED |  |  |
| config get | Group Subcommand | cget | Leadership | NOT_TESTED |  |  |
| help | Hybrid | h | Everyone | NOT_TESTED |  |  |
| link | Hybrid | ln | Everyone | NOT_TESTED |  |  |
| setmain | Hybrid | mainacc | Everyone | NOT_TESTED |  |  |
| unlink | Text-only | unln | Everyone | NOT_TESTED |  |  |
| whois | Text-only | wi | Everyone | NOT_TESTED |  |  |
| status | Hybrid | st | Everyone | NOT_TESTED |  |  |
| calculate | Hybrid | calc | Everyone | NOT_TESTED |  |  |
| maintenance | Hybrid | maint | Leadership | NOT_TESTED |  |  |
| maintstatus | Hybrid | mstat | Leadership | NOT_TESTED |  |  |
| remind | Hybrid | rm | Everyone | NOT_TESTED |  |  |
| clearbot | Hybrid | cb | Admin | NOT_TESTED |  |  |
| clear | Hybrid | cg | Admin | NOT_TESTED |  |  |
| roster | Hybrid | ros | Everyone | NOT_TESTED |  |  |
| kicksuggestions | Hybrid | ks | Leadership | NOT_TESTED |  |  |
| inactive | Hybrid | ia | Leadership | NOT_TESTED |  |  |
| clearcache | Slash-only | - | Leadership | NOT_TESTED |  |  |
| cleanup | Slash-only | - | Leadership | NOT_TESTED |  |  |
| syncroles | Slash-only | - | Leadership | NOT_TESTED |  |  |
| addclan | Slash-only | - | Leadership | NOT_TESTED |  |  |
| removeclan | Slash-only | - | Leadership | NOT_TESTED |  |  |
| addbase | Hybrid | ab | Everyone | NOT_TESTED |  |  |
| setbase | Slash-only | - | Everyone | NOT_TESTED |  |  |
| fetchbase | Hybrid | fb | Everyone | NOT_TESTED |  |  |
| addattack | Hybrid | aatk | Everyone | NOT_TESTED |  |  |
| fetchattack | Hybrid | fatk | Everyone | NOT_TESTED |  |  |
| getbase | Slash-only | - | Everyone | NOT_TESTED |  |  |
| basebook | Slash-only | - | Everyone | NOT_TESTED |  |  |
| botstats | Hybrid | bs | Everyone | NOT_TESTED |  |  |
| findplayer | Hybrid | fp | Leadership | NOT_TESTED |  |  |
| familyreport | Hybrid | fr | Leadership | NOT_TESTED |  |  |
| clanhealth | Hybrid | chl | Everyone | NOT_TESTED |  |  |
| transferlog | Hybrid | tlog | Leadership | NOT_TESTED |  |  |
| promotionsuggestions | Hybrid | ps | Leadership | NOT_TESTED |  |  |
| poll | Hybrid | pl | Leadership | NOT_TESTED |  |  |
| createevent | Hybrid | ce | Admin | NOT_TESTED |  |  |
| welcome | Hybrid | wel | Leadership | NOT_TESTED |  |  |
| onboardingdm | Hybrid | odm | Leadership | NOT_TESTED |  |  |
| test-join | Text-only | testjoin, tj | Everyone | NOT_TESTED |  |  |

## Summary

- Total command entries: 88
- Working: 0
- Failing: 0
- Not tested: 88
- New commands added Mar 24, 2026: 13 (rankings, labels, locations, warleagues, warleague, bbclanrank, bbplayerrank, leagues, league, builderbaseleagues, builderbaseleague, leagueseasons, leagueseason)

## QA Tracker Notes

- `upgradecheck` is restored; include both slash and prefix paths in QA cycles.
- `milestone` now includes progress bars, remaining-to-next, and reached-count output.
- `inactive` now includes risk tiers and recommended leadership action.
- `rushhistory` now includes status/trend outlook and recommended next step.
- `raidstatus`/`raidsleft` now include urgency + action guidance.
- `whohavenotattacked`/`warmap`/`warpreview`/`opponentlineup` now include tactical guidance lines.
- `warperformance` now includes performance band and coaching next-step guidance.
- `warhistory` now includes momentum band and recommended leadership action.
- Use the isolated QA harness in `Discord bot testing/` for repeatable command-cycle checks.
- Update counts and statuses after each test cycle.
