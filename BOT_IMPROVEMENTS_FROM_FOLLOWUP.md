# CC2 Bot Improvements From Follow-Up Notes

Date: 2026-03-20
Source: COMMAND_FOLLOWUP_NOTES.md
Status: Planned (not implemented)

Purpose:
- Consolidate requested improvements from follow-up notes into a single implementation file.
- Keep this as a direct execution checklist for future development.

## Priority Improvement Backlog

1. Fix upgradecheck
- Investigate why cc2 upgradecheck fails.
- Rework output/logic so cc2 upgradecheck behaves correctly and consistently.
- Confirm slash and prefix behavior both work.
- Re-test with clan filter and default all-clans mode.

2. Revise upgradepriority system
- Review and redesign the upgradepriority logic/output.
- Improve recommendation quality and clarity for users.

3. Restrict takesnapshot permissions
- Make cc2 takesnapshot admin/leadership only.
- Ensure both slash and prefix paths enforce the same permission rule.

4. Improve challenge command
- Redesign challenge command messaging so it is clear and meaningful.
- Clarify progress, goals, and what users should do next.

5. Improve compare command
- Improve compare output quality and decision value.
- Make key differences easier to read and act on.

6. Improve milestone quality
- Keep current milestone command, but improve accuracy and output quality.
- Review milestone thresholds and presentation clarity.

7. Rework inactive command
- Fix inactive detection reliability and output clarity.
- Verify inactivity thresholds and edge cases.

8. Rework rush calculation
- Revisit rush-scoring formula and weighting.
- Ensure consistency across all commands that use rush metrics.

9. Rework clan poll command
- Add single-choice and multi-choice poll modes.
- Change poll duration UX from minutes to hours.

10. Rework promotion suggestions
- Improve recommendation logic and confidence.
- Make reasoning output clearer for leadership decisions.

11. Rework raid commands
- Review raid command outputs, calculations, and consistency.
- Improve clarity of status/history/trend messaging.

12. Rework upgradepriority command
- Redesign upgradepriority logic/output for better recommendations.
- Align priority output with practical in-game upgrade flow.

13. Rework war commands
- Review war command reliability and data quality.
- Improve war output clarity for decision-making.

14. Rework war attack data sending interface
- Redesign war attack data sending flow and interface logic.
- Ensure delivery is reliable, clear, and actionable.
- Status: Implemented (2026-03-20)

15. Add opponent lineup feature
- Add a feature to display opponent lineup details.
- Ensure lineup view is readable and useful for war planning.
- Status: Implemented (2026-03-20)

16. Auto-remove bot-pinned messages after 24 hours
- Add cleanup logic for messages pinned by the bot.
- Ensure auto-unpin/remove runs safely after 24 hours.
- Status: Implemented (2026-03-20)

17. Add periodic war-started active-war reminder
- Send war started message once every 12 hours while any war is active.
- Prevent duplicate spam within the same 12-hour window.
- Status: Implemented (2026-03-20)

18. Add new admin-only event creation command
- Create a command for admins to create events.
- Ensure strict admin/leadership permission enforcement.
- Status: Implemented (2026-03-20)

19. Expand top ranking categories and explain ranking formula
- Add explanation below output for how rank is calculated.
- Keep trophies category.
- Add more ranking options, including:
  - trophies
  - war stars
  - cwl stars
  - top loot
- Ensure category selection works consistently for both slash and prefix usage.
- Status: Implemented (2026-03-20)

## Acceptance Checks

- cc2 upgradecheck returns valid results for all clans, single clan, and different min hero values.
- cc2 upgradepriority recommendations are clear, accurate, and actionable.
- cc2 takesnapshot is blocked for non-admin/non-leadership users.
- cc2 challenge output is understandable at a glance.
- cc2 compare output is clearer and more useful.
- cc2 milestone output is stable and accurate.
- cc2 inactive command is reliable and accurate.
- rush calculations are consistent and validated across commands.
- clan poll supports single-choice and multi-choice with hour-based duration.
- promotion suggestions are accurate and well-explained.
- raid commands are consistent, clear, and reliable.
- upgradepriority recommendations are consistent and actionable.
- war command outputs are reliable and clear.
- war attack data sending interface is clear and reliable.
- opponent lineup feature is accurate and useful for planning.
- bot-pinned messages are automatically removed after 24 hours.
- war-started reminders are sent at most once per 12 hours during active war.
- admin-only event creation command works and blocks non-admin users.
- cc2 top supports multiple categories and explains ranking logic.

## Note

Do not auto-implement from this file.
Use it as a planned backlog and execute items intentionally.
