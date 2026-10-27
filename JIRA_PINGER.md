# Jira Watcher

(Formerly Jira Pinger. The GitHub repo, workflow and script keep their old names, `jira-pinger`.)

A GitHub Actions job that posts to the support team's Slack channel when a Jira issue labelled `techsupport` is resolved, and tags the agents on it so they know to check it.

## What it does

- **What triggers a post:** a `techsupport` issue, in any project and by any reporter, that just moved to Done, Resolved, Closed, Cancelled or Canceled, Released, Deployed, Complete or Completed, Published, Won't Do, or Production. Integration does not post.
- **What a post looks like:** a short list under the header "Resolved, please check:". Each line has the issue link, its summary and status, then an @-tag for each agent on it. Several issues resolved in the same run go in one post.
- **Fix incoming heads-up:** when a `techsupport` issue reaches Integration (the fix is built and on its way), a separate post goes out under the header ":eyes: Fix incoming, keep an eye out:", tagging the same agents. Each issue gets one heads-up and later its normal resolved post. Issues under `strict_only` and issues that already posted as resolved get no heads-up. Weekend heads-ups are included in the Monday recap while the issue is still at Integration.
- **Who is tagged:** the reporter and watchers of the issue who are listed in the `SLACK_MENTIONS` secret. Everyone else, such as developers, is never tagged.
- **Former agents:** an entry like `"Former Agent": "->Current Agent"` in the secret re-routes that person's tickets to the current agent. Each person is tagged once per issue.
- **No repeats:** a ticket that posts at Production does not post again at Done. Announced issues are remembered for 14 days.
- **Weekend recap:** the job keeps running every hour all weekend and posts as usual. Whatever it posts from Friday 10 PM to Monday 6 AM Central is also logged (ticket keys only), and the first run at or after 6 AM Central Monday posts one bundled recap of it under ":spiral_calendar_pad: Weekend recap, in case anything slipped:", with the resolved tickets and the fix-incoming tickets in two sections and the same tags. Tickets reopened or moved on since then are left out, and nothing is posted if there was no weekend activity. To change the window, edit `WEEKEND_START_HOUR` and `WEEKEND_END_HOUR` in `jira_pinger.py`.
- **Customer emails:** email addresses in issue summaries are replaced with `[email]`.

## Table watch (dropped-row alerts)

Several agents add rows to the affected-accounts table inside the description of the same `techsupport` Jira ticket. Jira saves the whole description, so when two people save at nearly the same time the later save can wipe out the earlier one's row. Table watch reads each ticket's change history, compares every saved version of each table with the one before it, and when a row has disappeared it sends a short Slack message that tags the agent named in that row and shows the exact row, so they can check it and re-add it. It never edits Jira and uses no AI.

- **Who gets told:** the rep named in the Agent column of the dropped row gets a direct message from the bot with the exact row. If no rep can be matched, or more than 10 rows disappear in one save (a clean-up or restructure), one untagged message goes only to the owner.
- **How often:** every 15 minutes. The status pings, channel posts and daily list still run about once an hour (the script does the hourly work when 55 minutes have passed since the last one; to force it, use **Run workflow** with `force_full` ticked).
- **Switches** (repository variables, Settings, Secrets and variables, Actions, Variables tab): `TABLE_WATCH_ENABLED` = `true` turns it on. It stays in dry run, where alerts go only to the owner's DM labeled with who they would have gone to, until `TABLE_WATCH_DRY_RUN` = `false`. Optional: `TABLE_WATCH_CHANNEL` (post in that channel instead of DMs) and `TABLE_WATCH_LABEL` (default `techsupport`). The public Actions log only ever shows counts.
- **What alerts:** any row that disappears. Two clear cleanups stay quiet: a duplicate row removed while another row for the same Thryv ID stays, and a row whose Resolved column is filled. More than half the table removed in one save posts one untagged message for a human to check.
- **Tagging:** by the Agent column, using the `SLACK_MENTIONS` secret. Add each spelling that appears in tables as its own entry (for example `"Barb Larrosa": "U..."`). Matching ignores case and accents. An unmapped name posts without a tag.
- **No repeats:** each alert is remembered by a short hash, so account IDs and ticket numbers are not saved in `state.json`. The first time a ticket is seen it is only recorded, so old history is never alerted.
- **Safe by design:** an error inside table watch is reported by type only and never stops the pings above it.
- Code lives in `table_watch/`, tests in `tests/test_table_watch.py` (`python -m unittest tests.test_table_watch`).

## When it runs

An outside timer (cron-job.org) starts the workflow every 15 minutes, because GitHub's own scheduler often skips runs. A GitHub schedule is also set as a backup. Table watch runs on every pass. Everything else (status pings, channel posts, recap, daily list) runs about once an hour. Each run looks back about two hours, so a skipped run is covered, and the saved list of announced issues in `state.json` prevents repeat posts.

## Exceptions for partly fixed issues

List an issue under `strict_only` in `overrides.json` and Production no longer counts as resolved for it. It posts only when it reaches a fully finished status (Done, Resolved, Closed, Released, Deployed, Complete or Completed, Cancelled, Published, Won't Do). Example: `"SHAR-8117": "Type 1 is resolved but type 2 is not"`.

## Setup notes

- The Slack app must be a member of the channel (in the channel, type `/invite @Jira Watcher`).
- The first run after the channel is turned on only records what is already resolved, so the channel is not flooded.
- If a post fails (for example the app was never added to the channel), the run is marked failed so GitHub emails the owner. Nothing is lost, and the issue is retried on the next run.
- The Actions logs of this public repo only ever show counts, never ticket numbers, summaries or people's names. `state.json` stores ticket keys and statuses only.

## Required secrets

Add these at **Settings → Secrets and variables → Actions** in the repo:

| Secret name        | What it is                                                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `JIRA_SITE`        | The Jira base URL, for example `https://your-site.atlassian.net`                                                     |
| `JIRA_EMAIL`       | The email address on the Atlassian account used for the API                                                          |
| `JIRA_API_TOKEN`   | API token from id.atlassian.com under Security → API tokens                                                          |
| `SLACK_BOT_TOKEN`  | Slack bot token (`xoxb-...`) from the Slack app's OAuth page                                                         |
| `SLACK_USER_ID`    | The owner's Slack member ID (starts with `U` or `W`), required by the script                                         |
| `SLACK_CHANNEL_ID` | The Slack channel ID (starts with `C`) the posts go to. Leave it out to turn channel posts off.                      |
| `SLACK_MENTIONS`   | JSON of Jira display name to Slack member ID, for the agents tagged in posts. Without it nobody is tagged.           |

## Run the workflow manually

- In GitHub, open the **Actions** tab, choose **jira-pinger**, then **Run workflow**.
- Or from a terminal with the GitHub CLI: `gh workflow run jira-pinger.yml`

## Run locally

Set the variables in your shell only. Do not write them to a file in this repo.

Preview messages without contacting Slack or changing `state.json`:

```bash
python jira_pinger.py --dry-run
```

The script needs Python 3.12 or later and the `requests` and `tzdata` packages: `pip install requests tzdata`.
