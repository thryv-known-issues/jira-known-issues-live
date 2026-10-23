# jira-pinger

A GitHub Actions job that runs every 15 minutes and sends you a Slack DM when a Jira issue you reported or watch moves into a finished status: Done, Cancelled or Canceled, Released, Deployed, Complete or Completed, Closed, Resolved, Integration, or Production.

## How it works

- `jira_pinger.py` queries Jira Cloud with JQL:
  `(reporter = currentUser() OR watcher = currentUser()) AND updated >= "-125m" ORDER BY updated DESC`
  The wider window covers a skipped hourly run, since GitHub sometimes skips scheduled jobs.
- It compares each issue's status with the status saved in `state.json`. A DM is sent only when an issue has just moved into one of the finished statuses. Edits, comments and other status changes don't trigger a DM.
- The first run after this change only records the current statuses and sends nothing.
- Once a day, on weekdays, the first run at or after 8 AM Central also sends a list of every open issue you reported or watch, grouped by Jira board and status. Nothing else is sent when nothing has finished. To change the hour or include weekends, edit `DIGEST_HOUR` or `DIGEST_WEEKDAYS_ONLY` in `jira_pinger.py`.
- Team channel (optional): if the `SLACK_CHANNEL_ID` secret is set, every Jira issue labelled `techsupport`, in any project and by any reporter, whose status just changed to a resolved status is also posted to that channel, batched into one message per run. The channel only ever gets these resolved posts, never the open-issues list. Its resolved statuses follow the team rules: Done, Resolved, Closed, Cancelled or Canceled, Released, Deployed, Complete or Completed, Published, Won't Do, and Production. Integration is not included there, although it still pings you personally. A ticket that posts at Production does not post again at Done (it is remembered for 14 days). Issues under `strict_only` skip Production too.
  - The first run after the channel is turned on only records what is already resolved, so the channel is not flooded.
  - Email addresses in issue summaries are replaced with `[email]` in channel posts.
  - If a channel post fails (for example the app was never added to the channel), your personal DMs and saved state are not affected; the run is marked failed so GitHub emails you.
  - Each line is short: the issue, its summary and status, then an @-tag for each agent on it who is in the optional `SLACK_MENTIONS` secret, followed by the header "Resolved, please check". Only the reporter and watchers listed in that secret are ever tagged.
  - `SLACK_MENTIONS` is JSON of Jira display name to Slack member ID. A value like `"->Scott Savage"` re-routes a former agent's tickets to that person's ID (each person is tagged once). Without the secret nobody is tagged.
  - Quiet hours: from Friday 10 PM to Monday 6 AM Central, nothing is posted to the channel. Issues that resolve in that window are held (only their keys are saved in `state.json`) and go out together in one post on the first run at or after 6 AM Central Monday, skipping any that are no longer resolved. Your personal DMs and daily list are not affected. To change the hours, edit `QUIET_FRIDAY_HOUR` and `QUIET_MONDAY_HOUR` in `jira_pinger.py`.
  - The Slack app must be a member of the channel (in the channel, type `/invite @Jira Pinger`).
- Exceptions for partly fixed issues: list an issue under `strict_only` in `overrides.json` and Integration or Production no longer count as resolved for it. It stays in your daily open-issues list, and you get your DM only when it reaches a truly finished status (Done, Resolved, Closed, Released, Deployed, Complete or Completed, Cancelled, Published, Won't Do). To add one, edit `overrides.json` in the repo, for example `"SHAR-8117": "why it is an exception"`.
- One-time test snapshot: if `overrides.json` has a `people_snapshot` label that is new to `state.json`, the next run sends a DM to you (only you, never a channel, and with no @-mentions) listing who is reporter or watcher on each open techsupport issue. The message says it is a one-time test run. The label is then recorded in `state.json` so it never repeats; change the label to run it again on purpose.
- The Actions logs of this public repo only ever show counts, never ticket numbers, summaries or people's names.
- The job runs every 15 minutes because GitHub often skips scheduled runs. Saved statuses prevent repeat DMs.
- `state.json` keeps the last known status of every issue seen, and is committed back to the repo by the workflow only when it changes.
- Slack messages go to `chat.postMessage` with the `SLACK_USER_ID` as the channel.

## Required secrets

Add these at **Settings â†’ Secrets and variables â†’ Actions** in the repo:

| Secret name       | What it is                                                        |
| ----------------- | ----------------------------------------------------------------- |
| `JIRA_SITE`       | Your Jira base URL, for example `https://your-site.atlassian.net` |
| `JIRA_EMAIL`      | The email address on your Atlassian account                       |
| `JIRA_API_TOKEN`  | API token from id.atlassian.com under Security â†’ API tokens       |
| `SLACK_BOT_TOKEN` | Slack bot token (`xoxb-...`) from your Slack app's OAuth page     |
| `SLACK_USER_ID`   | Your Slack member ID (starts with `U` or `W`)                     |
| `SLACK_CHANNEL_ID`| Optional. A Slack channel ID (starts with `C`) for resolved techsupport posts. Leave it out to keep the channel feature off. |

## Run the workflow manually

- In GitHub, open the **Actions** tab, choose **jira-pinger**, then **Run workflow**.
- Or from a terminal with the GitHub CLI: `gh workflow run jira-pinger.yml`

## Run locally

Set the five variables in your shell only. Do not write them to a file in this repo.

Preview messages without contacting Slack or changing `state.json`:

```bash
python jira_pinger.py --dry-run
```

Omit `--dry-run` to send real DMs and update `state.json`.

The script needs Python 3.12 or later and the `requests` package: `pip install requests`.



