# jira-pinger

A GitHub Actions job that runs every 15 minutes and sends you a Slack DM when a Jira issue you reported or watch moves into a finished status: Done, Cancelled or Canceled, Released, Deployed, Complete or Completed, Closed, Resolved, Integration, or Production.

## How it works

- `jira_pinger.py` queries Jira Cloud with JQL:
  `(reporter = currentUser() OR watcher = currentUser()) AND updated >= "-125m" ORDER BY updated DESC`
  The wider window covers a skipped hourly run, since GitHub sometimes skips scheduled jobs.
- It compares each issue's status with the status saved in `state.json`. A DM is sent only when an issue has just moved into one of the finished statuses. Edits, comments and other status changes don't trigger a DM.
- The first run after this change only records the current statuses and sends nothing.
- Once a day, on weekdays, the first run at or after 8 AM Central also sends a list of every open issue you reported or watch, grouped by Jira board and status. Nothing else is sent when nothing has finished. To change the hour or include weekends, edit `DIGEST_HOUR` or `DIGEST_WEEKDAYS_ONLY` in `jira_pinger.py`.
- Team channel (optional): if the `SLACK_CHANNEL_ID` secret is set, every Jira issue labelled `techsupport`, in any project and by any reporter, whose status just changed to a resolved status is also posted to that channel, batched into one message per run. The channel only ever gets these resolved posts, never the open-issues list. Its resolved statuses follow the team rules: Done, Resolved, Closed, Cancelled or Canceled, Released, Deployed, Complete or Completed, Published, and Won't Do. Integration and Production are not included there, although they still ping you personally.
  - The first run after the channel is turned on only records what is already resolved, so the channel is not flooded.
  - Email addresses in issue summaries are replaced with `[email]` in channel posts.
  - If a channel post fails (for example the app was never added to the channel), your personal DMs and saved state are not affected; the run is marked failed so GitHub emails you.
  - The Slack app must be a member of the channel (in the channel, type `/invite @Jira Pinger`).
- The job runs every 15 minutes because GitHub often skips scheduled runs. Saved statuses prevent repeat DMs.
- `state.json` keeps the last known status of every issue seen, and is committed back to the repo by the workflow only when it changes.
- Slack messages go to `chat.postMessage` with the `SLACK_USER_ID` as the channel.

## Required secrets

Add these at **Settings → Secrets and variables → Actions** in the repo:

| Secret name       | What it is                                                        |
| ----------------- | ----------------------------------------------------------------- |
| `JIRA_SITE`       | Your Jira base URL, for example `https://your-site.atlassian.net` |
| `JIRA_EMAIL`      | The email address on your Atlassian account                       |
| `JIRA_API_TOKEN`  | API token from id.atlassian.com under Security → API tokens       |
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
