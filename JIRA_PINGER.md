# jira-pinger

An hourly GitHub Actions job that sends you a Slack DM when a Jira issue you reported or watch moves into a finished status: Done, Cancelled or Canceled, Released, Deployed, Complete or Completed, Closed, or Resolved.

## How it works

- `jira_pinger.py` queries Jira Cloud with JQL:
  `(reporter = currentUser() OR watcher = currentUser()) AND updated >= "-125m" ORDER BY updated DESC`
  The wider window covers a skipped hourly run, since GitHub sometimes skips scheduled jobs.
- It compares each issue's status with the status saved in `state.json`. A DM is sent only when an issue has just moved into one of the finished statuses. Edits, comments and other status changes don't trigger a DM.
- The first run after this change only records the current statuses and sends nothing.
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
