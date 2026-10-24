# jira-pinger

A GitHub Actions job that watches Jira and keeps Ryan and the support team in Slack up to date. It does two separate things.

1. **Personal DMs to Ryan.** A Slack DM when a Jira issue he reported or watches moves into a finished status, plus one list of his open issues each weekday morning.
2. **Team channel posts.** A post in the support team channel when any Jira issue labelled `techsupport` is resolved, tagging the agents on it so they know to check it.

## When it runs

The job runs every hour. An outside timer (cron-job.org) starts the workflow on the hour, because GitHub's own scheduler often skips runs. A GitHub schedule is also set as a backup. Each run looks back about two hours, so a skipped run is covered, and saved statuses in `state.json` prevent repeat messages.

## Personal DMs

- Finished statuses that send a DM: Done, Cancelled or Canceled, Released, Deployed, Complete or Completed, Closed, Resolved, Integration, and Production.
- It compares each issue's status with the status saved in `state.json` and sends a DM only when an issue has just moved into one of those statuses. Edits and comments do nothing.
- Once a day, on weekdays, the first run at or after 8 AM Central sends a list of every open issue Ryan reported or watches, grouped by Jira board and status. To change this, edit `DIGEST_HOUR` or `DIGEST_WEEKDAYS_ONLY` in `jira_pinger.py`.
- Nothing is sent on a run where nothing finished and the daily list is not due.

## Team channel posts

- **What triggers a post:** a `techsupport` issue, in any project and by any reporter, that just moved to Done, Resolved, Closed, Cancelled or Canceled, Released, Deployed, Complete or Completed, Published, Won't Do, or Production. Integration does not post there, although it still pings Ryan personally.
- **What a post looks like:** a short list under the header "Resolved, please check:". Each line has the issue link, its summary and status, then an @-tag for each agent on it. Several issues resolved in the same run go in one post.
- **Who is tagged:** the reporter and watchers of the issue who are listed in the `SLACK_MENTIONS` secret. Everyone else, such as developers, is never tagged.
- **Former agents:** an entry like `"Former Agent": "->Current Agent"` in the secret re-routes that person's tickets to the current agent. Each person is tagged once per issue.
- **No repeats:** a ticket that posts at Production does not post again at Done. Announced issues are remembered for 14 days.
- **Quiet hours:** from Friday 10 PM to Monday 6 AM Central nothing is posted. Issues that resolve in that window are held, and only their keys are saved. They go out together in one post on the first run at or after 6 AM Central Monday, skipping any that were reopened. Ryan's personal DMs and daily list are not affected. To change the hours, edit `QUIET_FRIDAY_HOUR` and `QUIET_MONDAY_HOUR` in `jira_pinger.py`.
- **Customer emails:** email addresses in issue summaries are replaced with `[email]`.
- **First run:** when the channel is first turned on, the job only records what is already resolved, so the channel is not flooded.
- **Failures:** if a channel post fails (for example the Slack app was never added to the channel), personal DMs and saved state are not affected, and the run is marked failed so GitHub emails Ryan.
- The Slack app must be a member of the channel (in the channel, type `/invite @Jira Pinger`).

## Exceptions for partly fixed issues

List an issue under `strict_only` in `overrides.json` and Integration or Production no longer count as resolved for it, in both the DMs and the channel. It stays in the daily open-issues list, and posts only when it reaches a fully finished status (Done, Resolved, Closed, Released, Deployed, Complete or Completed, Cancelled, Published, Won't Do). Example: `"SHAR-8117": "Type 1 is resolved but type 2 is not"`.

## Notes

- The Actions logs of this public repo only ever show counts, never ticket numbers, summaries or people's names. `state.json` stores ticket keys and statuses only.
- `state.json` keeps the last known status of every issue seen, and the workflow commits it back to the repo only when it changes.
- Personal DMs go to `chat.postMessage` with `SLACK_USER_ID` as the channel. Team posts use `SLACK_CHANNEL_ID`.

## Required secrets

Add these at **Settings → Secrets and variables → Actions** in the repo:

| Secret name        | What it is                                                                                                                  |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `JIRA_SITE`        | Your Jira base URL, for example `https://your-site.atlassian.net`                                                           |
| `JIRA_EMAIL`       | The email address on your Atlassian account                                                                                 |
| `JIRA_API_TOKEN`   | API token from id.atlassian.com under Security → API tokens                                                                 |
| `SLACK_BOT_TOKEN`  | Slack bot token (`xoxb-...`) from your Slack app's OAuth page                                                               |
| `SLACK_USER_ID`    | Your Slack member ID (starts with `U` or `W`)                                                                               |
| `SLACK_CHANNEL_ID` | Optional. A Slack channel ID (starts with `C`) for resolved techsupport posts. Leave it out to keep the channel feature off. |
| `SLACK_MENTIONS`   | Optional. JSON of Jira display name to Slack member ID, for the agents tagged in channel posts. Without it nobody is tagged. |

## Run the workflow manually

- In GitHub, open the **Actions** tab, choose **jira-pinger**, then **Run workflow**.
- Or from a terminal with the GitHub CLI: `gh workflow run jira-pinger.yml`

## Run locally

Set the variables in your shell only. Do not write them to a file in this repo.

Preview messages without contacting Slack or changing `state.json`:

```bash
python jira_pinger.py --dry-run
```

Omit `--dry-run` to send real messages and update `state.json`.

The script needs Python 3.12 or later and the `requests` and `tzdata` packages: `pip install requests tzdata`.
