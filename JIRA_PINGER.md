# jira-pinger

A GitHub Actions job that posts to the support team's Slack channel when a Jira issue labelled `techsupport` is resolved, and tags the agents on it so they know to check it.

## What it does

- **What triggers a post:** a `techsupport` issue, in any project and by any reporter, that just moved to Done, Resolved, Closed, Cancelled or Canceled, Released, Deployed, Complete or Completed, Published, Won't Do, or Production. Integration does not post.
- **What a post looks like:** a short list under the header "Resolved, please check:". Each line has the issue link, its summary and status, then an @-tag for each agent on it. Several issues resolved in the same run go in one post.
- **Fix incoming heads-up:** when a `techsupport` issue reaches Integration (the fix is built and on its way), a separate post goes out under the header ":eyes-jiggle: Fix incoming, keep an eye out:", tagging the same agents. Each issue gets one heads-up and later its normal resolved post. Issues under `strict_only` and issues that already posted as resolved get no heads-up. It needs a custom Slack emoji named `eyes-jiggle`. The heads-up follows the same quiet hours, and weekend heads-ups go out Monday only if the issue is still at Integration.
- **Who is tagged:** the reporter and watchers of the issue who are listed in the `SLACK_MENTIONS` secret. Everyone else, such as developers, is never tagged.
- **Former agents:** an entry like `"Former Agent": "->Current Agent"` in the secret re-routes that person's tickets to the current agent. Each person is tagged once per issue.
- **No repeats:** a ticket that posts at Production does not post again at Done. Announced issues are remembered for 14 days.
- **Quiet hours:** from Friday 10 PM to Monday 6 AM Central nothing is posted. Issues that resolve in that window are held, and only their keys are saved. They go out together in one post on the first run at or after 6 AM Central Monday, skipping any that were reopened. To change the hours, edit `QUIET_FRIDAY_HOUR` and `QUIET_MONDAY_HOUR` in `jira_pinger.py`.
- **Customer emails:** email addresses in issue summaries are replaced with `[email]`.

## When it runs

The job runs every hour. An outside timer (cron-job.org) starts the workflow on the hour, because GitHub's own scheduler often skips runs. A GitHub schedule is also set as a backup. Each run looks back about two hours, so a skipped run is covered, and the saved list of announced issues in `state.json` prevents repeat posts.

## Exceptions for partly fixed issues

List an issue under `strict_only` in `overrides.json` and Production no longer counts as resolved for it. It posts only when it reaches a fully finished status (Done, Resolved, Closed, Released, Deployed, Complete or Completed, Cancelled, Published, Won't Do). Example: `"SHAR-8117": "Type 1 is resolved but type 2 is not"`.

## Setup notes

- The Slack app must be a member of the channel (in the channel, type `/invite @Jira Pinger`).
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
