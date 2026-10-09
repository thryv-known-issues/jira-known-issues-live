#!/usr/bin/env python3
"""DM a Slack user when Jira issues they reported or watch move into a finished status."""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

REQUIRED_ENV = ["JIRA_SITE", "JIRA_EMAIL", "JIRA_API_TOKEN", "SLACK_BOT_TOKEN", "SLACK_USER_ID"]
# The 125 minute lookback covers a skipped hourly run. Saved statuses in state.json prevent repeat DMs.
JQL = '(reporter = currentUser() OR watcher = currentUser()) AND updated >= "-125m" ORDER BY updated DESC'
# Compared case-insensitively against the Jira status name.
TARGET_STATUSES = {
    "done",
    "cancelled",
    "canceled",
    "released",
    "deployed",
    "complete",
    "completed",
    "closed",
    "resolved",
}
# The job runs every 15 minutes so skipped GitHub runs don't matter, but "No data" goes out at most hourly.
NO_DATA_INTERVAL = timedelta(minutes=55)
NO_DATA_MESSAGE = "No data: none of your Jira issues moved into a finished status in the last 2 hours."
SLACK_POST_URL = "https://slack.com/api/chat.postMessage"
STATE_FILE = Path(__file__).resolve().parent / "state.json"
HTTP_TIMEOUT = 30


def load_config():
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")
    config = {name: os.environ[name] for name in REQUIRED_ENV}
    config["JIRA_SITE"] = config["JIRA_SITE"].rstrip("/")
    return config


def fetch_jira_issues(config):
    url = f"{config['JIRA_SITE']}/rest/api/3/search/jql"
    auth = (config["JIRA_EMAIL"], config["JIRA_API_TOKEN"])
    params = {"jql": JQL, "fields": "summary,status", "maxResults": 100}
    issues = []
    while True:
        resp = requests.get(url, params=params, auth=auth, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        issues.extend(data.get("issues", []))
        next_token = data.get("nextPageToken")
        if not next_token or data.get("isLast"):
            break
        params["nextPageToken"] = next_token
    return issues


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(statuses, last_no_data=None):
    # Keep every issue ever seen, so a later edit to an already-finished issue is not mistaken for a new transition.
    state = {"statuses": statuses}
    if last_no_data:
        state["last_no_data_dm"] = last_no_data
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def no_data_due(last_iso):
    if not last_iso:
        return True
    try:
        last = datetime.fromisoformat(last_iso)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - last >= NO_DATA_INTERVAL


def is_finished(status):
    return status.strip().lower() in TARGET_STATUSES


def find_transitions(issues, previous_statuses):
    """Return (key, summary, status) for issues that just moved into a finished status."""
    hits = []
    for issue in issues:
        key = issue["key"]
        status = issue["fields"]["status"]["name"]
        before = previous_statuses.get(key)
        if is_finished(status) and (before is None or not is_finished(before)):
            hits.append((key, issue["fields"].get("summary", ""), status))
    return hits


def build_message(config, key, summary, status):
    return f"*Jira:* {key} {summary}\nStatus: {status}\n{config['JIRA_SITE']}/browse/{key}"


def send_slack_dm(config, text):
    resp = requests.post(
        SLACK_POST_URL,
        headers={"Authorization": f"Bearer {config['SLACK_BOT_TOKEN']}"},
        json={"channel": config["SLACK_USER_ID"], "text": text},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"Slack chat.postMessage failed: {body.get('error', 'unknown error')}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print messages without sending to Slack or changing state.json",
    )
    args = parser.parse_args()

    config = load_config()
    issues = fetch_jira_issues(config)
    state = load_state()
    current = {issue["key"]: issue["fields"]["status"]["name"] for issue in issues}

    if "statuses" not in state:
        # First run of this version: record statuses without DMing, so old issues don't flood Slack.
        print(f"First run: recorded {len(current)} issue status(es) without sending DMs.")
        if not args.dry_run:
            save_state(current)
        return

    previous = state["statuses"]
    last_no_data = state.get("last_no_data_dm")
    hits = find_transitions(issues, previous)
    print(f"Found {len(issues)} recent issue(s); {len(hits)} moved into a finished status.")

    for key, summary, status in hits:
        message = build_message(config, key, summary, status)
        if args.dry_run:
            print(f"--- {key} ---\n{message}")
        else:
            send_slack_dm(config, message)

    if not hits:
        print(NO_DATA_MESSAGE)
        if not args.dry_run and no_data_due(last_no_data):
            send_slack_dm(config, NO_DATA_MESSAGE)
            last_no_data = datetime.now(timezone.utc).isoformat()

    if args.dry_run:
        print("Dry run: Slack was not contacted and state.json was not changed.")
        return

    save_state({**previous, **current}, last_no_data)


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        sys.exit(f"Request failed: {exc}")
    except RuntimeError as exc:
        sys.exit(str(exc))
