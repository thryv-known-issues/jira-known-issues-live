#!/usr/bin/env python3
"""DM a Slack user about Jira issues they reported or watch that changed recently."""

import argparse
import json
import os
import sys
from pathlib import Path

import requests

REQUIRED_ENV = ["JIRA_SITE", "JIRA_EMAIL", "JIRA_API_TOKEN", "SLACK_BOT_TOKEN", "SLACK_USER_ID"]
JQL = '(reporter = currentUser() OR watcher = currentUser()) AND updated >= "-65m" ORDER BY updated DESC'
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
    params = {"jql": JQL, "fields": "summary,updated", "maxResults": 100}
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


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_message(config, key, summary):
    return f"*Jira:* {key} {summary}\n{config['JIRA_SITE']}/browse/{key}"


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
    previous = load_state()

    # Track every current issue so state.json always reflects the latest results.
    current = {}
    to_notify = []
    for issue in issues:
        key = issue["key"]
        updated = issue["fields"]["updated"]
        current[key] = updated
        if previous.get(key) != updated:
            to_notify.append((key, issue["fields"].get("summary", "")))

    print(f"Found {len(issues)} recent issue(s); {len(to_notify)} new or changed.")

    for key, summary in to_notify:
        message = build_message(config, key, summary)
        if args.dry_run:
            print(f"--- {key} ---\n{message}")
        else:
            send_slack_dm(config, message)

    if args.dry_run:
        print("Dry run: Slack was not contacted and state.json was not changed.")
        return

    save_state(current)


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        sys.exit(f"Request failed: {exc}")
    except RuntimeError as exc:
        sys.exit(str(exc))
