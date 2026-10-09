#!/usr/bin/env python3
"""DM a Slack user when Jira issues they reported or watch move into a finished status.

When nothing has finished, it periodically sends a digest of every open issue they created or watch.
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

REQUIRED_ENV = ["JIRA_SITE", "JIRA_EMAIL", "JIRA_API_TOKEN", "SLACK_BOT_TOKEN", "SLACK_USER_ID"]
# The 125 minute lookback covers a skipped run. Saved statuses in state.json prevent repeat DMs.
JQL = '(reporter = currentUser() OR watcher = currentUser()) AND updated >= "-125m" ORDER BY updated DESC'
OPEN_JQL = "(reporter = currentUser() OR watcher = currentUser()) AND statusCategory != Done ORDER BY updated DESC"
CREATED_OPEN_JQL = "reporter = currentUser() AND statusCategory != Done"
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
    "integration",
    "production",
}
# The job runs every 15 minutes, but the open-issues digest goes out at most this often. Change it here.
DIGEST_INTERVAL = timedelta(hours=4)
MAX_DIGEST_ISSUES = 100
SUMMARY_MAX_CHARS = 90
NO_OPEN_MESSAGE = "No data: you have no open Jira issues that you created or are watching."
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


def fetch_jira_issues(config, jql=JQL):
    url = f"{config['JIRA_SITE']}/rest/api/3/search/jql"
    auth = (config["JIRA_EMAIL"], config["JIRA_API_TOKEN"])
    params = {"jql": jql, "fields": "summary,status", "maxResults": 100}
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


def save_state(statuses, last_digest=None):
    # Keep every issue ever seen, so a later edit to an already-finished issue is not mistaken for a new transition.
    state = {"statuses": statuses}
    if last_digest:
        state["last_digest_dm"] = last_digest
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def digest_due(last_iso):
    if not last_iso:
        return True
    try:
        last = datetime.fromisoformat(last_iso)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - last >= DIGEST_INTERVAL


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


def escape_slack(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_message(config, key, summary, status):
    return f"*Jira:* {key} {summary}\nStatus: {status}\n{config['JIRA_SITE']}/browse/{key}"


def build_digest(config, issues, created_keys):
    """Summarize open issues in two sections (created by the user, watched by the user), grouped by status."""
    open_issues = [i for i in issues if not is_finished(i["fields"]["status"]["name"])]
    if not open_issues:
        return NO_OPEN_MESSAGE

    shown = open_issues[:MAX_DIGEST_ISSUES]
    sections = {"Created by you": defaultdict(list), "Watching": defaultdict(list)}
    for issue in shown:
        key = issue["key"]
        status = issue["fields"]["status"]["name"]
        summary = issue["fields"].get("summary") or ""
        if len(summary) > SUMMARY_MAX_CHARS:
            summary = summary[:SUMMARY_MAX_CHARS].rstrip() + "..."
        summary = escape_slack(summary)
        section = "Created by you" if key in created_keys else "Watching"
        sections[section][status].append(f"- <{config['JIRA_SITE']}/browse/{key}|{key}> {summary}")

    lines = [f"*Open Jira issues pending an outcome ({len(open_issues)})*"]
    for title, by_status in sections.items():
        if not by_status:
            continue
        count = sum(len(items) for items in by_status.values())
        lines.append(f"\n*{title} ({count})*")
        for status, items in sorted(by_status.items(), key=lambda pair: -len(pair[1])):
            lines.append(f"_{escape_slack(status)}_ ({len(items)})")
            lines.extend(items)
    if len(open_issues) > len(shown):
        lines.append(f"\n...and {len(open_issues) - len(shown)} more")
    return "\n".join(lines)


def send_slack_dm(config, text):
    resp = requests.post(
        SLACK_POST_URL,
        headers={"Authorization": f"Bearer {config['SLACK_BOT_TOKEN']}"},
        json={
            "channel": config["SLACK_USER_ID"],
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        },
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
    last_digest = state.get("last_digest_dm")
    hits = find_transitions(issues, previous)
    print(f"Found {len(issues)} recent issue(s); {len(hits)} moved into a finished status.")

    for key, summary, status in hits:
        message = build_message(config, key, summary, status)
        if args.dry_run:
            print(f"--- {key} ---\n{message}")
        else:
            send_slack_dm(config, message)

    if not hits:
        if digest_due(last_digest):
            open_issues = fetch_jira_issues(config, OPEN_JQL)
            created_keys = {issue["key"] for issue in fetch_jira_issues(config, CREATED_OPEN_JQL)}
            digest = build_digest(config, open_issues, created_keys)
            print(digest)
            if not args.dry_run:
                send_slack_dm(config, digest)
                last_digest = datetime.now(timezone.utc).isoformat()
        else:
            print("Nothing finished, and the open-issues digest is not due yet.")

    if args.dry_run:
        print("Dry run: Slack was not contacted and state.json was not changed.")
        return

    save_state({**previous, **current}, last_digest)


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        sys.exit(f"Request failed: {exc}")
    except RuntimeError as exc:
        sys.exit(str(exc))
