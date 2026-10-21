#!/usr/bin/env python3
"""DM a Slack user when Jira issues they reported or watch move into a finished status.

When nothing has finished, it sends a digest of every open issue they created or watch, at most once a day.

If SLACK_CHANNEL_ID is set, any Jira issue labelled "techsupport" that is resolved (by the team's rules) is also posted to
that Slack channel. The channel only ever gets resolved issues, never the open-issues digest.
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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
# Team channel: every "techsupport" issue (any project, any reporter) whose status just changed to a resolved status.
# These are the team's resolved statuses (the Known System Issues rules), so Integration and Production are NOT here,
# even though they ping you personally. Cancelled is included; the post shows the status word so readers can tell.
CHANNEL_JQL = 'labels = "techsupport" AND status CHANGED AFTER "-125m" ORDER BY updated DESC'
CHANNEL_STATUSES = {
    "done",
    "resolved",
    "closed",
    "cancelled",
    "canceled",
    "released",
    "deployed",
    "complete",
    "completed",
    "published",
    "won't do",
}
CHANNEL_MAX_PER_MESSAGE = 25
CHANNEL_SUMMARY_CHARS = 110
CHANNEL_KEEP_DAYS = 14
EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# The job runs every 15 minutes, but the open-issues digest goes out once a day: on the first run at or after this
# hour (Central time). Set DIGEST_WEEKDAYS_ONLY to False to get it on weekends too.
try:
    LOCAL_TZ = ZoneInfo("America/Chicago")
except Exception:  # no time zone database installed: fall back to a fixed Central Standard offset (an hour off in summer)
    LOCAL_TZ = timezone(timedelta(hours=-6), "CST")
DIGEST_HOUR = 8
DIGEST_WEEKDAYS_ONLY = True
MAX_DIGEST_ISSUES = 100
SUMMARY_MAX_CHARS = 90
NO_OPEN_MESSAGE = "No data: you have no open Jira issues that you created or are watching."
SLACK_POST_URL = "https://slack.com/api/chat.postMessage"
STATE_FILE = Path(__file__).resolve().parent / "state.json"
OVERRIDES_FILE = Path(__file__).resolve().parent / "overrides.json"
HTTP_TIMEOUT = 30


def load_config():
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")
    config = {name: os.environ[name] for name in REQUIRED_ENV}
    config["JIRA_SITE"] = config["JIRA_SITE"].rstrip("/")
    config["SLACK_CHANNEL_ID"] = os.environ.get("SLACK_CHANNEL_ID", "").strip()   # optional: turns on team-channel posts
    return config


def fetch_jira_issues(config, jql=JQL):
    url = f"{config['JIRA_SITE']}/rest/api/3/search/jql"
    auth = (config["JIRA_EMAIL"], config["JIRA_API_TOKEN"])
    params = {"jql": jql, "fields": "summary,status,project", "maxResults": 100}
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
        # utf-8-sig also accepts a file saved with a byte-order mark, which some Windows editors add.
        return json.loads(STATE_FILE.read_text(encoding="utf-8-sig"))
    return {}


def save_state(statuses, last_digest=None, channel_announced=None):
    # Keep every issue ever seen, so a later edit to an already-finished issue is not mistaken for a new transition.
    state = {"statuses": statuses}
    if last_digest:
        state["last_digest_dm"] = last_digest
    if channel_announced is not None:
        state["channel_announced"] = channel_announced
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def digest_due(last_iso, now=None):
    """True once per Central-time day: from DIGEST_HOUR on, and only if no digest has gone out yet today."""
    now = (now or datetime.now(timezone.utc)).astimezone(LOCAL_TZ)
    if DIGEST_WEEKDAYS_ONLY and now.weekday() >= 5:
        return False
    if now.hour < DIGEST_HOUR:
        return False
    if not last_iso:
        return True
    try:
        last = datetime.fromisoformat(last_iso).astimezone(LOCAL_TZ)
    except ValueError:
        return True
    return last.date() < now.date()


def load_strict_only():
    """Issues listed under "strict_only" in overrides.json are only treated as resolved once they reach a truly
    finished status (the team list), not Integration or Production. Use it for partly fixed issues."""
    if not OVERRIDES_FILE.exists():
        return {}
    try:
        data = json.loads(OVERRIDES_FILE.read_text(encoding="utf-8-sig"))
        return {str(key).strip().upper(): note for key, note in data.get("strict_only", {}).items()}
    except (ValueError, AttributeError) as exc:
        print(f"WARNING: could not read {OVERRIDES_FILE.name} ({exc}); ignoring it.")
        return {}


STRICT_ONLY = load_strict_only()


def is_finished(status, key=None):
    status = status.strip().lower()
    if key and key.upper() in STRICT_ONLY:
        return status in CHANNEL_STATUSES
    return status in TARGET_STATUSES


def find_transitions(issues, previous_statuses):
    """Return (key, summary, status) for issues that just moved into a finished status."""
    hits = []
    for issue in issues:
        key = issue["key"]
        status = issue["fields"]["status"]["name"]
        before = previous_statuses.get(key)
        if is_finished(status, key) and (before is None or not is_finished(before, key)):
            hits.append((key, issue["fields"].get("summary", ""), status))
    return hits


def escape_slack(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_message(config, key, summary, status):
    return f"*Jira:* {key} {summary}\nStatus: {status}\n{config['JIRA_SITE']}/browse/{key}"


def build_digest(config, issues, created_keys):
    """Summarize open issues grouped by Jira board (project), then by status. Watched-only issues are tagged."""
    open_issues = [i for i in issues if not is_finished(i["fields"]["status"]["name"], i["key"])]
    if not open_issues:
        return NO_OPEN_MESSAGE

    shown = open_issues[:MAX_DIGEST_ISSUES]
    boards = {}
    for issue in shown:
        key = issue["key"]
        fields = issue["fields"]
        status = fields["status"]["name"]
        project = fields.get("project") or {}
        board_key = project.get("key") or key.split("-")[0]
        board_name = project.get("name") or board_key
        summary = fields.get("summary") or ""
        if len(summary) > SUMMARY_MAX_CHARS:
            summary = summary[:SUMMARY_MAX_CHARS].rstrip() + "..."
        tag = "" if key in created_keys else " (watching)"
        line = f"- <{config['JIRA_SITE']}/browse/{key}|{key}> {escape_slack(summary)}{tag}"
        board = boards.setdefault(board_key, {"name": board_name, "by_status": defaultdict(list)})
        board["by_status"][status].append(line)

    def board_size(board):
        return sum(len(items) for items in board["by_status"].values())

    lines = [f"*Open Jira issues pending an outcome ({len(open_issues)})*"]
    # Biggest boards first, then alphabetical; within a board, statuses with the most issues first.
    for board_key, board in sorted(boards.items(), key=lambda pair: (-board_size(pair[1]), pair[1]["name"].lower())):
        title = board_key if board["name"] == board_key else f"{escape_slack(board['name'])} ({board_key})"
        lines.append(f"\n*{title}* - {board_size(board)}")
        for status, items in sorted(board["by_status"].items(), key=lambda pair: (-len(pair[1]), pair[0].lower())):
            lines.append(f"_{escape_slack(status)}_")
            lines.extend(items)
    if len(open_issues) > len(shown):
        lines.append(f"\n...and {len(open_issues) - len(shown)} more")
    return "\n".join(lines)


def channel_resolved(issues):
    """(key, summary, status) for techsupport issues now in one of the team's resolved statuses."""
    return [
        (i["key"], i["fields"].get("summary") or "", i["fields"]["status"]["name"])
        for i in issues
        if i["fields"]["status"]["name"].strip().lower() in CHANNEL_STATUSES
    ]


def mask_emails(text):
    """Ticket summaries sometimes contain customer email addresses; keep them out of a shared channel."""
    return EMAIL_RE.sub("[email]", text)


def build_channel_batches(config, items):
    """Yield (message text, keys it covers), at most CHANNEL_MAX_PER_MESSAGE issues per message."""
    for start in range(0, len(items), CHANNEL_MAX_PER_MESSAGE):
        chunk = items[start:start + CHANNEL_MAX_PER_MESSAGE]
        lines = [f":white_check_mark: Jira resolved (techsupport): {len(chunk)}"]
        for key, summary, status in chunk:
            summary = mask_emails(summary)
            if len(summary) > CHANNEL_SUMMARY_CHARS:
                summary = summary[:CHANNEL_SUMMARY_CHARS].rstrip() + "..."
            lines.append(f"- <{config['JIRA_SITE']}/browse/{key}|{key}> {escape_slack(summary)} ({escape_slack(status)})")
        yield "\n".join(lines), [key for key, _, _ in chunk]


def prune_announced(announced, now=None):
    """Forget announcements older than CHANNEL_KEEP_DAYS so state.json stays small."""
    now = now or datetime.now(timezone.utc)
    keep = {}
    for key, stamp in announced.items():
        try:
            if now - datetime.fromisoformat(stamp) <= timedelta(days=CHANNEL_KEEP_DAYS):
                keep[key] = stamp
        except ValueError:
            keep[key] = stamp
    return keep


def _post(config, channel, text):
    resp = requests.post(
        SLACK_POST_URL,
        headers={"Authorization": f"Bearer {config['SLACK_BOT_TOKEN']}"},
        json={
            "channel": channel,
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


def send_slack_dm(config, text):
    _post(config, config["SLACK_USER_ID"], text)


def send_slack_channel(config, text):
    _post(config, config["SLACK_CHANNEL_ID"], text)


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
            print("Nothing finished, and today's open-issues digest is not due (or was already sent).")

    # Team channel: resolved techsupport issues only. A failure here must never undo or repeat the personal DMs,
    # so it is caught, state is saved, and the run is then reported as failed at the very end.
    channel_error = None
    announced = state.get("channel_announced")
    if config["SLACK_CHANNEL_ID"]:
        try:
            candidates = channel_resolved(fetch_jira_issues(config, CHANNEL_JQL))
            now_iso = datetime.now(timezone.utc).isoformat()
            if announced is None:
                # First run with a channel set: record what is already resolved so the channel is not flooded.
                print(f"Channel: first run, recorded {len(candidates)} recently resolved issue(s) without posting.")
                if not args.dry_run:
                    announced = {key: now_iso for key, _, _ in candidates}
            else:
                fresh = [item for item in candidates if item[0] not in announced]
                print(f"Channel: {len(candidates)} techsupport issue(s) resolved recently; {len(fresh)} new to the channel.")
                for text, keys in build_channel_batches(config, fresh):
                    if args.dry_run:
                        print(f"--- channel post ---\n{text}")
                    else:
                        send_slack_channel(config, text)
                        announced = {**announced, **{key: now_iso for key in keys}}
            if announced is not None:
                announced = prune_announced(announced)
        except (requests.RequestException, RuntimeError) as exc:
            channel_error = f"Channel post failed: {exc}. Personal DMs and saved state were not affected."
            print(channel_error)

    if args.dry_run:
        print("Dry run: Slack was not contacted and state.json was not changed.")
        return

    save_state({**previous, **current}, last_digest, announced)
    if channel_error:
        sys.exit(channel_error)


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        sys.exit(f"Request failed: {exc}")
    except RuntimeError as exc:
        sys.exit(str(exc))
