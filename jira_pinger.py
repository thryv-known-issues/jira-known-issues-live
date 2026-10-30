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
# STRICT_STATUSES are the team's fully resolved statuses (the Known System Issues rules); Integration is NOT here.
# The channel also posts Production. Issues listed under strict_only in overrides.json use STRICT_STATUSES only,
# so a partly fixed issue does not post at Production. Cancelled is included; the post shows the status word.
CHANNEL_JQL = 'labels = "techsupport" AND status CHANGED AFTER "-125m" ORDER BY updated DESC'
STRICT_STATUSES = {
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
CHANNEL_STATUSES = STRICT_STATUSES | {"production"}
# "Fix incoming" heads-up: an issue that just reached Integration (the fix is built and on its way) is posted once with
# the built-in eyes emoji. Issues under strict_only are skipped, because Integration
# there means only part of the fix is in.
INCOMING_STATUS = "integration"
INCOMING_HEADER = ":eyes: Fix incoming, keep an eye out:"
RESOLVED_HEADER = ":white_check_mark: Resolved, please check:"
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
# Weekend window (Central): from Friday WEEKEND_START_HOUR to Monday WEEKEND_END_HOUR. Channel posts still go out as usual;
# what is posted in this window is also logged (by key only) and bundled into one recap on the first run after it ends.
WEEKEND_START_HOUR = 22
WEEKEND_END_HOUR = 6
RECAP_HEADER = ":spiral_calendar_pad: Weekend recap, in case anything slipped:"
RECAP_MAX_PER_SECTION = 40
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
    config["MENTIONS"] = load_mentions(os.environ.get("SLACK_MENTIONS", ""))
    # Table watch is off unless TABLE_WATCH_ENABLED is true. Dry run (the default) sends its alerts to the owner's DM only.
    config["TABLE_WATCH_ENABLED"] = os.environ.get("TABLE_WATCH_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")
    config["TABLE_WATCH_DRY_RUN"] = os.environ.get("TABLE_WATCH_DRY_RUN", "").strip().lower() not in ("0", "false", "no", "off")
    config["TABLE_WATCH_CHANNEL"] = os.environ.get("TABLE_WATCH_CHANNEL", "").strip()
    config["TABLE_WATCH_LABEL"] = os.environ.get("TABLE_WATCH_LABEL", "").strip() or "techsupport"
    return config


def load_mentions(raw):
    """Optional secret SLACK_MENTIONS: JSON like {"Jira display name": "U012ABCDEF"}. Only these people are ever tagged in the channel."""
    raw = raw.lstrip("\ufeff").strip()            # tolerate a byte-order mark or stray whitespace from how the secret was pasted
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except ValueError:
        print("SLACK_MENTIONS is not valid JSON, so channel posts will not tag anyone.")
        return {}
    if not isinstance(data, dict):
        return {}
    mentions = {}
    for name, uid in data.items():
        uid = str(uid or "").strip()
        if uid.startswith("->"):                  # "Former Agent": "->Current Agent" re-routes to that person's ID
            uid = str(data.get(uid[2:].strip()) or "").strip()
        if uid and not uid.startswith("->"):
            mentions[str(name)] = uid
    return mentions


def mention_ids(config, key, reporter, watcher_lookup=None):
    """Slack IDs to tag for one resolved issue: its reporter and watchers who are in SLACK_MENTIONS, each person once."""
    mentions = config.get("MENTIONS") or {}
    if not mentions:
        return []
    names = [reporter] if reporter else []
    watchers = (watcher_lookup or fetch_watcher_names)(config, key)
    names += watchers or []
    ids = []
    for name in names:
        uid = mentions.get(name)
        if uid and uid not in ids:
            ids.append(uid)
    return ids


def fetch_jira_issues(config, jql=JQL, fields="summary,status,project"):
    url = f"{config['JIRA_SITE']}/rest/api/3/search/jql"
    auth = (config["JIRA_EMAIL"], config["JIRA_API_TOKEN"])
    params = {"jql": jql, "fields": fields, "maxResults": 100}
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


def fetch_changelog(config, key, page_size=100):
    """Every change-history entry of one issue (read-only), following pagination."""
    url = f"{config['JIRA_SITE']}/rest/api/3/issue/{key}/changelog"
    auth = (config["JIRA_EMAIL"], config["JIRA_API_TOKEN"])
    out, start = [], 0
    while True:
        resp = requests.get(url, params={"startAt": start, "maxResults": page_size}, auth=auth,
                            headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        page = resp.json()
        values = page.get("values", [])
        out.extend(values)
        start += len(values)
        if not values or page.get("isLast", True) or start >= page.get("total", start):
            return out


def table_watch_step(config, tw_state):
    """One table-watch pass. Returns the new saved state. Any problem is reported by type only (the log is public)."""
    from table_watch import runner

    def fetch_issues(jql):
        return [{"key": i["key"], "labels": i["fields"].get("labels", [])} for i in fetch_jira_issues(config, jql, fields="labels")]

    dry = config["TABLE_WATCH_DRY_RUN"]
    owner = config["SLACK_USER_ID"]

    def send(message):
        text = message["text"]
        if dry:
            who = message["agent"] or "nobody (no agent named)"
            _post(config, owner, f"*TABLE WATCH DRY RUN. Nothing was sent to the agent. This would have gone to: {who}.*\n{text}")
        elif message["user_id"] and config["TABLE_WATCH_CHANNEL"]:
            _post(config, config["TABLE_WATCH_CHANNEL"], text)           # tagged alert in the team channel, like the status posts
        elif message["user_id"]:
            _post(config, message["user_id"], text)                      # no channel set: the rep who added the row gets the DM
        elif message["agent"]:
            _post(config, owner, f"*Table watch: I could not match the agent name \"{message['agent']}\" to a Slack user, so this went only to you.*\n{text}")
        elif any(a.kind == "bulk" for a in message["alerts"]):
            _post(config, owner, f"*Table watch: a large part of a table was removed in one save. Sent only to you for a human check.*\n{text}")
        else:
            _post(config, owner, f"*Table watch: the dropped row names no agent, so this went only to you.*\n{text}")

    new_state, counts = runner.run(config, tw_state, fetch_issues, lambda key: fetch_changelog(config, key), send,
                                   label=config["TABLE_WATCH_LABEL"])
    mode = "dry run" if dry else "live"
    print("Table watch ({}): {} ticket(s) checked, {} new to it, {} alert(s) sent, {} unmapped name(s), {} send failure(s).".format(
        mode, counts["checked"], counts["baselined"], counts["alerts"], counts["unmapped"], counts["failed"]))
    return new_state


FULL_RUN_MINUTES = 55          # status pings, channel posts and the daily list run about hourly; table watch runs on every pass


def full_run_due(state, now=None):
    """True when the hourly work is due. The outside timer fires every 15 minutes, so the other passes only run table watch."""
    if os.environ.get("FORCE_FULL", "").strip().lower() in ("1", "true", "yes"):
        return True
    last = state.get("last_full_run")
    if not last:
        return True
    try:
        elapsed = (now or datetime.now(timezone.utc)) - datetime.fromisoformat(last)
    except ValueError:
        return True
    return elapsed >= timedelta(minutes=FULL_RUN_MINUTES)


def light_run(config, state):
    """Table watch only. Keeps every other saved field exactly as it was."""
    if not config.get("TABLE_WATCH_ENABLED"):
        print("Light pass: nothing to do (hourly work is not due yet).")
        return
    try:
        state["table_watch"] = table_watch_step(config, state.get("table_watch"))
    except Exception as exc:
        print(f"Table watch error ({type(exc).__name__}); it will retry on the next pass. Pings are not affected.")
        return
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_state():
    if STATE_FILE.exists():
        # utf-8-sig also accepts a file saved with a byte-order mark, which some Windows editors add.
        return json.loads(STATE_FILE.read_text(encoding="utf-8-sig"))
    return {}


def save_state(statuses, last_digest=None, channel_announced=None, snapshot_done=None, weekend_resolved=None, extra=None):
    # Keep every issue ever seen, so a later edit to an already-finished issue is not mistaken for a new transition.
    state = {"statuses": statuses}
    if last_digest:
        state["last_digest_dm"] = last_digest
    if channel_announced is not None:
        state["channel_announced"] = channel_announced
    if snapshot_done:
        state["people_snapshot_done"] = snapshot_done
    if weekend_resolved:
        state["weekend_resolved"] = sorted(weekend_resolved)
    for name, value in (extra or {}).items():
        if value:
            state[name] = value
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def weekend_window(now=None):
    """True from Friday 10 PM through Monday 6 AM Central, the stretch the Monday recap covers."""
    now = (now or datetime.now(timezone.utc)).astimezone(LOCAL_TZ)
    day, hour = now.weekday(), now.hour
    return (day == 4 and hour >= WEEKEND_START_HOUR) or day in (5, 6) or (day == 0 and hour < WEEKEND_END_HOUR)


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


def read_overrides():
    if not OVERRIDES_FILE.exists():
        return {}
    try:
        data = json.loads(OVERRIDES_FILE.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except ValueError as exc:
        print(f"WARNING: could not read {OVERRIDES_FILE.name} ({exc}); ignoring it.")
        return {}


def load_strict_only():
    """Issues listed under "strict_only" in overrides.json are only treated as resolved once they reach a truly
    finished status (the team list), not Integration or Production. Use it for partly fixed issues."""
    strict = read_overrides().get("strict_only", {})
    return {str(key).strip().upper(): note for key, note in strict.items()} if isinstance(strict, dict) else {}


STRICT_ONLY = load_strict_only()


# ---- ONE-TIME TEST: "who is associated with what" snapshot (sent only to your own DM, never pings anyone) ----
SNAPSHOT_JQL = 'labels = "techsupport" AND statusCategory != Done ORDER BY updated DESC'
SNAPSHOT_MAX_CHARS = 3000
SNAPSHOT_MAX_KEYS_PER_LINE = 40


def bot_preview_id():
    """overrides.json can hold "bot_preview": "<any label>". A fake-data sample of each channel message style is DMed to the owner once per label."""
    value = read_overrides().get("bot_preview")
    return str(value) if value else None


def build_bot_previews(config):
    """The three channel message styles built from made-up tickets, tagging only the owner, each marked as a test."""
    me = [config["SLACK_USER_ID"]]
    note = "*TEST PREVIEW, fake data, sent only to you. Nothing was posted to the team channel.*\n"
    resolved = [("TEST-0001", "Example: customer cannot see their add-on", "Done"), ("TEST-0002", "Example: report is missing data", "Production")]
    incoming = [("TEST-0003", "Example: fix is built and heading to production", "Integration")]
    tags = {key: me for key, _, _ in resolved + incoming}
    texts = [text for text, _ in build_channel_batches(config, resolved, tags)]
    texts += [text for text, _ in build_channel_batches(config, incoming, tags, INCOMING_HEADER)]
    texts.append(build_recap(config, resolved, incoming, tags))
    texts += [text.split("\n", 1)[1] for text in build_table_watch_preview(config)]   # a dropped-row alert, without its own test label
    return [note + text for text in texts]


def table_watch_preview_id():
    """overrides.json can hold "table_watch_preview": "<any label>". One fake-data dropped-row alert is DMed to the owner per label."""
    value = read_overrides().get("table_watch_preview")
    return str(value) if value else None


def build_table_watch_preview(config):
    """A made-up dropped-row alert built by the real table-watch code, tagging only the owner."""
    from table_watch.detect import analyze
    from table_watch.message import build_messages, norm_name
    head = "||*Thryv ID*||*Country+Package*||*Zendesk Ticket*||*Tech Support Agent*||"
    rows = [["demo101", "US MC Plus", "1000001", "Demo Agent"], ["demo102", "US MC Pro", "1000002", "Demo Agent"],
            ["demo103", "AU MC Plus", "1000003", "Demo Agent"]]
    table = lambda rs: head + "\n" + "\n".join("|" + "|".join(r) + "|" for r in rs)
    text_before = "Demo ticket\n\n" + table(rows[:2])
    text_with_third = "Demo ticket\n\n" + table(rows)
    # save 1 adds the third row; save 2 is made from the older text, so the third row disappears
    history = [
        {"id": "1", "created": "2026-10-26T10:00:00.000-0700", "author": {"displayName": "Someone Else"},
         "items": [{"field": "description", "fromString": text_before, "toString": text_with_third}]},
        {"id": "2", "created": "2026-10-26T10:00:30.000-0700", "author": {"displayName": "Another Agent"},
         "items": [{"field": "description", "fromString": text_with_third, "toString": text_before}]},
    ]
    result = analyze("DEMO-1", history, labels=["techsupport"])
    messages, _ = build_messages(result.alerts, {norm_name("Demo Agent"): config["SLACK_USER_ID"]}, config["JIRA_SITE"])
    note = "*TEST PREVIEW, fake data, sent only to you. This is how a table watch alert will look. The ticket DEMO-1 and its rows are made up.*\n"
    return [note + message["text"] for message in messages]


def snapshot_id():
    """overrides.json can hold "people_snapshot": "<any label>". The snapshot is sent once per distinct label."""
    value = read_overrides().get("people_snapshot")
    return str(value) if value else None


def fetch_watcher_names(config, key):
    """Display names of the watchers of one issue, or None when Jira will not say (for example no permission)."""
    url = f"{config['JIRA_SITE']}/rest/api/3/issue/{key}/watchers"
    resp = requests.get(url, auth=(config["JIRA_EMAIL"], config["JIRA_API_TOKEN"]), headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
    if resp.status_code in (401, 403, 404):
        return None
    resp.raise_for_status()
    return [w.get("displayName") for w in resp.json().get("watchers", []) if w.get("displayName")]


def collect_people(config, issues, watcher_lookup=fetch_watcher_names):
    """{name: {"reporter": [keys], "watching": [keys]}}, plus how many issues had no people listed and how many lookups failed."""
    people, nobody, failed = {}, 0, 0
    for issue in issues:
        key = issue["key"]
        reporter = ((issue["fields"].get("reporter")) or {}).get("displayName")
        watchers = watcher_lookup(config, key)
        if watchers is None:
            failed += 1
            watchers = []
        names = {}
        if reporter:
            names[reporter] = "reporter"
        for name in watchers:
            names.setdefault(name, "watching")        # reporter wins if the same person also watches
        if not names:
            nobody += 1
        for name, role in names.items():
            people.setdefault(name, {"reporter": [], "watching": []})[role].append(key)
    return people, nobody, failed


def build_people_snapshot(people, issue_count, nobody, failed):
    """Plain-text messages (no @-mentions, so nobody is pinged), each under SNAPSHOT_MAX_CHARS."""
    head = ("ONE-TIME TEST RUN. This is a one-off test of the associations list. It will not repeat and nobody was pinged.\n"
            f"Snapshot: who is associated with the {issue_count} open techsupport Jira issues right now ({len(people)} people). "
            "Reporter = created it; watching = watching it.")
    notes = []
    if nobody:
        notes.append(f"{nobody} issue(s) list no reporter or watchers.")
    if failed:
        notes.append(f"Jira would not show the watchers of {failed} issue(s), so only their reporters are counted.")
    blocks = []
    for name, roles in sorted(people.items(), key=lambda item: (-(len(item[1]["reporter"]) + len(item[1]["watching"])), item[0].lower())):
        lines = [f"{name}: {len(roles['reporter']) + len(roles['watching'])} issue(s)"]
        for role in ("reporter", "watching"):
            keys = roles[role]
            if keys:
                shown = ", ".join(keys[:SNAPSHOT_MAX_KEYS_PER_LINE]) + (f" (+{len(keys) - SNAPSHOT_MAX_KEYS_PER_LINE} more)" if len(keys) > SNAPSHOT_MAX_KEYS_PER_LINE else "")
                lines.append(f"  {role}: {shown}")
        blocks.append("\n".join(lines))
    reminder = "(One-time test run, continued. Not a recurring message.)"
    messages, current = [], "\n".join([head, *notes])
    for block in blocks:
        if len(current) + len(block) + 2 > SNAPSHOT_MAX_CHARS and current:
            messages.append(current)
            current = reminder
        current = f"{current}\n\n{block}" if current else block
    messages.append(current)
    return messages


def is_finished(status, key=None):
    status = status.strip().lower()
    if key and key.upper() in STRICT_ONLY:
        return status in STRICT_STATUSES
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


def incoming_items(issues):
    """(key, summary, status) for techsupport issues now at Integration, except strict_only exceptions."""
    return [
        (i["key"], i["fields"].get("summary") or "", i["fields"]["status"]["name"])
        for i in issues
        if i["fields"]["status"]["name"].strip().lower() == INCOMING_STATUS and i["key"].upper() not in STRICT_ONLY
    ]


def channel_resolved(issues):
    """(key, summary, status) for techsupport issues now in one of the team's resolved statuses."""
    return [
        (i["key"], i["fields"].get("summary") or "", i["fields"]["status"]["name"])
        for i in issues
        if i["fields"]["status"]["name"].strip().lower() in (STRICT_STATUSES if i["key"].upper() in STRICT_ONLY else CHANNEL_STATUSES)
    ]


def mask_emails(text):
    """Ticket summaries sometimes contain customer email addresses; keep them out of a shared channel."""
    return EMAIL_RE.sub("[email]", text)


def build_channel_batches(config, items, tags=None, header=None):
    """Yield (message text, keys it covers), at most CHANNEL_MAX_PER_MESSAGE issues per message.
    tags maps an issue key to the Slack IDs to tag on its line."""
    tags = tags or {}
    for start in range(0, len(items), CHANNEL_MAX_PER_MESSAGE):
        chunk = items[start:start + CHANNEL_MAX_PER_MESSAGE]
        lines = [header or RESOLVED_HEADER]
        for key, summary, status in chunk:
            summary = mask_emails(summary)
            if len(summary) > CHANNEL_SUMMARY_CHARS:
                summary = summary[:CHANNEL_SUMMARY_CHARS].rstrip() + "..."
            who = "".join(f" <@{uid}>" for uid in tags.get(key, []))
            lines.append(f"- <{config['JIRA_SITE']}/browse/{key}|{key}> {escape_slack(summary)} ({escape_slack(status)}){who}")
        yield "\n".join(lines), [key for key, _, _ in chunk]


def build_recap(config, resolved, incoming, tags=None):
    """One message listing what was posted over the weekend, or "" when nothing is left to list."""
    tags = tags or {}
    sections = []
    for header, items in ((RESOLVED_HEADER, resolved), (INCOMING_HEADER, incoming)):
        if not items:
            continue
        lines = [header]
        for key, summary, status in items[:RECAP_MAX_PER_SECTION]:
            summary = mask_emails(summary)
            if len(summary) > CHANNEL_SUMMARY_CHARS:
                summary = summary[:CHANNEL_SUMMARY_CHARS].rstrip() + "..."
            who = "".join(f" <@{uid}>" for uid in tags.get(key, []))
            lines.append(f"- <{config['JIRA_SITE']}/browse/{key}|{key}> {escape_slack(summary)} ({escape_slack(status)}){who}")
        if len(items) > RECAP_MAX_PER_SECTION:
            lines.append(f"...and {len(items) - RECAP_MAX_PER_SECTION} more.")
        sections.append("\n".join(lines))
    return RECAP_HEADER + "\n\n" + "\n\n".join(sections) if sections else ""


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
    state = load_state()
    if not args.dry_run and "statuses" in state and not full_run_due(state):
        light_run(config, state)
        return
    issues = fetch_jira_issues(config)
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
            # This repo is public, so its Actions logs are public too: never print ticket details outside a dry run.
            print(digest if args.dry_run else f"Daily list built: {len(open_issues)} open issue(s).")
            if not args.dry_run:
                send_slack_dm(config, digest)
                last_digest = datetime.now(timezone.utc).isoformat()
        else:
            print("Nothing finished, and today's open-issues digest is not due (or was already sent).")

    # Team channel: resolved techsupport issues and "fix incoming" heads-ups, posted as they happen, all week. A failure
    # here must never undo or repeat the personal DMs, so it is caught, state is saved, and the run is reported as failed.
    # Over the weekend (Friday 10 PM to Monday 6 AM Central) the keys of what was posted are also logged, and the first run
    # after that window posts one bundled recap of them, so nothing slips past anyone.
    channel_error = None
    announced = state.get("channel_announced")
    incoming_announced = state.get("channel_incoming")
    weekend_resolved = list(state.get("weekend_resolved") or state.get("channel_held") or [])   # channel_held: older name
    weekend_incoming = list(state.get("weekend_incoming", []))
    if config["SLACK_CHANNEL_ID"]:
        try:
            channel_issues = fetch_jira_issues(config, CHANNEL_JQL, fields="summary,status,reporter")
            reporters = {i["key"]: ((i["fields"].get("reporter")) or {}).get("displayName") for i in channel_issues}
            candidates = channel_resolved(channel_issues)
            incoming = incoming_items(channel_issues)
            now_iso = datetime.now(timezone.utc).isoformat()
            in_window = weekend_window()

            def tags_for(items):
                tags = {}
                for key, _, _ in items:
                    try:
                        tags[key] = mention_ids(config, key, reporters.get(key))
                    except requests.RequestException:
                        tags[key] = []             # never lose the post because a watcher lookup failed
                return tags

            if announced is None:
                # First run with a channel set: record what is already resolved so the channel is not flooded.
                print(f"Channel: first run, recorded {len(candidates)} recently resolved issue(s) without posting.")
                if not args.dry_run:
                    announced = {key: now_iso for key, _, _ in candidates}
            else:
                fresh = [item for item in candidates if item[0] not in announced]
                print(f"Channel: {len(candidates)} techsupport issue(s) resolved recently; {len(fresh)} new to the channel.")
                for text, keys in build_channel_batches(config, fresh, tags_for(fresh)):
                    if args.dry_run:
                        print(f"--- channel post ---\n{text}")
                    else:
                        send_slack_channel(config, text)
                        announced = {**announced, **{key: now_iso for key in keys}}
                        if in_window:
                            weekend_resolved = sorted(set(weekend_resolved) | set(keys))
                announced = prune_announced(announced)

                if incoming_announced is None:
                    # First run with the heads-up feature: record what is already at Integration, post nothing.
                    print(f"Channel: fix-incoming first run, recorded {len(incoming)} issue(s) without posting.")
                    if not args.dry_run:
                        incoming_announced = {key: now_iso for key, _, _ in incoming}
                else:
                    fresh_in = [i for i in incoming if i[0] not in incoming_announced and i[0] not in announced]
                    print(f"Channel: {len(incoming)} issue(s) at Integration recently; {len(fresh_in)} new heads-up(s).")
                    for text, keys in build_channel_batches(config, fresh_in, tags_for(fresh_in), INCOMING_HEADER):
                        if args.dry_run:
                            print(f"--- fix incoming post ---\n{text}")
                        else:
                            send_slack_channel(config, text)
                            incoming_announced = {**incoming_announced, **{key: now_iso for key in keys}}
                            if in_window:
                                weekend_incoming = sorted(set(weekend_incoming) | set(keys))
                if incoming_announced is not None:
                    incoming_announced = prune_announced(incoming_announced)

                # Monday recap: the first run after the weekend window posts everything logged, once, in one message.
                if not in_window and (weekend_resolved or weekend_incoming):
                    logged = sorted(set(weekend_resolved) | set(weekend_incoming))
                    logged_issues = fetch_jira_issues(config, "key in (" + ",".join(logged) + ")", fields="summary,status,reporter")
                    reporters.update({i["key"]: ((i["fields"].get("reporter")) or {}).get("displayName") for i in logged_issues})
                    recap_resolved = [i for i in channel_resolved(logged_issues) if i[0] in weekend_resolved or i[0] in weekend_incoming]
                    recap_incoming = [i for i in incoming_items(logged_issues) if i[0] in weekend_incoming]
                    text = build_recap(config, recap_resolved, recap_incoming, tags_for(recap_resolved + recap_incoming))
                    if text:
                        print(f"Channel: weekend recap with {len(recap_resolved)} resolved and {len(recap_incoming)} heads-up issue(s).")
                        if args.dry_run:
                            print(f"--- weekend recap ---\n{text}")
                        else:
                            send_slack_channel(config, text)
                    if not args.dry_run:
                        weekend_resolved, weekend_incoming = [], []
        except (requests.RequestException, RuntimeError) as exc:
            # keep the weekend log as it was in the saved state, so the next run retries the recap
            weekend_resolved = list(state.get("weekend_resolved") or state.get("channel_held") or [])
            weekend_incoming = list(state.get("weekend_incoming", []))
            channel_error = f"Channel post failed: {exc}. Personal DMs and saved state were not affected."
            print(channel_error)

    # Table watch: a separate layer. Off by default, and any error in it is swallowed so the pings above are never affected.
    tw_state = state.get("table_watch")
    if config.get("TABLE_WATCH_ENABLED"):
        if args.dry_run:
            print("Table watch: skipped in a --dry-run preview.")
        else:
            try:
                tw_state = table_watch_step(config, tw_state)
            except Exception as exc:
                print(f"Table watch error ({type(exc).__name__}); it will retry on the next run. Pings are not affected.")

    # ONE-TIME TEST: a snapshot of who is associated with which open techsupport issue. DM to the owner only, once per label.
    snapshot_done = state.get("people_snapshot_done")
    wanted = snapshot_id()
    snapshot_error = None
    if wanted and wanted != snapshot_done:
        try:
            open_issues = fetch_jira_issues(config, SNAPSHOT_JQL, fields="summary,status,reporter")
            people, nobody, failed = collect_people(config, open_issues)
            messages = build_people_snapshot(people, len(open_issues), nobody, failed)
            print(f"Snapshot: {len(open_issues)} open techsupport issue(s), {len(people)} people, {len(messages)} message(s).")
            for text in messages:
                if args.dry_run:
                    print(f"--- snapshot DM ---\n{text}")
                else:
                    send_slack_dm(config, text)
            if not args.dry_run:
                snapshot_done = wanted
        except (requests.RequestException, RuntimeError) as exc:
            snapshot_error = f"Snapshot failed: {exc}."
            print(snapshot_error)

    # ONE-TIME TEST: sample channel messages, sent as the bot to the owner's DM only, once per label.
    preview_done = state.get("bot_preview_done")
    preview_wanted = bot_preview_id()
    preview_error = None
    if preview_wanted and preview_wanted != preview_done:
        try:
            for text in build_bot_previews(config):
                if args.dry_run:
                    print(f"--- bot preview DM ---\n{text}")
                else:
                    send_slack_dm(config, text)
            print("Bot preview: sample messages built.")
            if not args.dry_run:
                preview_done = preview_wanted
        except (requests.RequestException, RuntimeError) as exc:
            preview_error = f"Bot preview failed: {exc}."
            print(preview_error)

    # ONE-TIME TEST: a fake table-watch alert, sent as the bot to the owner's DM only, once per label.
    tw_preview_done = state.get("table_watch_preview_done")
    tw_preview_wanted = table_watch_preview_id()
    if tw_preview_wanted and tw_preview_wanted != tw_preview_done:
        try:
            for text in build_table_watch_preview(config):
                if args.dry_run:
                    print(f"--- table watch preview DM ---\n{text}")
                else:
                    send_slack_dm(config, text)
            print("Table watch preview: sample alert built.")
            if not args.dry_run:
                tw_preview_done = tw_preview_wanted
        except Exception as exc:
            preview_error = (preview_error or "") + f" Table watch preview failed ({type(exc).__name__})."
            print(f"Table watch preview failed ({type(exc).__name__}).")

    if args.dry_run:
        print("Dry run: Slack was not contacted and state.json was not changed.")
        return

    save_state({**previous, **current}, last_digest, announced, snapshot_done, weekend_resolved, {"channel_incoming": incoming_announced, "weekend_incoming": weekend_incoming, "bot_preview_done": preview_done, "table_watch": tw_state, "table_watch_preview_done": tw_preview_done,
                                                   "last_full_run": datetime.now(timezone.utc).isoformat()})
    if channel_error or snapshot_error or preview_error:
        sys.exit(" ".join(m for m in (channel_error, snapshot_error, preview_error) if m))


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        sys.exit(f"Request failed: {exc}")
    except RuntimeError as exc:
        sys.exit(str(exc))
