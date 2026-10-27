"""One table-watch pass inside the pinger run. Read-only on Jira; idempotent; never raises into the pinger."""
import hashlib
from datetime import datetime, timedelta, timezone

from .detect import analyze
from .message import build_messages, name_map_from_mentions

SENT_KEEP_DAYS = 30
DEFAULT_LABEL = "techsupport"
LOOKBACK = "-125m"                  # same window as the pinger, so one skipped hourly run is covered


def alert_hash(alert):
    """Short fingerprint of (ticket, save, row), so the saved state never holds account IDs or ticket numbers."""
    raw = "|".join([alert.issue, str(alert.history_id)] + [part for key in alert.keys for part in key])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def run(config, tw_state, fetch_issues, fetch_changelog, send, label=DEFAULT_LABEL, now=None):
    """Check recently updated labelled tickets. Returns (new state, counts).

    `send(message)` posts one message dict (text, user_id, agent); if it raises, that ticket is retried next run.
    The returned counts hold numbers only, so they are safe to print in a public log.
    """
    now = now or datetime.now(timezone.utc)
    last = dict((tw_state or {}).get("last", {}))
    sent = dict((tw_state or {}).get("sent", {}))
    counts = {"checked": 0, "baselined": 0, "alerts": 0, "unmapped": 0, "failed": 0}
    name_map = name_map_from_mentions(config.get("MENTIONS"))
    jira_base = config["JIRA_SITE"]
    for issue in fetch_issues(f'labels = "{label}" AND updated >= "{LOOKBACK}"'):
        key = issue["key"]
        counts["checked"] += 1
        histories = fetch_changelog(key)
        newest = max((int(h["id"]) for h in histories), default=0)
        previous = last.get(key)
        if previous is None:        # first sight of this ticket: remember where we are, alert only on newer saves
            last[key] = newest
            counts["baselined"] += 1
            continue
        result = analyze(key, histories, labels=issue.get("labels"), label=label)
        fresh = [a for a in result.alerts if int(a.history_id) > previous and alert_hash(a) not in sent]
        messages, unmapped = build_messages(fresh, name_map, jira_base)
        counts["unmapped"] += len(unmapped)
        failed = False
        for message in messages:
            try:
                send(message)
            except Exception:       # leave the ticket's pointer where it was so the next run retries
                failed = True
                counts["failed"] += 1
                continue
            for a in message["alerts"]:
                sent[alert_hash(a)] = now.isoformat()
            counts["alerts"] += len(message["alerts"])
        if not failed:
            last[key] = max(newest, previous)
    cutoff = now - timedelta(days=SENT_KEEP_DAYS)
    sent = {h: stamp for h, stamp in sent.items() if _recent(stamp, cutoff)}
    return {"last": last, "sent": sent}, counts


def _recent(stamp, cutoff):
    try:
        return datetime.fromisoformat(stamp) >= cutoff
    except ValueError:
        return True
