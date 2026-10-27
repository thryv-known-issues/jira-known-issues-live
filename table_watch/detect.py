"""Compare saved versions of each table and classify rows that disappeared."""
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .parse import parse_tables

SIGNATURE_WINDOW = timedelta(minutes=60)


@dataclass
class Alert:
    kind: str  # "drop" (one row) or "bulk" (more than half the table, one alert)
    issue: str
    history_id: str
    created: datetime
    author: str
    table_no: int  # 1-based position among the ticket's tables
    table_count: int
    header: list
    rows: list
    keys: list
    agent: str = ""  # Agent cell of the dropped row (drop only)
    thryv_id: str = ""  # Thryv ID as written in the row (drop only)
    ticket: str = ""  # ticket cell of the dropped row, empty without a ticket column (drop only)
    table_size: int = 0  # rows in the table before the save
    signature_seconds: float = None  # set when another author added the row within 60 minutes


@dataclass
class LogEntry:
    kind: str  # consolidation | resolved | acknowledged | readded
    history_id: str
    row: list
    detail: str = ""


@dataclass
class Result:
    alerts: list = field(default_factory=list)
    logs: list = field(default_factory=list)


def parse_created(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")


def _tables_by_id(text):
    """Identify a table by its header text plus its occurrence among tables with that header."""
    seen = Counter()
    out = {}
    tables = parse_tables(text)
    for pos, t in enumerate(tables, 1):
        seen[t.header_key] += 1
        out[(t.header_key, seen[t.header_key])] = (pos, t)
    return out, len(tables)


def _diff(prev_rows, new_rows, table):
    prev_rows = [r for r in prev_rows if any(r)]       # blank rows are layout, not data
    new_rows = [r for r in new_rows if any(r)]
    remaining = Counter(table.key(r) for r in new_rows)
    removed = []
    for r in prev_rows:
        k = table.key(r)
        if remaining[k] > 0:
            remaining[k] -= 1
        else:
            removed.append(r)
    remaining = Counter(table.key(r) for r in prev_rows)
    added = []
    for r in new_rows:
        k = table.key(r)
        if remaining[k] > 0:
            remaining[k] -= 1
        else:
            added.append(r)
    return removed, added


def analyze(issue, histories, labels=None, label="techsupport", muted=frozenset()):
    """Walk a ticket's description saves oldest to newest and return alerts and log entries.

    `muted` holds row keys marked intentional. A ticket whose `labels` lack `label` is ignored.
    """
    result = Result()
    if labels is not None and label not in labels:
        return result
    added_by = {}  # (table id, row key) -> (author, created)
    open_alerts = set()  # (table id, row key) alerted and not yet re-added
    ordered = sorted(histories, key=lambda h: (parse_created(h["created"]), int(h["id"]) if str(h["id"]).isdigit() else 0))
    for h in ordered:
        created = parse_created(h["created"])
        author = (h.get("author") or {}).get("displayName", "")
        for item in h.get("items", []):
            if item.get("field") != "description":
                continue
            prev_tables, _ = _tables_by_id(item.get("fromString"))
            new_tables, count = _tables_by_id(item.get("toString"))
            for tid, (pos, prev) in prev_tables.items():
                if tid not in new_tables:
                    continue  # header changed or table deleted: nothing to compare
                new = new_tables[tid][1]
                removed, added = _diff(prev.rows, new.rows, prev)
                new_ids = {new.thryv_id(r) for r in new.rows}
                alertable = []
                for row in removed:
                    key = prev.key(row)
                    if key in muted:
                        result.logs.append(LogEntry("acknowledged", h["id"], row))
                    elif prev.is_resolved(row):
                        result.logs.append(LogEntry("resolved", h["id"], row))
                    elif prev.thryv_id(row) in new_ids:
                        result.logs.append(LogEntry("consolidation", h["id"], row, "same Thryv ID still present"))
                    else:
                        alertable.append(row)
                common = dict(issue=issue, history_id=h["id"], created=created, author=author,
                              table_no=pos, table_count=count, header=prev.header,
                              table_size=len(prev.rows))
                if alertable and len(alertable) > len(prev.rows) / 2:
                    keys = [prev.key(r) for r in alertable]
                    result.alerts.append(Alert("bulk", rows=alertable, keys=keys, **common))
                    open_alerts.update((tid, k) for k in keys)
                else:
                    for row in alertable:
                        key = prev.key(row)
                        alert = Alert("drop", rows=[row], keys=[key],
                                      agent=prev.cell(row, prev.agent_col),
                                      thryv_id=prev.cell(row, prev.id_col),
                                      ticket=prev.cell(row, prev.ticket_col), **common)
                        ab = added_by.get((tid, key))
                        if ab and ab[0] != author and timedelta(0) <= created - ab[1] <= SIGNATURE_WINDOW:
                            alert.signature_seconds = (created - ab[1]).total_seconds()
                        result.alerts.append(alert)
                        open_alerts.add((tid, key))
                for row in added:
                    key = new.key(row)
                    added_by[(tid, key)] = (author, created)
                    if (tid, key) in open_alerts:
                        open_alerts.discard((tid, key))
                        result.logs.append(LogEntry("readded", h["id"], row))
    # An alert only matters while the row is still gone: rows the newest save put back are not reported.
    still_missing = {key for _, key in open_alerts}
    kept = []
    for alert in result.alerts:
        pairs = [(k, r) for k, r in zip(alert.keys, alert.rows) if k in still_missing]
        if pairs:
            alert.keys = [k for k, _ in pairs]
            alert.rows = [r for _, r in pairs]
            kept.append(alert)
    result.alerts = kept
    return result
