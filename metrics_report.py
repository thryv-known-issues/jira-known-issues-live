"""Print the weekly counters kept in state.json (numbers only). Usage: python metrics_report.py [path to state.json]

The two things this tool is for:
  1. Telling ticket openers sooner when their Jira is resolved (minutes from resolution to the channel post).
  2. Catching slippage, rows that fall out of a shared table (dropped rows flagged, and rows put back afterwards).
"""
import json
import sys
from pathlib import Path

NOTIFY = [("tickets_announced", "Resolved tickets announced"), ("fix_incoming_announced", "Fix-incoming heads-ups"),
          ("people_tagged", "Agents tagged"), ("duplicates_consolidated", "Duplicate closures explained"),
          ("recaps", "Monday recaps"), ("recap_tickets", "Tickets caught by recaps"), ("status_dms", "Status DMs"),
          ("daily_lists", "Daily open-ticket lists")]
SLIPPAGE = [("table_drops_alerted", "Dropped rows flagged"), ("table_rows_restored", "Rows put back after a drop"),
            ("table_resolved_alerted", "Rows marked resolved, agent told"), ("table_bulk_flagged", "Large changes flagged for a human check")]


def average_minutes(week):
    samples = week.get("minutes_to_announce_samples", 0)
    return week.get("minutes_to_announce_total", 0) / samples if samples else None


def report(metrics):
    weeks = sorted(metrics)
    if not weeks:
        return "No counters yet. They fill in as the tool runs."
    lines = []
    total = {}
    for week in weeks:
        for name, value in metrics[week].items():
            total[name] = total.get(name, 0) + value
    lines.append(f"Weeks recorded: {len(weeks)} ({weeks[0]} to {weeks[-1]})")
    lines.append("")
    lines.append("1. Telling ticket openers sooner")
    for key, label in NOTIFY:
        lines.append(f"   {label}: {int(total.get(key, 0))}")
    avg = average_minutes(total)
    lines.append("   Average minutes from resolved in Jira to posted: " + (f"{avg:.0f}" if avg is not None else "no data yet"))
    lines.append("")
    lines.append("2. Catching slippage in shared tables")
    for key, label in SLIPPAGE:
        lines.append(f"   {label}: {int(total.get(key, 0))}")
    lines.append("")
    lines.append("By week (tickets announced / dropped rows flagged / average minutes to announce)")
    for week in weeks:
        w = metrics[week]
        a = average_minutes(w)
        lines.append(f"   {week}: {int(w.get('tickets_announced', 0))} / {int(w.get('table_drops_alerted', 0))} / " + (f"{a:.0f}" if a is not None else "-"))
    return "\n".join(lines)


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent / "state.json"
    print(report(json.loads(path.read_text(encoding="utf-8-sig")).get("metrics", {})))
