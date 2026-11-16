"""Print the weekly counters kept in state.json (numbers only).

Usage:
  python metrics_report.py                      print the report
  python metrics_report.py --csv impact.csv     also write one row per week that opens in Excel

The two things this tool is for:
  1. Telling ticket openers sooner when their Jira is resolved (minutes from resolution to the channel post).
  2. Catching slippage, rows that fall out of a shared table (dropped rows flagged, and rows put back afterwards).
"""
import csv
import json
import sys
from pathlib import Path

NOTIFY = [("tickets_announced", "Resolved tickets announced"), ("fix_incoming_announced", "Fix-incoming heads-ups"),
          ("people_tagged", "Agents tagged"), ("duplicates_consolidated", "Duplicate closures explained"),
          ("recaps", "Monday recaps"), ("recap_tickets", "Tickets caught by recaps"), ("status_dms", "Status DMs"),
          ("daily_lists", "Daily open-ticket lists")]
SLIPPAGE = [("table_drops_alerted", "Dropped rows flagged"), ("table_rows_restored", "Rows put back after a drop"),
            ("table_resolved_alerted", "Rows marked resolved, agent told"), ("table_bulk_flagged", "Large changes flagged for a human check")]
# The waiting-time counters were renamed when the old ones turned out to include a date artifact. Old names are ignored.
WAIT_TOTAL, WAIT_SAMPLES, WAIT_IGNORED = "announce_minutes_total", "announce_minutes_samples", "announce_minutes_ignored"


def average_minutes(week):
    samples = week.get(WAIT_SAMPLES, 0)
    return week.get(WAIT_TOTAL, 0) / samples if samples else None


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
    lines.append("   Average minutes from resolved in Jira to posted: " + (f"{avg:.0f} (from {int(total.get(WAIT_SAMPLES, 0))} tickets)" if avg is not None else "no data yet"))
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


def write_csv(metrics, path):
    columns = [k for k, _ in NOTIFY + SLIPPAGE]
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:       # utf-8-sig so Excel opens it cleanly
        writer = csv.writer(handle)
        writer.writerow(["Week"] + [label for _, label in NOTIFY + SLIPPAGE] + ["Average minutes to announce"])
        for week in sorted(metrics):
            w = metrics[week]
            avg = average_minutes(w)
            writer.writerow([week] + [int(w.get(c, 0)) for c in columns] + ["" if avg is None else round(avg)])


if __name__ == "__main__":
    args = sys.argv[1:]
    csv_path = None
    if "--csv" in args:
        i = args.index("--csv")
        csv_path = args[i + 1]
        args = args[:i] + args[i + 2:]
    path = Path(args[0]) if args else Path(__file__).resolve().parent / "state.json"
    data = json.loads(path.read_text(encoding="utf-8-sig")).get("metrics", {})
    print(report(data))
    if csv_path:
        write_csv(data, csv_path)
        print(f"\nWrote {csv_path}")
