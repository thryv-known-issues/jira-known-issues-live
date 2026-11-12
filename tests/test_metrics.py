"""Tests for the weekly counters and the report. All data is made up."""
import contextlib
import io
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import jira_pinger as j  # noqa: E402
import metrics_report  # noqa: E402
from table_watch import runner  # noqa: E402

NOW = datetime(2026, 11, 12, 18, 0, tzinfo=timezone.utc)      # a Thursday


class Counters(unittest.TestCase):
    def test_week_key_uses_central_time(self):
        self.assertEqual(j.week_key(NOW), "2026-W46")
        monday_early = datetime(2026, 11, 9, 5, 30, tzinfo=timezone.utc)      # Sunday night in Central
        self.assertEqual(j.week_key(monday_early), "2026-W45")

    def test_bump_adds_and_does_not_change_the_input(self):
        base = {"2026-W46": {"tickets_announced": 2}}
        out = j.bump_metrics(base, NOW, tickets_announced=3, recaps=1)
        self.assertEqual(out["2026-W46"], {"tickets_announced": 5, "recaps": 1})
        self.assertEqual(base["2026-W46"], {"tickets_announced": 2})

    def test_bump_with_nothing_to_add_creates_no_empty_week(self):
        self.assertEqual(j.bump_metrics({}, NOW, tickets_announced=0, recaps=0), {})

    def test_only_numbers_are_ever_stored(self):
        out = j.bump_metrics({}, NOW, tickets_announced=2, minutes_to_announce_total=12.345)
        self.assertTrue(all(isinstance(v, (int, float)) for v in out["2026-W46"].values()))


class Report(unittest.TestCase):
    def test_report_sums_weeks_and_averages_minutes(self):
        metrics = {"2026-W45": {"tickets_announced": 4, "minutes_to_announce_total": 120, "minutes_to_announce_samples": 4, "table_drops_alerted": 1},
                   "2026-W46": {"tickets_announced": 6, "minutes_to_announce_total": 60, "minutes_to_announce_samples": 6, "table_drops_alerted": 2,
                                "table_rows_restored": 3, "recaps": 1}}
        text = metrics_report.report(metrics)
        self.assertIn("Resolved tickets announced: 10", text)
        self.assertIn("Average minutes from resolved in Jira to posted: 18", text)       # 180 minutes over 10 tickets
        self.assertIn("Dropped rows flagged: 3", text)
        self.assertIn("Rows put back after a drop: 3", text)
        self.assertIn("2026-W45: 4 / 1 / 30", text)

    def test_empty_report_says_so(self):
        self.assertIn("No counters yet", metrics_report.report({}))


class Wired(unittest.TestCase):
    def setUp(self):
        self._orig = {n: getattr(j, n) for n in ("send_slack_dm", "send_slack_channel", "save_state", "digest_due", "snapshot_id", "bot_preview_id",
                                                  "table_watch_preview_id", "load_config", "load_state", "fetch_jira_issues", "fetch_close_context", "fetch_issue_title")}

    def tearDown(self):
        for name, value in self._orig.items():
            setattr(j, name, value)

    def test_a_channel_post_updates_this_weeks_counters(self):
        sent, saved = [], []
        j.send_slack_dm = lambda config, text: None
        j.send_slack_channel = lambda config, text: sent.append(text)
        j.save_state = lambda s, last=None, ann=None, snap=None, held=None, extra=None: saved.append(extra)
        j.digest_due = lambda last, now=None: False
        j.snapshot_id = lambda: None
        j.bot_preview_id = lambda: None
        j.table_watch_preview_id = lambda: None
        j.load_config = lambda: {"JIRA_SITE": "https://example.invalid", "SLACK_CHANNEL_ID": "C1", "SLACK_USER_ID": "UME", "MENTIONS": {},
                                 "TABLE_WATCH_ENABLED": False, "TABLE_WATCH_DRY_RUN": True, "TABLE_WATCH_CHANNEL": "", "TABLE_WATCH_LABEL": "techsupport"}
        j.load_state = lambda: {"statuses": {}, "channel_announced": {"OLD-1": "2026-01-01T00:00:00+00:00"}, "channel_incoming": {}}
        j.fetch_close_context = lambda config, key: {"links": [], "comments": []}
        j.fetch_issue_title = lambda config, key: "Title"
        changed = (datetime.now(timezone.utc)).astimezone().strftime("%Y-%m-%dT%H:%M:%S.000%z")

        def issue(key, status):
            return {"key": key, "fields": {"summary": f"s {key}", "status": {"name": status}, "project": {"key": "T", "name": "T"},
                                           "reporter": {"displayName": "Ryan Lann"}, "statuscategorychangedate": changed}}
        rows = [issue("TP-1", "Done"), issue("TP-2", "Done")]
        j.fetch_jira_issues = lambda config, jql=j.JQL, fields="x": list(rows) if jql == j.CHANNEL_JQL else []
        sys.argv = ["jira_pinger.py"]
        with contextlib.redirect_stdout(io.StringIO()):
            j.main()
        week = saved[0]["metrics"][j.week_key()]
        self.assertEqual((week["channel_posts"], week["tickets_announced"]), (1, 2))
        self.assertEqual(week["minutes_to_announce_samples"], 2)
        self.assertTrue(0 <= week["minutes_to_announce_total"] < 5)             # they were closed a moment ago


class RunnerCounts(unittest.TestCase):
    def test_runner_counts_each_kind_and_restored_rows(self):
        head = "||Thryv ID||ZD Ticket||Agent||Resolved||"
        make = lambda rows_: "Intro\n\n" + head + "\n" + "\n".join("|" + "|".join(r) + "|" for r in rows_)
        rows_ = [["a1", "4000001", "Karla Cruz", ""], ["b2", "4000002", "Karla Cruz", ""], ["c3", "4000003", "Karla Cruz", ""]]
        gone = rows_[:2]
        yes = [["a1", "4000001", "Karla Cruz", "Yes"], rows_[1]]
        history = [
            {"id": "5", "created": "2026-11-12T10:00:00.000-0600", "author": {"displayName": "Someone"},
             "items": [{"field": "description", "fromString": make(rows_), "toString": make(gone)}]},
            {"id": "6", "created": "2026-11-12T10:10:00.000-0600", "author": {"displayName": "Someone"},
             "items": [{"field": "description", "fromString": make(gone), "toString": make(rows_)}]},
            {"id": "7", "created": "2026-11-12T10:20:00.000-0600", "author": {"displayName": "Someone"},
             "items": [{"field": "description", "fromString": make(rows_), "toString": make(yes + rows_[2:])}]},
        ]
        sent = []
        cfg = {"JIRA_SITE": "https://example.invalid", "MENTIONS": {"Karla Cruz": "UKARLA"}}
        state, counts = runner.run(cfg, {"last": {"TP-1": 4}, "sent": {}}, lambda jql: [{"key": "TP-1", "labels": ["techsupport"]}], lambda key: history, sent.append)
        self.assertEqual((counts["drops"], counts["resolved_marks"], counts["restored"]), (0, 1, 1))     # the drop was undone before this check
        self.assertEqual(len(sent), 1)


if __name__ == "__main__":
    unittest.main()
