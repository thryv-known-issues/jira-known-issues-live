"""Tests for closure context: Cancelled tickets are posted with the reason they were closed. All data is made up."""
import contextlib
import io
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import jira_pinger as j  # noqa: E402

CFG = {"JIRA_SITE": "https://example.invalid", "JIRA_EMAIL": "e", "JIRA_API_TOKEN": "t"}
SITE = "https://example.invalid/browse"


def comment(text, created="2026-11-11T20:44:48.147-0800"):
    return {"text": text, "created": created}


def adf(text):
    return {"type": "doc", "version": 1, "content": [{"type": "paragraph", "content": [{"type": "text", "text": text}]}]}


class Helpers(unittest.TestCase):
    def test_adf_text_reads_nested_content_links_and_plain_strings(self):
        body = {"type": "doc", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Closing as a"}, {"type": "text", "text": "duplicate."}]},
                                           {"type": "paragraph", "content": [{"type": "inlineCard", "attrs": {"url": "https://x.example/browse/TP-43198"}}]}]}
        text = j.adf_text(body)
        self.assertIn("Closing as a duplicate.", text)
        self.assertIn("TP-43198", text)
        self.assertEqual((j.adf_text("plain"), j.adf_text(None)), ("plain", ""))

    def test_jira_time_formats(self):
        self.assertEqual(j.parse_jira_time("2026-11-11T20:44:48.152-0800").utcoffset().total_seconds(), -8 * 3600)
        self.assertIsNone(j.parse_jira_time(""))
        self.assertIsNone(j.parse_jira_time("garbage"))


class Notes(unittest.TestCase):
    CLOSED = j.parse_jira_time("2026-11-11T20:44:48.152-0800")

    def test_resolution_duplicate_with_a_comment_naming_the_target(self):
        ctx = {"links": [], "comments": [comment('Closing as a duplicate. All accounts consolidated into https://x.example/browse/TP-43198#icft=TP-43198 .')]}
        self.assertEqual(j.closure_note("TP-1", "Cancelled", "Duplicate", ctx, self.CLOSED), {"dup_of": "TP-43198", "reason": "duplicate"})

    def test_duplicate_link_beats_comment_parsing(self):
        ctx = {"links": [{"text": "Duplicate is duplicated by duplicates", "key": "TP-777"}], "comments": [comment("Closing as a duplicate of TP-5")]}
        self.assertEqual(j.closure_note("TP-1", "Cancelled", "Duplicate", ctx, self.CLOSED)["dup_of"], "TP-777")

    def test_duplicate_without_a_findable_target_still_says_duplicate(self):
        self.assertEqual(j.closure_note("TP-1", "Cancelled", "Duplicate", {"links": [], "comments": []}, self.CLOSED), {"dup_of": None, "reason": "duplicate"})

    def test_the_tickets_own_key_is_never_the_target(self):
        ctx = {"links": [], "comments": [comment("TP-1 closed as a duplicate of TP-9")]}
        self.assertEqual(j.closure_note("TP-1", "Cancelled", "Duplicate", ctx, self.CLOSED)["dup_of"], "TP-9")

    def test_wont_do_with_a_closing_comment_shows_the_comment(self):
        ctx = {"links": [], "comments": [comment("Customer fixed it on their side, not needed. Contact jane@x.example")]}
        note = j.closure_note("TP-1", "Cancelled", "Won't Do", ctx, self.CLOSED)
        self.assertEqual(note["dup_of"], None)
        self.assertTrue(note["reason"].startswith("Won't Do, \"Customer fixed it on their side"))
        self.assertNotIn("jane@x.example", note["reason"])                  # customer emails are masked

    def test_only_a_comment_near_the_status_change_counts_as_the_closing_comment(self):
        old = comment("Please send the HAR file", "2026-10-26T11:45:43.502-0700")
        near = comment("Cancelled, the merchant never replied", "2026-11-11T20:40:00.000-0800")
        self.assertIn("never replied", j.closure_note("TP-1", "Cancelled", None, {"links": [], "comments": [old, near]}, self.CLOSED)["reason"])
        self.assertIsNone(j.closure_note("TP-1", "Cancelled", None, {"links": [], "comments": [old]}, self.CLOSED)["reason"])

    def test_a_long_comment_is_shortened(self):
        long_text = "not needed " * 40
        reason = j.closure_note("TP-1", "Cancelled", None, {"links": [], "comments": [comment(long_text)]}, self.CLOSED)["reason"]
        self.assertTrue(len(reason) < 140 and reason.endswith('..."'))

    def test_no_context_means_no_note(self):
        self.assertEqual(j.closure_note("TP-1", "Cancelled", None, {"links": [], "comments": []}, self.CLOSED), {"dup_of": None, "reason": None})


class Review(unittest.TestCase):
    def test_only_cancelled_type_tickets_are_looked_up_and_failures_are_harmless(self):
        calls = []
        def fetch(config, key):
            calls.append(key)
            if key == "T-3":
                raise RuntimeError("jira down")
            return {"links": [], "comments": [comment("Closing as a duplicate of TP-9", "2026-11-11T20:44:00.000-0800")]}
        items = [("T-1", "s", "Cancelled"), ("T-2", "s", "Done"), ("T-3", "s", "Cancelled"), ("T-4", "s", "Production")]
        changed = {"T-1": "2026-11-11T20:44:48.152-0800", "T-3": "2026-11-11T20:44:48.152-0800"}
        notes = j.review_closures(CFG, items, {"T-1": "Duplicate"}, changed, fetch=fetch)
        self.assertEqual(calls, ["T-1", "T-3"])
        self.assertEqual(notes, {"T-1": {"dup_of": "TP-9", "reason": "duplicate"}})

    def test_a_done_ticket_with_resolution_duplicate_gets_a_note_without_any_lookup(self):
        calls = []
        notes = j.review_closures(CFG, [("T-1", "s", "Closed")], {"T-1": "Duplicate"}, {}, fetch=lambda c, k: calls.append(k))
        self.assertEqual((calls, notes["T-1"]["reason"]), ([], "duplicate"))


class Posting(unittest.TestCase):
    def batch(self, items, notes, tags=None):
        return list(j.build_channel_batches(CFG, items, tags or {}, notes=notes))

    def test_five_duplicates_of_one_ticket_become_one_line(self):
        items = [(f"TP-{n}", f"Update Deposit Status {n}", "Cancelled") for n in (1, 2, 3, 4, 5)]
        notes = {k: {"dup_of": "TP-99", "reason": "duplicate"} for k, _, _ in items}
        text, keys = self.batch(items, notes, {"TP-1": ["UME"], "TP-2": ["UME"]})[0]
        lines = text.splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn(f"<{SITE}/TP-1|TP-1>, <{SITE}/TP-2|TP-2>, <{SITE}/TP-3|TP-3>, <{SITE}/TP-4|TP-4> and <{SITE}/TP-5|TP-5> closed as duplicates of <{SITE}/TP-99|TP-99> (Cancelled)", lines[1])
        self.assertEqual(lines[1].count("<@UME>"), 1)                        # the same person is tagged once
        self.assertEqual(sorted(keys), ["TP-1", "TP-2", "TP-3", "TP-4", "TP-5"])

    def test_a_single_duplicate_keeps_its_summary_and_names_the_target(self):
        text, _ = self.batch([("TP-1", "Some summary", "Cancelled")], {"TP-1": {"dup_of": "TP-99", "reason": "duplicate"}})[0]
        self.assertIn(f"- <{SITE}/TP-1|TP-1> Some summary (Cancelled: duplicate of <{SITE}/TP-99|TP-99>)", text)

    def test_other_reasons_show_in_the_brackets_and_plain_tickets_are_unchanged(self):
        items = [("TP-1", "A", "Cancelled"), ("TP-2", "B", "Done")]
        text, _ = self.batch(items, {"TP-1": {"dup_of": None, "reason": "Won't Do, \"not needed\""}})[0]
        self.assertIn("(Cancelled: Won't Do, \"not needed\")", text)
        self.assertIn(f"- <{SITE}/TP-2|TP-2> B (Done)", text)

    def test_duplicates_of_different_tickets_stay_separate_and_mixed_with_normal_lines(self):
        items = [("TP-1", "A", "Cancelled"), ("TP-2", "B", "Cancelled"), ("TP-3", "C", "Cancelled"), ("TP-4", "D", "Done")]
        notes = {"TP-1": {"dup_of": "TP-90", "reason": "duplicate"}, "TP-2": {"dup_of": "TP-90", "reason": "duplicate"}, "TP-3": {"dup_of": "TP-91", "reason": "duplicate"}}
        text, keys = self.batch(items, notes)[0]
        lines = text.splitlines()
        self.assertEqual(len(lines), 4)
        self.assertIn("closed as duplicates of", lines[1])
        self.assertIn("(Cancelled: duplicate of", lines[2])
        self.assertIn("(Done)", lines[3])
        self.assertEqual(sorted(keys), ["TP-1", "TP-2", "TP-3", "TP-4"])

    def test_without_notes_the_post_is_exactly_as_before(self):
        text, keys = self.batch([("TP-1", "A", "Done")], None, {"TP-1": ["U1"]})[0]
        self.assertEqual(text, f":white_check_mark: Resolved, please check:\n- <{SITE}/TP-1|TP-1> A (Done) <@U1>")


class Intact(unittest.TestCase):
    def test_the_other_message_builders_still_exist_and_work(self):
        recap = j.build_recap(CFG, [("TP-1", "A", "Done")], [("TP-2", "B", "Integration")], {})
        self.assertIn("Weekend recap", recap)
        self.assertIn("TP-1", recap)
        self.assertIn("TP-2", recap)
        self.assertEqual(len(j.build_bot_previews({**CFG, "SLACK_USER_ID": "UME"})), 4)


class InMain(unittest.TestCase):
    """The tickets from the report stay in the channel, grouped, with the reason, and the public log shows counts only."""

    def setUp(self):
        self._orig = {n: getattr(j, n) for n in ("send_slack_dm", "send_slack_channel", "save_state", "digest_due", "snapshot_id", "bot_preview_id",
                                                  "table_watch_preview_id", "load_config", "load_state", "fetch_jira_issues", "fetch_close_context")}

    def tearDown(self):
        for name, value in self._orig.items():
            setattr(j, name, value)

    def test_cancelled_duplicates_are_grouped_with_their_target(self):
        sent, saved = [], []
        j.send_slack_dm = lambda config, text: None
        j.send_slack_channel = lambda config, text: sent.append(text)
        j.save_state = lambda s, last=None, ann=None, snap=None, held=None, extra=None: saved.append(ann)
        j.digest_due = lambda last, now=None: False
        j.snapshot_id = lambda: None
        j.bot_preview_id = lambda: None
        j.table_watch_preview_id = lambda: None
        j.load_config = lambda: {"JIRA_SITE": "https://example.invalid", "SLACK_CHANNEL_ID": "C1", "SLACK_USER_ID": "UME", "MENTIONS": {},
                                 "TABLE_WATCH_ENABLED": False, "TABLE_WATCH_DRY_RUN": True, "TABLE_WATCH_CHANNEL": "", "TABLE_WATCH_LABEL": "techsupport"}
        j.load_state = lambda: {"statuses": {}, "channel_announced": {"OLD-1": "2026-01-01T00:00:00+00:00"}, "channel_incoming": {}}
        j.fetch_close_context = lambda config, key: {"links": [], "comments": [comment("Closing as a duplicate. Consolidated into https://x.example/browse/TP-43198#icft=TP-43198")]}

        def issue(key, status, resolution=None):
            fields = {"summary": f"s {key}", "status": {"name": status}, "project": {"key": "T", "name": "T"}, "reporter": {"displayName": "Ryan Lann"},
                      "statuscategorychangedate": "2026-11-11T20:44:48.152-0800"}
            if resolution:
                fields["resolution"] = {"name": resolution}
            return {"key": key, "fields": fields}
        rows = [issue("TP-1", "Cancelled", "Duplicate"), issue("TP-2", "Cancelled", "Duplicate"), issue("TP-3", "Done", "Done")]
        j.fetch_jira_issues = lambda config, jql=j.JQL, fields="x": list(rows) if jql == j.CHANNEL_JQL else []
        sys.argv = ["jira_pinger.py"]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            j.main()
        self.assertEqual(len(sent), 1)
        self.assertIn("TP-1|TP-1>", sent[0])
        self.assertIn("closed as duplicates of", sent[0])
        self.assertIn("TP-43198", sent[0])
        self.assertIn("(Done)", sent[0])
        self.assertTrue({"TP-1", "TP-2", "TP-3"} <= set(saved[0]))
        self.assertIn("2 with a closure reason added", buf.getvalue())
        self.assertNotIn("TP-43198", buf.getvalue())                       # the public log shows counts only


if __name__ == "__main__":
    unittest.main()

