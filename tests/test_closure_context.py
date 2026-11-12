"""Tests for closure context: tickets closed as a duplicate are not announced as resolved. All data is made up."""
import contextlib
import io
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import jira_pinger as j  # noqa: E402

CFG = {"JIRA_SITE": "https://example.invalid", "JIRA_EMAIL": "e", "JIRA_API_TOKEN": "t"}


def adf(text):
    return {"type": "doc", "version": 1, "content": [{"type": "paragraph", "content": [{"type": "text", "text": text}]}]}


class Helpers(unittest.TestCase):
    def test_adf_text_reads_nested_content_and_plain_strings(self):
        body = {"type": "doc", "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Closing as a"}, {"type": "text", "text": "duplicate."}]},
                                           {"type": "paragraph", "content": [{"type": "inlineCard", "attrs": {"url": "https://x.example/browse/TP-1"}}]}]}
        text = j.adf_text(body)
        self.assertIn("Closing as a duplicate.", text)
        self.assertIn("TP-1", text)
        self.assertEqual(j.adf_text("plain"), "plain")
        self.assertEqual(j.adf_text(None), "")

    def test_resolution_duplicate(self):
        self.assertTrue(j.duplicate_by_resolution("Duplicate"))
        self.assertTrue(j.duplicate_by_resolution("duplicate"))
        self.assertFalse(j.duplicate_by_resolution("Done"))
        self.assertFalse(j.duplicate_by_resolution(None))

    def test_context_words_and_links(self):
        self.assertTrue(j.duplicate_by_context({"comments": ["All accounts are consolidated into TP-9"], "links": []}))
        self.assertTrue(j.duplicate_by_context({"comments": ["Closing as a duplicate"], "links": []}))
        self.assertTrue(j.duplicate_by_context({"comments": [], "links": ["Duplicate is duplicated by duplicates"]}))
        self.assertFalse(j.duplicate_by_context({"comments": ["Fixed in prod, please verify"], "links": ["Relates relates to relates to"]}))


class Review(unittest.TestCase):
    def test_resolution_duplicate_is_left_out_without_any_lookup(self):
        calls = []
        items = [("T-1", "s", "Cancelled"), ("T-2", "s", "Done")]
        post, hidden = j.review_closures(CFG, items, {"T-1": "Duplicate", "T-2": "Done"}, fetch=lambda c, k: calls.append(k))
        self.assertEqual((post, hidden, calls), ([("T-2", "s", "Done")], ["T-1"], []))

    def test_cancelled_without_a_resolution_is_checked_by_links_and_comments(self):
        contexts = {"T-1": {"comments": ["Closing as a dupe of T-9"], "links": []}, "T-3": {"comments": ["not needed anymore"], "links": []}}
        items = [("T-1", "s", "Cancelled"), ("T-3", "s", "Cancelled"), ("T-4", "s", "Done")]
        post, hidden = j.review_closures(CFG, items, {}, fetch=lambda c, k: contexts[k])
        self.assertEqual(([i[0] for i in post], hidden), (["T-3", "T-4"], ["T-1"]))

    def test_done_tickets_are_never_looked_up(self):
        calls = []
        post, hidden = j.review_closures(CFG, [("T-4", "s", "Done"), ("T-5", "s", "Production")], {}, fetch=lambda c, k: calls.append(k))
        self.assertEqual((len(post), hidden, calls), (2, [], []))

    def test_a_failed_lookup_still_posts(self):
        def boom(config, key):
            raise RuntimeError("jira down")
        post, hidden = j.review_closures(CFG, [("T-1", "s", "Cancelled")], {}, fetch=boom)
        self.assertEqual((len(post), hidden), (1, []))


class InMain(unittest.TestCase):
    """The five Cancelled-as-Duplicate tickets from the report stay out of the channel, a normal Done ticket still posts."""

    def setUp(self):
        self._orig = {n: getattr(j, n) for n in ("send_slack_dm", "send_slack_channel", "save_state", "digest_due", "snapshot_id", "bot_preview_id",
                                                  "table_watch_preview_id", "load_config", "load_state", "fetch_jira_issues", "fetch_close_context")}

    def tearDown(self):
        for name, value in self._orig.items():
            setattr(j, name, value)

    def test_duplicates_are_not_posted_and_are_remembered(self):
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
        j.fetch_close_context = lambda config, key: {"comments": [], "links": []}

        def issue(key, status, resolution=None):
            fields = {"summary": f"s {key}", "status": {"name": status}, "project": {"key": "T", "name": "T"}, "reporter": {"displayName": "Ryan Lann"}}
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
        self.assertIn("TP-3", sent[0])
        self.assertNotIn("TP-1", sent[0])
        self.assertNotIn("TP-2", sent[0])
        self.assertTrue({"TP-1", "TP-2", "TP-3"} <= set(saved[0]))          # all three remembered, so none is looked at again
        self.assertIn("2 closed as a duplicate and left out", buf.getvalue())
        self.assertNotIn("TP-1", buf.getvalue())                            # the public log shows counts only


if __name__ == "__main__":
    unittest.main()
