"""Tests for table watch. All data is made up; no real account IDs, ticket numbers or names."""
import contextlib
import io
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import jira_pinger as j  # noqa: E402
from table_watch.detect import analyze  # noqa: E402
from table_watch.message import build_messages, name_map_from_mentions  # noqa: E402
from table_watch.parse import parse_tables  # noqa: E402
from table_watch import runner  # noqa: E402

HEAD = "||*Thryv ID*||*Country+Package*||*Zendesk Ticket*||*Tech Support Agent*||"
MENTIONS = {"Agent Karla": "UKARLA", "Agent Naomi": "UNAOMI", "Naomi Jiminian": "UNAOMI", "Agent Barb": "UBARB",
            "Barb Larrosa": "UBARB", "Former Rep": "UBARB"}
NAMES = name_map_from_mentions(MENTIONS)
JIRA = "https://example.invalid"


def desc(*tables):
    return "Intro text.\n\n" + "\n\n".join(tables)


def table(rows, head=HEAD):
    return head + "\n" + "\n".join("|" + "|".join(r) + "|" for r in rows)


def save(hid, before, after, when="2026-10-26T10:00:00.000-0700", who="Someone"):
    return {"id": str(hid), "created": when, "author": {"displayName": who},
            "items": [{"field": "description", "fromString": before, "toString": after}]}


def rows(n, agent="Agent Karla"):
    return [[f"id{i}", "Pkg", str(4000 + i), agent] for i in range(n)]


class Fixtures(unittest.TestCase):
    def test_overwrite_is_one_alert_with_signature(self):
        base = rows(11)
        new_row = ["zzz111", "Pkg", "4999", "Agent Barb"]
        other = ["yyy222", "Pkg", "4998", "Agent Naomi"]
        h = [save(1, desc(table(base)), desc(table(base + [new_row])), "2026-10-26T08:43:17.295-0700", "Agent Barb"),
             save(2, desc(table(base)), desc(table(base + [other])), "2026-10-26T08:43:51.650-0700", "Agent Naomi")]
        res = analyze("TP-1", h, labels=["techsupport"])
        self.assertEqual(len(res.alerts), 0)  # the second save's "before" is the older text, so only the add of "other" differs
        h2 = [save(1, desc(table(base)), desc(table(base + [new_row])), "2026-10-26T08:43:17.295-0700", "Agent Barb"),
              save(2, desc(table(base + [new_row])), desc(table(base + [other])), "2026-10-26T08:43:51.650-0700", "Agent Naomi")]
        res = analyze("TP-1", h2, labels=["techsupport"])
        self.assertEqual(len(res.alerts), 1)
        a = res.alerts[0]
        self.assertEqual((a.rows[0], a.agent, a.history_id), (new_row, "Agent Barb", "2"))
        self.assertAlmostEqual(a.signature_seconds, 34.355, places=2)

    def test_dedupe_is_not_a_drop(self):
        r = rows(4)
        dup = r + [["id1", "Pkg", "9999", "Agent Karla"]]
        res = analyze("TP-1", [save(1, desc(table(dup)), desc(table(r)))])
        self.assertEqual(res.alerts, [])
        self.assertEqual([e.kind for e in res.logs], ["consolidation"])

    def test_normal_adds_and_other_fields_no_alert(self):
        r = rows(3)
        h = [save(1, desc(table(r)), desc(table(r + rows(1)[:0] + [["new1", "Pkg", "1", "Agent Karla"]]))),
             {"id": "2", "created": "2026-10-26T10:01:00.000-0700", "author": {"displayName": "Auto"},
              "items": [{"field": "status", "fromString": "Open", "toString": "Done"}]}]
        res = analyze("TP-1", h)
        self.assertEqual((res.alerts, res.logs), ([], []))


class Parsing(unittest.TestCase):
    def test_link_markup_and_bold_stripped(self):
        t = parse_tables(table([["tem1", "AU", "[4845793|https://x.example/agent/tickets/4845793]", "Barb"]]))[0]
        self.assertEqual(t.header, ["Thryv ID", "Country+Package", "Zendesk Ticket", "Tech Support Agent"])
        self.assertEqual(t.rows[0], ["tem1", "AU", "4845793", "Barb"])

    def test_alternate_columns_and_no_ticket_column(self):
        t = parse_tables("||*Notes*||*Location ID*||*ZD Ticket*||\n|x|LOC1|55|")[0]
        self.assertEqual(t.key(t.rows[0]), ("loc1", "55"))
        t = parse_tables("||*Account ID*||*Agent*||\n|ABC|Agent Karla|")[0]
        self.assertEqual(t.key(t.rows[0]), ("abc",))


class Detection(unittest.TestCase):
    def test_two_tables_only_matching_header_compared(self):
        h2 = "||*Location ID*||*Ticket*||*Agent*||"
        t2 = [["LOC1", "55", "Agent Karla"], ["LOC2", "56", "Agent Karla"]]
        res = analyze("TP-1", [save(1, desc(table(rows(3)), table(t2, head=h2)), desc(table(rows(3)), table(t2[:1], head=h2)))])
        self.assertEqual([(a.table_no, a.table_count, a.keys) for a in res.alerts], [(2, 2, [("loc2", "56")])])
        msgs, _ = build_messages(res.alerts, NAMES, JIRA)
        self.assertIn("(table 2)", msgs[0]["text"])

    def test_changed_header_is_not_compared(self):
        after = desc(table([], head="||*Thryv ID*||*Package*||*Zendesk Ticket*||*Tech Support Agent*||"))
        self.assertEqual(analyze("TP-1", [save(1, desc(table(rows(3))), after)]).alerts, [])

    def test_three_drops_grouped_one_per_line(self):
        r = rows(10)
        r[8][3] = r[9][3] = "Naomi Jiminian"
        res = analyze("TP-1", [save(1, desc(table(r)), desc(table(r[:7])))])
        msgs, _ = build_messages(res.alerts, NAMES, JIRA)
        self.assertEqual(len(msgs), 2)
        naomi = next(m for m in msgs if m["user_id"] == "UNAOMI")
        self.assertIn("id8 (ZD <https://thryv.zendesk.com/4008|4008>) and id9", naomi["text"])
        self.assertIn("```\nid8 | Pkg | 4008 | Naomi Jiminian\nid9 | Pkg | 4009 | Naomi Jiminian\n```", naomi["text"])

    def test_bulk_removal_untagged(self):
        res = analyze("TP-1", [save(1, desc(table(rows(4))), desc(table(rows(1))))])
        self.assertEqual([a.kind for a in res.alerts], ["bulk"])
        msgs, _ = build_messages(res.alerts, NAMES, JIRA)
        self.assertIsNone(msgs[0]["user_id"])
        self.assertNotIn("<@", msgs[0]["text"])
        self.assertIn("3 of 4 rows", msgs[0]["text"])

    def test_unmapped_agent_untagged_and_reported(self):
        r = rows(4, "Someone New")
        res = analyze("TP-1", [save(1, desc(table(r)), desc(table(r[:3])))])
        msgs, unmapped = build_messages(res.alerts, NAMES, JIRA)
        self.assertIsNone(msgs[0]["user_id"])
        self.assertTrue(msgs[0]["text"].startswith("id3 (ZD"))
        self.assertEqual(unmapped, ["Someone New"])

    def test_spelling_and_accent_variants_match_and_reroute_entry_works(self):
        names = name_map_from_mentions({"Naomi Jiminian": "UNAOMI", "Former Rep": "USCOTT"})
        r = rows(4, "Naomi Jiminián")
        res = analyze("TP-1", [save(1, desc(table(r)), desc(table(r[:3])))])
        self.assertEqual(build_messages(res.alerts, names, JIRA)[0][0]["user_id"], "UNAOMI")
        r = rows(4, "former rep")
        res = analyze("TP-1", [save(1, desc(table(r)), desc(table(r[:3])))])
        self.assertEqual(build_messages(res.alerts, names, JIRA)[0][0]["user_id"], "USCOTT")

    def test_message_matches_handoff_format(self):
        r = rows(4, "Agent Barb")
        r[3] = ["lem1", "US Signature+", "4864337", "Agent Barb"]
        res = analyze("TP-9", [save(1, desc(table(r)), desc(table(r[:3])))])
        msgs, _ = build_messages(res.alerts, NAMES, JIRA)
        self.assertEqual(msgs[0]["text"], (
            "<@UBARB> lem1 (ZD <https://thryv.zendesk.com/4864337|4864337>) may have been dropped from "
            "<https://example.invalid/browse/TP-9|TP-9>. Please review and re-add if applicable.\n"
            "```\nlem1 | US Signature+ | 4864337 | Agent Barb\n```"))
        self.assertNotIn("—", msgs[0]["text"])

    def test_re_add_after_alert(self):
        r = rows(4)
        gone, back = desc(table(r[:3])), desc(table(r))
        res = analyze("TP-1", [save(1, desc(table(r)), gone), save(2, gone, back, "2026-10-26T10:05:00.000-0700")])
        self.assertEqual(len(res.alerts), 1)
        self.assertEqual([e.kind for e in res.logs], ["readded"])

    def test_resolved_and_acknowledged_not_alerted(self):
        head = "||*Thryv ID*||*Zendesk Ticket*||*Tech Support Agent*||*Resolved*||"
        r = [["a", "1", "Agent Karla", "yes"], ["b", "2", "Agent Karla", ""], ["c", "3", "Agent Karla", ""], ["d", "4", "Agent Karla", ""]]
        res = analyze("TP-1", [save(1, desc(table(r, head)), desc(table(r[1:], head)))])
        self.assertEqual((res.alerts, [e.kind for e in res.logs]), ([], ["resolved"]))
        res = analyze("TP-1", [save(1, desc(table(rows(4))), desc(table(rows(4)[:3])))], muted={("id3", "4003")})
        self.assertEqual((res.alerts, [e.kind for e in res.logs]), ([], ["acknowledged"]))

    def test_ticket_without_label_ignored(self):
        h = [save(1, desc(table(rows(4))), desc(table(rows(4)[:3])))]
        self.assertEqual(analyze("TP-1", h, labels=["other"]).alerts, [])
        self.assertEqual(len(analyze("TP-1", h, labels=["techsupport"]).alerts), 1)


class Runner(unittest.TestCase):
    def setUp(self):
        r = rows(4)
        self.before, self.after = desc(table(r)), desc(table(r[:3]))
        self.config = {"JIRA_SITE": JIRA, "MENTIONS": MENTIONS}
        self.sent = []

    def go(self, state, histories, send=None):
        return runner.run(self.config, state, lambda jql: [{"key": "TP-1", "labels": ["techsupport"]}],
                          lambda key: histories, send or self.sent.append)

    def test_first_sight_baselines_without_alerting(self):
        state, counts = self.go(None, [save(5, self.before, self.after)])
        self.assertEqual((self.sent, counts["baselined"], state["last"]["TP-1"]), ([], 1, 5))

    def test_alert_sent_once_and_state_holds_no_ids(self):
        state = {"last": {"TP-1": 4}, "sent": {}}
        h = [save(5, self.before, self.after)]
        state, counts = self.go(state, h)
        self.assertEqual((len(self.sent), counts["alerts"], state["last"]["TP-1"]), (1, 1, 5))
        blob = repr(state)
        self.assertNotIn("id3", blob)
        self.assertNotIn("4003", blob)
        state, counts = self.go(state, h)             # same history again: nothing new
        self.assertEqual(len(self.sent), 1)
        state2, _ = self.go({"last": {"TP-1": 4}, "sent": state["sent"]}, h)   # pointer lost, hash kept: still once
        self.assertEqual(len(self.sent), 1)

    def test_older_saves_are_not_alerted(self):
        self.go({"last": {"TP-1": 9}, "sent": {}}, [save(5, self.before, self.after)])
        self.assertEqual(self.sent, [])

    def test_send_failure_keeps_pointer_for_retry(self):
        def boom(message):
            raise RuntimeError("slack down")
        state, counts = self.go({"last": {"TP-1": 4}, "sent": {}}, [save(5, self.before, self.after)], send=boom)
        self.assertEqual((state["last"]["TP-1"], state["sent"], counts["failed"]), (4, {}, 1))
        state, _ = self.go(state, [save(5, self.before, self.after)])
        self.assertEqual(len(self.sent), 1)

    def test_changelog_pagination(self):
        pages = [{"values": [{"id": "1"}, {"id": "2"}], "total": 3, "isLast": False}, {"values": [{"id": "3"}], "total": 3, "isLast": True}]
        urls = []

        class Resp:
            def __init__(self, data): self.data = data
            def raise_for_status(self): pass
            def json(self): return self.data

        def fake_get(url, params=None, **kw):
            urls.append(params["startAt"])
            return Resp(pages[len(urls) - 1])

        real, j.requests.get = j.requests.get, fake_get
        try:
            out = j.fetch_changelog({"JIRA_SITE": JIRA, "JIRA_EMAIL": "e", "JIRA_API_TOKEN": "t"}, "TP-1", page_size=2)
        finally:
            j.requests.get = real
        self.assertEqual(([h["id"] for h in out], urls), (["1", "2", "3"], [0, 2]))


class Wiring(unittest.TestCase):
    """The new layer inside main(): off by default, dry run goes to the owner's DM only, errors never stop the pings."""

    def run_main(self, env, tw_raises=False, watch_state=None, labelled=None):
        sent_dm, saved = [], []
        j.send_slack_dm = lambda config, text: sent_dm.append(text)
        j.send_slack_channel = lambda config, text: None
        j.save_state = lambda s, last=None, ann=None, snap=None, held=None, extra=None: saved.append(extra)
        j.digest_due = lambda last, now=None: False
        j.snapshot_id = lambda: None
        j.bot_preview_id = lambda: None
        base = {"JIRA_SITE": JIRA, "SLACK_CHANNEL_ID": "", "SLACK_USER_ID": "UME", "MENTIONS": MENTIONS,
                "TABLE_WATCH_ENABLED": False, "TABLE_WATCH_DRY_RUN": True, "TABLE_WATCH_CHANNEL": "", "TABLE_WATCH_LABEL": "techsupport"}
        base.update(env)
        j.load_config = lambda: base
        j.load_state = lambda: {"statuses": {"A-1": "Open"}, "table_watch": watch_state}
        status_issue = {"key": "A-1", "fields": {"status": {"name": "Done"}, "summary": "s", "project": {"key": "A", "name": "A"}}}
        j.fetch_jira_issues = lambda config, jql=j.JQL, fields="x": (
            [{"key": "TP-1", "fields": {"labels": ["techsupport"]}}] if "updated >=" in jql and "labels = " in jql and fields == "labels"
            else [status_issue])
        posted = []
        j._post = lambda config, channel, text: posted.append((channel, text))
        if tw_raises:
            def boom(config, state):
                raise ValueError("boom")
            j.table_watch_step = boom
        sys.argv = ["jira_pinger.py"]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            j.main()
        return buf.getvalue(), sent_dm, saved, posted

    def setUp(self):
        self._orig = {n: getattr(j, n) for n in ("send_slack_dm", "send_slack_channel", "save_state", "digest_due", "snapshot_id",
                                                  "bot_preview_id", "load_config", "load_state", "fetch_jira_issues", "_post",
                                                  "table_watch_step")}

    def tearDown(self):
        for name, value in self._orig.items():
            setattr(j, name, value)

    def test_off_by_default_does_nothing(self):
        out, dms, saved, posted = self.run_main({})
        self.assertNotIn("Table watch", out)
        self.assertEqual(len(dms), 1)

    def test_dry_run_goes_to_owner_dm_only_and_log_has_counts_only(self):
        r = rows(4, "Agent Barb")
        h = [save(5, desc(table(r)), desc(table(r[:3])))]
        j.fetch_changelog = lambda config, key: h
        out, dms, saved, posted = self.run_main({"TABLE_WATCH_ENABLED": True, "TABLE_WATCH_CHANNEL": "CCHAN"},
                                                watch_state={"last": {"TP-1": 4}, "sent": {}})
        self.assertEqual([channel for channel, _ in posted], ["UME"])
        self.assertIn("TABLE WATCH DRY RUN", posted[0][1])
        self.assertIn("Agent Barb", posted[0][1])
        self.assertIn("id3 | Pkg | 4003 | Agent Barb", posted[0][1])
        self.assertIn("Table watch (dry run): 1 ticket(s) checked", out)
        for secret in ("id3", "4003", "Agent Barb", "TP-1"):
            self.assertNotIn(secret, out)
        self.assertEqual(saved[0]["table_watch"]["last"]["TP-1"], 5)
        self.assertEqual(len(dms), 1)                      # the normal ping still went out

    def test_live_mode_posts_to_the_table_watch_channel(self):
        r = rows(4, "Agent Barb")
        j.fetch_changelog = lambda config, key: [save(5, desc(table(r)), desc(table(r[:3])))]
        out, dms, saved, posted = self.run_main({"TABLE_WATCH_ENABLED": True, "TABLE_WATCH_DRY_RUN": False, "TABLE_WATCH_CHANNEL": "CCHAN"},
                                                watch_state={"last": {"TP-1": 4}, "sent": {}})
        self.assertEqual([channel for channel, _ in posted], ["CCHAN"])
        self.assertTrue(posted[0][1].startswith("<@UBARB> id3"))
        self.assertIn("Table watch (live)", out)

    def test_error_in_table_watch_never_stops_the_pings(self):
        out, dms, saved, posted = self.run_main({"TABLE_WATCH_ENABLED": True}, tw_raises=True)
        self.assertEqual(len(dms), 1)
        self.assertIn("Table watch error (ValueError)", out)
        self.assertNotIn("boom", out)


if __name__ == "__main__":
    unittest.main()
