"""Build the Slack message text. Plain mrkdwn text, no Block Kit, no em dashes."""
import re
import unicodedata

from .parse import agent_name

ZENDESK_BASE = "https://thryv.zendesk.com"


def norm_name(name):
    """Lowercase, accents removed, spaces collapsed, so 'Naomi Jiminián' and 'naomi jiminian' match."""
    text = unicodedata.normalize("NFKD", name or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.lower().split())


def name_map_from_mentions(mentions):
    """{normalized name: slack user id} from the SLACK_MENTIONS secret (name to ID, extra spellings allowed)."""
    return {norm_name(name): uid for name, uid in (mentions or {}).items() if uid}


def lookup_agent(name_map, agent):
    """Slack ID for the name in the Agent cell. Exact spelling first; a single word (a first name) matches when exactly one person has it."""
    key = norm_name(agent)
    if key in name_map:
        return name_map[key]
    if key and " " not in key:
        ids = {uid for name, uid in name_map.items() if name.split(" ")[0] == key}
        if len(ids) == 1:
            return ids.pop()
    elif len(key.split(" ")) == 2 and len(key.split(" ")[1]) == 1:       # "Katie C": first name plus last initial
        first, initial = key.split(" ")
        ids = {uid for name, uid in name_map.items() if " " in name and name.split(" ")[0] == first and name.split(" ")[1].startswith(initial)}
        if len(ids) == 1:
            return ids.pop()
    elif key:                                   # "Karla Cruz (US)": a known full name inside a longer cell
        ids = {uid for name, uid in name_map.items() if " " in name and re.search(r"\b" + re.escape(name) + r"\b", key)}
        if len(ids) == 1:
            return ids.pop()
    return None


def find_agent(name_map, alert):
    """(slack id or None, name text). Uses the agent cell; only when that cell is empty or the table has no agent column
    are the other cells searched for a known person."""
    if alert.agent:
        return lookup_agent(name_map, alert.agent), alert.agent
    row = alert.rows[0] if alert.rows else []
    for cell in row:
        text = agent_name(cell)
        uid = lookup_agent(name_map, text) if text else None
        if uid:
            return uid, text
    return None, ""


def _join(items):
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _ref(alert, zendesk_base):
    if re.fullmatch(r"\d+", alert.ticket):
        return f"{alert.thryv_id} (ZD <{zendesk_base}/{alert.ticket}|{alert.ticket}>)"
    return alert.thryv_id


def _where(alert, jira_base):
    place = f"<{jira_base}/browse/{alert.issue}|{alert.issue}>"
    if alert.table_count > 1:
        place += f" (table {alert.table_no})"
    return place


def _rows_block(rows):
    return "```\n" + "\n".join(" | ".join(r) for r in rows) + "\n```"


def build_messages(alerts, name_map, jira_base, zendesk_base=ZENDESK_BASE):
    """Return (messages, unmapped_names). One message per save, table and rep; bulk removals go untagged."""
    messages = []
    unmapped = []
    groups = {}
    for a in alerts:
        if a.kind == "bulk":
            text = (f"{len(a.rows)} of {a.table_size} rows were removed from {_where(a, jira_base)} "
                    f"in one save. Please check the history and re-add anything that should stay.\n"
                    + _rows_block(a.rows))
            messages.append({"history_id": a.history_id, "user_id": None, "agent": "", "kind": "bulk", "text": text, "alerts": [a]})
        elif a.kind == "resolved_bulk":
            text = (f"{len(a.rows)} rows were marked resolved in {_where(a, jira_base)} in one save. Please check them.\n"
                    + _rows_block(a.rows))
            messages.append({"history_id": a.history_id, "user_id": None, "agent": "", "kind": "resolved_bulk", "text": text, "alerts": [a]})
        else:
            uid, who = find_agent(name_map, a)
            if a.kind == "resolved" and uid and lookup_agent(name_map, a.author) == uid:
                continue                                 # the agent marked their own row resolved: they already know
            groups.setdefault((a.kind, a.history_id, a.table_no, uid or norm_name(who)), []).append((a, uid, who))
    for (kind, history_id, _, _), group in groups.items():
        alerts = [a for a, _, _ in group]
        first, uid, who = alerts[0], group[0][1], group[0][2]
        if who and not uid and who not in unmapped:
            unmapped.append(who)
        tag = f"<@{uid}> " if uid else ""
        counted = {}
        for a in alerts:
            counted[_ref(a, zendesk_base)] = counted.get(_ref(a, zendesk_base), 0) + 1
        refs = _join([ref if n == 1 else f"{ref} ({n} rows)" for ref, n in counted.items()])
        if kind == "resolved":
            verb = "was" if len(counted) == 1 and len(alerts) == 1 else "were"
            text = (f":white_check_mark: {tag}{refs} {verb} marked resolved in {_where(first, jira_base)}. Please check.\n"
                    + _rows_block([a.rows[0] for a in alerts]))
        else:
            text = (f"{tag}{refs} may have been dropped from {_where(first, jira_base)}. "
                    f"Please review and re-add if applicable.\n" + _rows_block([a.rows[0] for a in alerts]))
        messages.append({"history_id": history_id, "user_id": uid, "agent": who, "kind": kind, "text": text, "alerts": alerts})
    return messages, unmapped
