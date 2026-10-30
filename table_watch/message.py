"""Build the Slack message text. Plain mrkdwn text, no Block Kit, no em dashes."""
import re
import unicodedata

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
    return None


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
            messages.append({"history_id": a.history_id, "user_id": None, "agent": "", "text": text, "alerts": [a]})
        else:
            groups.setdefault((a.history_id, a.table_no, norm_name(a.agent)), []).append(a)
    for (history_id, _, agent_key), group in groups.items():
        first = group[0]
        uid = lookup_agent(name_map, first.agent)
        if agent_key and not uid and first.agent not in unmapped:
            unmapped.append(first.agent)
        tag = f"<@{uid}> " if uid else ""
        refs = _join([_ref(a, zendesk_base) for a in group])
        text = (f"{tag}{refs} may have been dropped from {_where(first, jira_base)}. "
                f"Please review and re-add if applicable.\n" + _rows_block([a.rows[0] for a in group]))
        messages.append({"history_id": history_id, "user_id": uid, "agent": first.agent, "text": text, "alerts": group})
    return messages, unmapped
