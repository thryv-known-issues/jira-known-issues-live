"""Parse Jira wiki-markup tables out of a description."""
import re
from dataclasses import dataclass

LINK = re.compile(r"\[([^\]|]*)\|[^\]]*\]")
COLOR = re.compile(r"\{color[^}]*\}")
WRAP_MARKUP = "*+_-~^"

ID_HEADERS = ("thryv id", "location id", "account id")
TICKET_HEADERS = ("zendesk ticket", "zd ticket", "ticket #", "ticket")
AGENT_HEADERS = ("tech support agent", "agent", "tech agent")
RESOLVED_NO = ("n", "no", "false", "-", "0")
TICKET_NUMBER = re.compile(r"\d{5,}")
PAREN = re.compile(r"\([^)]*\)")
NAME_JUNK = {"-", "+", "/", "#", ":", "|", "&", "and"}


def agent_name(cell):
    """The person's name from an agent cell: drops ticket numbers, links text, dashes and notes in brackets.
    "Karla Cruz - 4883721" -> "Karla Cruz", "Barbara 4842994" -> "Barbara"."""
    text = PAREN.sub(" ", cell or "")
    words = [w for w in text.replace(",", " ").split() if w.lower() not in NAME_JUNK and not any(ch.isdigit() for ch in w)]
    return " ".join(words)


@dataclass
class Table:
    header: list
    rows: list

    @property
    def header_key(self):
        return tuple(h.lower() for h in self.header)

    def col(self, names):
        for i, h in enumerate(self.header):
            if h.lower() in names:
                return i
        return None

    def col_like(self, names, contains=None, starts=None):
        """Column whose header is one of `names`, else one that contains / starts with the given word."""
        i = self.col(names)
        if i is not None:
            return i
        for i, h in enumerate(self.header):
            low = h.lower()
            if (contains and contains in low) or (starts and low.startswith(starts)):
                return i
        return None

    @property
    def id_col(self):
        i = self.col_like(ID_HEADERS, starts="thryv id")           # "Thryv IDs" counts too
        return 0 if i is None else i

    @property
    def ticket_col(self):
        return self.col_like(TICKET_HEADERS, contains="ticket")     # "Agent + ZD Ticket" holds the ticket number too

    @property
    def agent_col(self):
        return self.col_like(AGENT_HEADERS, contains="agent")       # "Tech Agent + Ticket", "Agent + ZD Ticket"

    def ticket_value(self, row):
        """The ticket number in the ticket cell (a note or name next to it is ignored), else the cell text."""
        cell = self.cell(row, self.ticket_col)
        found = TICKET_NUMBER.search(cell)
        return found.group() if found else cell.strip()

    def agent(self, row):
        return agent_name(self.cell(row, self.agent_col))

    @property
    def resolved_col(self):
        for i, h in enumerate(self.header):
            if h.lower().startswith("resolved"):      # Resolved, Resolved?, Resolved Y/N?, Resolved date
                return i
        return None

    def is_resolved(self, row):
        """True when the Resolved column holds anything other than an empty or a "no" value."""
        value = self.cell(row, self.resolved_col).strip().lower() if self.resolved_col is not None else ""
        return bool(value) and value not in RESOLVED_NO

    def cell(self, row, col):
        if col is None or col >= len(row):
            return ""
        return row[col]

    def thryv_id(self, row):
        return self.cell(row, self.id_col).lower()

    def key(self, row):
        """Row key: (Thryv ID lowercased, ticket number); Thryv ID alone without a ticket column."""
        if self.ticket_col is None:
            return (self.thryv_id(row),)
        return (self.thryv_id(row), self.ticket_value(row))


def clean_cell(cell):
    """Remove wiki formatting people add around a value (bold, underline, color, date macro), so formatting alone never changes a row."""
    cell = COLOR.sub("", cell).replace("{{", "").replace("}}", "").strip()
    while len(cell) > 2 and cell[0] == cell[-1] and cell[0] in WRAP_MARKUP:   # *bold*, +underline+, _italic_ ... but not a lone "Signature+"
        cell = cell[1:-1].strip()
    return cell


def _cells(text, sep):
    text = LINK.sub(r"\1", text)
    return [clean_cell(c) for c in text.split(sep)]


def parse_tables(text):
    """Return the tables in `text`, in document order.

    A row can span several lines when a cell holds a line break, so a row is read until the line that ends with "|"."""
    tables = []
    current = None
    pending = None          # lines of a row that has not been closed yet
    kind = None             # "||" header or "|" row

    def close():
        nonlocal pending, current
        text_ = " ".join(part.strip() for part in pending)
        pending = None
        if kind == "||":
            header = [c.replace("*", "").strip() for c in _cells(text_.strip("|"), "||")]
            current = Table(header, [])
            tables.append(current)
        elif current is not None:
            body = text_[1:-1] if text_.endswith("|") else text_[1:]
            current.rows.append(_cells(body, "|"))

    for line in (text or "").splitlines():
        line = line.strip()
        if pending is not None:
            if line.startswith("|"):            # a new row began before the old one was closed: keep what we have
                close()
            else:
                pending.append(line)
                if line.endswith("|"):
                    close()
                continue
        if line.startswith("||"):
            kind, pending = "||", [line]
        elif line.startswith("|") and current is not None:
            kind, pending = "|", [line]
        else:
            current = None
            continue
        if line.endswith("|") and (kind == "||" or len(line) > 1):
            close()
    if pending is not None:
        close()
    return tables
