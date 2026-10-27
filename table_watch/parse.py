"""Parse Jira wiki-markup tables out of a description."""
import re
from dataclasses import dataclass

LINK = re.compile(r"\[([^\]|]*)\|[^\]]*\]")

ID_HEADERS = ("thryv id", "location id", "account id")
TICKET_HEADERS = ("zendesk ticket", "zd ticket", "ticket #", "ticket")
AGENT_HEADERS = ("tech support agent", "agent", "tech agent")
RESOLVED_HEADERS = ("resolved", "resolved?", "resolved date")


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

    @property
    def id_col(self):
        i = self.col(ID_HEADERS)
        return 0 if i is None else i

    @property
    def ticket_col(self):
        return self.col(TICKET_HEADERS)

    @property
    def agent_col(self):
        return self.col(AGENT_HEADERS)

    @property
    def resolved_col(self):
        return self.col(RESOLVED_HEADERS)

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
        return (self.thryv_id(row), self.cell(row, self.ticket_col))


def _cells(text, sep):
    text = LINK.sub(r"\1", text)
    return [c.strip() for c in text.split(sep)]


def parse_tables(text):
    """Return the tables in `text`, in document order."""
    tables = []
    current = None
    for line in (text or "").splitlines():
        line = line.strip()
        if line.startswith("||"):
            header = [c.replace("*", "").strip() for c in _cells(line.strip("|"), "||")]
            current = Table(header, [])
            tables.append(current)
        elif line.startswith("|") and current is not None:
            body = line[1:-1] if line.endswith("|") else line[1:]
            current.rows.append(_cells(body, "|"))
        else:
            current = None
    return tables
