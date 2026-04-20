#!/usr/bin/env python3
"""
parse_descriptions.py
Reads data.json, processes each issue's ADF description to extract:
- brief: first meaningful sentence from the description
- affectedAccounts: count of data rows in Affected Accounts tables
Writes updated data.json back (with rawDescription removed to keep it lean).
"""
import json, sys, re

def extract_text_from_adf(node):
    """Recursively extract plain text from an ADF node."""
    if not node or not isinstance(node, dict):
        return ''
    if node.get('type') == 'text':
        return node.get('text', '')
    parts = []
    for child in node.get('content', []):
        parts.append(extract_text_from_adf(child))
    return ''.join(parts)

def get_brief(desc):
    """Extract first meaningful paragraph from ADF description."""
    if not desc or not isinstance(desc, dict):
        return ''
    for node in desc.get('content', []):
        if node.get('type') == 'paragraph':
            text = extract_text_from_adf(node).strip()
            # Skip short lines, known section headers, bullet-style lines
            if len(text) < 15:
                continue
            skip_prefixes = (
                'Desired Result', 'Actual Result', 'Steps to Reproduce',
                'Affected Account', 'Account info', 'Validation completed',
                'Account Name', 'Website:', 'Thryv ID:', 'Billing Plan:',
                'EAID:', 'SEO Proposal', 'Zendesk Ticket', 'Business name:',
                'ZD ticket:', 'Sponsored:'
            )
            if any(text.startswith(p) for p in skip_prefixes):
                continue
            # Remove "Description" or "Description of Issue" prefix
            text = re.sub(r'^Description\s*(of\s*Issue)?\s*', '', text, flags=re.I).strip()
            if len(text) < 15:
                continue
            if len(text) > 200:
                text = text[:200].rsplit(' ', 1)[0] + '...'
            return text
    return ''

def count_table_rows(desc):
    """Count data rows in tables (excluding header rows)."""
    if not desc or not isinstance(desc, dict):
        return 0
    count = 0
    for node in desc.get('content', []):
        if node.get('type') == 'table':
            rows = node.get('content', [])
            # First row is typically the header; count the rest
            data_rows = [r for r in rows if r.get('type') == 'tableRow']
            if len(data_rows) > 1:
                count += len(data_rows) - 1
    return count

def process(data):
    for issue in data.get('issues', []):
        desc = issue.get('rawDescription', None)
        issue['brief'] = get_brief(desc)
        issue['affectedAccounts'] = count_table_rows(desc)
        # Remove raw ADF to keep data.json small
        issue.pop('rawDescription', None)
    return data

if __name__ == '__main__':
    data = json.load(sys.stdin)
    result = process(data)
    json.dump(result, sys.stdout)
