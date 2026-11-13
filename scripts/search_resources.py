#!/usr/bin/env python3
"""Search the reviewed source catalogue locally, without a service or API key."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def search(catalog, query='', unit=None):
    tokens = query.casefold().split()
    found = []
    for kind in ('courses', 'sources', 'units'):
        for item in catalog[kind]:
            if unit and (kind != 'units' or item['id'] != unit):
                continue
            text = json.dumps(item, ensure_ascii=False).casefold()
            if all(token in text for token in tokens):
                found.append({'kind': kind, **item})
    return found


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('query', nargs='?', default='')
    p.add_argument('--unit')
    a = p.parse_args()
    catalog = json.loads((ROOT / 'resources/catalog.json').read_text())
    print(json.dumps(search(catalog, a.query, a.unit), ensure_ascii=False, indent=2))
