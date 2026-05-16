"""
Quick validation script — run a live SPARQL query for one or all categories
without executing the full pipeline.

Usage:
    python validate_query.py                     # test all categories
    python validate_query.py castle              # test one category by key
    python validate_query.py castle battlefield  # test several

Prints row count per category and first 3 results.
"""

import sys
import time
import logging
from pipeline.config import load_config
from pipeline.source.wikidata import WikidataAdapter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

config = load_config()
adapter = WikidataAdapter(config)

scope_clause = adapter._scope_filter(config.get("geographic_scope", {}))

keys_requested = sys.argv[1:] or [c["key"] for c in config["categories"]]
categories = [c for c in config["categories"] if c["key"] in keys_requested]

if not categories:
    print(f"No matching categories found. Available: {[c['key'] for c in config['categories']]}")
    sys.exit(1)

from pipeline.source.wikidata import _build_query
from SPARQLWrapper import SPARQLWrapper, JSON

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.addCustomHttpHeader("User-Agent", "histomap-validate/0.1")
sparql.setReturnFormat(JSON)

for i, cat in enumerate(categories):
    if i > 0:
        print("  Sleeping 5s between queries...")
        time.sleep(5)
    key = cat["key"]
    qids = cat["qids"]
    print(f"\n=== {key} ({len(qids)} QIDs: {', '.join(qids)}) ===")
    query = _build_query(qids, scope_clause)
    try:
        sparql.setQuery(query)
        raw = sparql.queryAndConvert()
        rows = raw.get("results", {}).get("bindings", [])
        print(f"  Rows returned: {len(rows)}")
        for row in rows[:3]:
            label = row.get("placeLabel", {}).get("value", "?")
            coords = row.get("coords", {}).get("value", "?")
            qid = row.get("place", {}).get("value", "?").rsplit("/", 1)[-1]
            print(f"    {qid}  {label}  {coords}")
    except Exception as exc:
        print(f"  FAILED: {exc}")
