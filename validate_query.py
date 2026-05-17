"""
Quick validation script — run a live SPARQL query for one or all categories
without executing the full pipeline.

Usage:
    python validate_query.py                              # test all categories
    python validate_query.py castle                       # test one category by key
    python validate_query.py castle battlefield           # test several
    python validate_query.py castle --qid Q23413         # override category QIDs
    python validate_query.py castle --limit 25 --timeout 20

Prints row count per category and a small result sample.
"""

import argparse
import logging
import os
import time
from pathlib import Path

import yaml
from SPARQLWrapper import JSON, SPARQLWrapper

from pipeline.source.wikidata import _build_query, _scope_filter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

_CONFIG_DIR = Path(__file__).parent / "config"


def load_query_config() -> dict:
    with (_CONFIG_DIR / "pipeline.yml").open(encoding="utf-8") as f:
        config = yaml.safe_load(f)

    with (_CONFIG_DIR / "categories.yml").open(encoding="utf-8") as f:
        config["categories"] = yaml.safe_load(f)["categories"]

    return config


def with_limit(query: str, limit: int) -> str:
    if limit <= 0:
        return query
    return f"{query.rstrip()}\nLIMIT {limit}\n"


parser = argparse.ArgumentParser()
parser.add_argument("categories", nargs="*")
parser.add_argument("--qid", dest="qids", action="append", default=[])
parser.add_argument("--limit", type=int, default=3)
parser.add_argument("--timeout", type=int, default=25)
parser.add_argument("--endpoint",
                    default=os.environ.get("SPARQL_PROXY_URL", "").strip() or "https://query.wikidata.org/sparql",
                    help="SPARQL endpoint URL (defaults to SPARQL_PROXY_URL env var, then WDQS)")
parser.add_argument("--proxy-key",
                    default=os.environ.get("SPARQL_PROXY_KEY", "").strip(),
                    help="X-Proxy-Key header value (defaults to SPARQL_PROXY_KEY env var)")
parser.add_argument("--sleep", type=int, default=2,
                    help="Seconds to sleep between queries (default 2)")
args = parser.parse_args()

config = load_query_config()
keys_requested = args.categories or [c["key"] for c in config["categories"]]
categories = [c for c in config["categories"] if c["key"] in keys_requested]

if not categories:
    print(f"No matching categories found. Available: {[c['key'] for c in config['categories']]}")
    raise SystemExit(1)

scope_clause = _scope_filter(config.get("geographic_scope", {}))

sparql = SPARQLWrapper(args.endpoint)
sparql.addCustomHttpHeader("User-Agent", "histomap-validate/0.1 (mailto:rothhamilton@gmail.com)")
if args.proxy_key:
    sparql.addCustomHttpHeader("X-Proxy-Key", args.proxy_key)
    logging.getLogger().info("Using proxy: %s", args.endpoint)
sparql.setReturnFormat(JSON)
sparql.setTimeout(args.timeout)

for i, cat in enumerate(categories):
    if i > 0:
        print(f"  Sleeping {args.sleep}s between queries...")
        time.sleep(args.sleep)

    key = cat["key"]
    qids = args.qids or cat["qids"]
    print(f"\n=== {key} ({len(qids)} QIDs: {', '.join(qids)}) ===")
    query = with_limit(_build_query(qids, scope_clause), args.limit)

    try:
        sparql.setQuery(query)
        raw = sparql.queryAndConvert()
        rows = raw.get("results", {}).get("bindings", [])
        print(f"  Rows returned: {len(rows)}")
        for row in rows[: args.limit]:
            label = row.get("placeLabel", {}).get("value", "?")
            coords = row.get("coords", {}).get("value", "?")
            qid = row.get("place", {}).get("value", "?").rsplit("/", 1)[-1]
            print(f"    {qid}  {label}  {coords}")
    except Exception as exc:
        print(f"  FAILED: {exc}")
