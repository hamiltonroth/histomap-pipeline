"""
Enrichment stage (SR-P-10, SR-P-11, SR-P-12, SR-P-13).

For each PlaceRecord:
- Resolves the Wikimedia Commons image URL to a sized thumbnail (pure computation)
- Batch-fetches Wikipedia English summaries for records that have a Wikipedia article
  but no Wikidata description, using the MediaWiki action API (50 titles per request)

Per-record failures are logged and skipped — they never abort the pipeline.
"""

import logging
import time
import urllib.parse

import requests

from pipeline.models import PlaceRecord

log = logging.getLogger(__name__)

_MEDIAWIKI_API = "https://en.wikipedia.org/w/api.php"
_WIKIDATA_API = "https://www.wikidata.org/w/api.php"
_COMMONS_THUMB_WIDTH = 800
_SESSION_TIMEOUT = 30
_BATCH_SIZE = 50        # MediaWiki / Wikidata API maximum IDs per request
_BATCH_DELAY_S = 1.0    # polite delay between batches (not per-record)


def enrich(records: list[PlaceRecord], config: dict) -> list[PlaceRecord]:
    # Pass 1: thumbnail resolution — pure computation, no network calls
    for record in records:
        if record.image_url:
            record.image_url = _resolve_thumbnail(record.image_url)

    session = requests.Session()
    session.headers["User-Agent"] = "histomap-pipeline/0.1 (https://github.com/private)"

    # Pass 2: batch-fetch English descriptions and Wikipedia URLs from Wikidata API.
    # This replaces the schema:description and ?wpArticle SPARQL OPTIONALs, which
    # were too slow on large result sets (5000+ rows with language-tagged lookups).
    log.info("  Fetching Wikidata properties for %d records", len(records))
    _fetch_wikidata_properties(records, session)

    # Pass 3: batch-fetch Wikipedia summaries for records that now have a Wikipedia URL.
    # Wikipedia extracts are richer than Wikidata short descriptions, so they overwrite.
    needs_wiki = [r for r in records if r.wikipedia_url]
    if needs_wiki:
        log.info("  Fetching Wikipedia summaries for %d/%d records (batches of %d)",
                 len(needs_wiki), len(records), _BATCH_SIZE)
        _batch_fetch_summaries(needs_wiki, session)

    return records


def _fetch_wikidata_properties(records: list[PlaceRecord], session: requests.Session) -> None:
    """
    Batch-fetch English descriptions and Wikipedia URLs via the Wikidata wbgetentities API.
    50 QIDs per request (API maximum). Sets record.description and record.wikipedia_url.
    Failures are per-batch and logged; they never abort the pipeline.
    """
    n_batches = (len(records) + _BATCH_SIZE - 1) // _BATCH_SIZE
    for batch_idx in range(n_batches):
        batch = records[batch_idx * _BATCH_SIZE:(batch_idx + 1) * _BATCH_SIZE]
        qid_list = "|".join(r.id for r in batch)
        try:
            resp = session.get(
                _WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "ids": qid_list,
                    "props": "descriptions|sitelinks",
                    "sitefilter": "enwiki",
                    "languages": "en",
                    "format": "json",
                },
                timeout=_SESSION_TIMEOUT,
            )
            if resp.status_code == 200:
                entities = resp.json().get("entities", {})
                for record in batch:
                    entity = entities.get(record.id, {})
                    desc = entity.get("descriptions", {}).get("en", {}).get("value")
                    if desc:
                        record.description = desc
                    title = entity.get("sitelinks", {}).get("enwiki", {}).get("title")
                    if title:
                        record.wikipedia_url = (
                            "https://en.wikipedia.org/wiki/"
                            + urllib.parse.quote(title.replace(" ", "_"), safe=":/")
                        )
            else:
                log.warning("Wikidata API HTTP %d for batch %d/%d",
                            resp.status_code, batch_idx + 1, n_batches)
        except Exception as exc:
            log.warning("Wikidata batch fetch failed (batch %d/%d): %s",
                        batch_idx + 1, n_batches, exc)
        if batch_idx < n_batches - 1:
            time.sleep(_BATCH_DELAY_S)


def _batch_fetch_summaries(records: list[PlaceRecord], session: requests.Session) -> None:
    """
    Fetch English Wikipedia extracts for a list of records using the MediaWiki action API.
    Groups up to _BATCH_SIZE titles per HTTP request (SR-P-10).
    Failures are per-batch, not per-record — a failed batch is logged and skipped.
    """
    # Map normalised title -> list of records (multiple records can share a title)
    title_map: dict[str, list[PlaceRecord]] = {}
    for record in records:
        raw = record.wikipedia_url.rstrip("/").rsplit("/wiki/", 1)[-1]
        title = urllib.parse.unquote(raw).replace("_", " ")
        title_map.setdefault(title, []).append(record)

    titles = list(title_map.keys())
    n_batches = (len(titles) + _BATCH_SIZE - 1) // _BATCH_SIZE

    for batch_idx in range(n_batches):
        batch = titles[batch_idx * _BATCH_SIZE:(batch_idx + 1) * _BATCH_SIZE]
        try:
            resp = session.get(
                _MEDIAWIKI_API,
                params={
                    "action": "query",
                    "prop": "extracts",
                    "exsentences": 3,
                    "exintro": True,
                    "explaintext": True,
                    "format": "json",
                    "titles": "|".join(batch),
                },
                timeout=_SESSION_TIMEOUT,
            )
            if resp.status_code == 200:
                pages = resp.json().get("query", {}).get("pages", {})
                for page in pages.values():
                    page_title = page.get("title", "")
                    extract = page.get("extract") or None
                    for record in title_map.get(page_title, []):
                        record.description = extract
            else:
                log.debug("Wikipedia batch HTTP %d for batch %d", resp.status_code, batch_idx)
        except Exception as exc:
            log.warning("Wikipedia batch fetch failed (batch %d/%d): %s", batch_idx + 1, n_batches, exc)

        if batch_idx < n_batches - 1:
            time.sleep(_BATCH_DELAY_S)


def _resolve_thumbnail(commons_url: str) -> str:
    """
    Convert a Wikimedia Commons file URL to a sized thumbnail URL (SR-P-11).

    Input:  https://commons.wikimedia.org/wiki/Special:FilePath/Foo.jpg
            or the raw P18 value  http://commons.wikimedia.org/wiki/Special:FilePath/Foo.jpg
    Output: https://upload.wikimedia.org/wikipedia/commons/thumb/.../Foo.jpg/800px-Foo.jpg
    """
    try:
        # Wikidata P18 gives either a Special:FilePath URL or a direct upload URL
        if "Special:FilePath/" in commons_url:
            filename = commons_url.split("Special:FilePath/", 1)[1]
        elif "upload.wikimedia.org" in commons_url:
            return commons_url  # Already a direct URL; keep as-is
        else:
            return commons_url

        filename = urllib.parse.unquote(filename)
        # Wikimedia thumbnail URL pattern
        name_encoded = urllib.parse.quote(filename.replace(" ", "_"), safe="")
        return (
            f"https://upload.wikimedia.org/wikipedia/commons/thumb/"
            f"{_md5_prefix(filename)}/{name_encoded}/{_COMMONS_THUMB_WIDTH}px-{name_encoded}"
        )
    except Exception as exc:
        log.debug("Could not resolve thumbnail for %s: %s", commons_url, exc)
        return commons_url


def _md5_prefix(filename: str) -> str:
    """Return the two-level hash prefix used by Wikimedia file paths."""
    import hashlib
    name = filename.replace(" ", "_")
    digest = hashlib.md5(name.encode()).hexdigest()
    return f"{digest[0]}/{digest[0]}{digest[1]}"


def _fetch_wikipedia_summary(wikipedia_url: str, session: requests.Session) -> str | None:
    """Single-record Wikipedia summary fetch via REST API (kept for test compatibility)."""
    try:
        title = wikipedia_url.rstrip("/").rsplit("/wiki/", 1)[-1]
        quoted = urllib.parse.quote(title, safe="")
        url = "https://en.wikipedia.org/api/rest_v1/page/summary/" + quoted
        resp = session.get(url, timeout=_SESSION_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("extract") or None
        log.debug("Wikipedia summary HTTP %d for %s", resp.status_code, wikipedia_url)
    except Exception as exc:
        log.debug("Wikipedia summary failed for %s: %s", wikipedia_url, exc)
    return None
