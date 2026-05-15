"""
Enrichment stage (SR-P-10, SR-P-11, SR-P-12, SR-P-13).

For each PlaceRecord:
- Resolves the Wikimedia Commons image URL to a sized thumbnail
- Fetches the Wikipedia English summary text
- Per-record failures are logged and skipped — they never abort the pipeline
"""

import logging
import re
import time
import urllib.parse

import requests

from pipeline.models import PlaceRecord

log = logging.getLogger(__name__)

_WIKIPEDIA_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
_COMMONS_THUMB_WIDTH = 800
_SESSION_TIMEOUT = 10
_REQUEST_DELAY_S = 0.2   # Be polite to public APIs


def enrich(records: list[PlaceRecord], config: dict) -> list[PlaceRecord]:
    session = requests.Session()
    session.headers["User-Agent"] = "histomap-pipeline/0.1 (https://github.com/private)"

    for i, record in enumerate(records):
        if i % 100 == 0:
            log.info("  Enriching record %d / %d", i, len(records))

        if record.image_url:
            record.image_url = _resolve_thumbnail(record.image_url)

        if record.wikipedia_url and not record.description:
            record.description = _fetch_wikipedia_summary(record.wikipedia_url, session)

        time.sleep(_REQUEST_DELAY_S)

    return records


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
    """Fetch the English page summary from the Wikipedia REST API (SR-P-10)."""
    try:
        title = wikipedia_url.rstrip("/").rsplit("/wiki/", 1)[-1]
        url = _WIKIPEDIA_SUMMARY_URL.format(title=urllib.parse.quote(title, safe=""))
        resp = session.get(url, timeout=_SESSION_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("extract") or None
        log.debug("Wikipedia summary HTTP %d for %s", resp.status_code, wikipedia_url)
    except Exception as exc:
        log.debug("Wikipedia summary failed for %s: %s", wikipedia_url, exc)
    return None
