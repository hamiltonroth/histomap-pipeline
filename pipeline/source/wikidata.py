"""
Wikidata source adapter (SR-P-01 to SR-P-06, SR-P-07).

Responsibilities:
- Build and execute one SPARQL query per category
- Translate raw Wikidata results into PlaceRecord objects
- Deduplicate by QID
- Apply geographic scope filter

Nothing below this module knows about SPARQL or Wikidata QIDs.
"""

import logging
import time
from urllib.parse import urlparse

from SPARQLWrapper import SPARQLWrapper, JSON

from pipeline.models import PlaceRecord

log = logging.getLogger(__name__)

# Endpoints tried in order. QLever is a community-hosted Wikidata SPARQL mirror
# that runs on different infrastructure and is not subject to WDQS outage throttle rules.
_SPARQL_ENDPOINTS = [
    ("QLever", "https://qlever.cs.uni-freiburg.de/api/wikidata"),
    ("WDQS",   "https://query.wikidata.org/sparql"),
]
_USER_AGENT = "histomap-pipeline/0.1 (mailto:rothhamilton@gmail.com)"
_RETRY_ATTEMPTS = 3
_RETRY_DELAY_S = 10
_RATE_LIMIT_DELAY_S = 65

# Explicit PREFIX declarations — required by some endpoints (e.g. QLever) that
# do not inject Wikidata prefixes automatically.
_QUERY_PREFIXES = """\
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


def _build_query(qids: list[str], scope_filter: str) -> str:
    qid_values = " ".join(f"wd:{q}" for q in qids)
    return f"""{_QUERY_PREFIXES}
SELECT DISTINCT ?place ?placeLabel ?coords ?desc ?image ?inception ?wpArticle WHERE {{
  VALUES ?type {{ {qid_values} }}
  ?place wdt:P31 ?type .
  ?place wdt:P625 ?coords .
  {scope_filter}
  OPTIONAL {{ ?place rdfs:label ?placeLabel . FILTER(LANG(?placeLabel) = "en") }}
  OPTIONAL {{ ?place wdt:P571 ?inception }}
  OPTIONAL {{ ?place wdt:P18 ?image }}
  OPTIONAL {{
    ?place schema:description ?desc .
    FILTER(LANG(?desc) = "en")
  }}
  OPTIONAL {{
    ?wpArticle schema:about ?place ;
               schema:isPartOf <https://en.wikipedia.org/> .
  }}
}}
"""


def _scope_filter(scope_config: dict) -> str:
    """
    Build a SPARQL filter clause from the scope config.
    Uses a VALUES + wdt:P17 country join — portable across WDQS and QLever.
    QLever handles this join efficiently; WDQS may be slower on large categories.
    """
    country_qids = scope_config.get("country_qids", [])
    if not country_qids:
        return ""
    values = " ".join(f"wd:{q}" for q in country_qids)
    return f"VALUES ?country {{ {values} }}\n  ?place wdt:P17 ?country ."


def _parse_coords(coords_str: str) -> tuple[float, float] | None:
    """Parse WKT Point literal. Handles both 'Point(lon lat)' (WDQS) and 'POINT(lon lat)' (QLever)."""
    try:
        upper = coords_str.strip().upper()
        if not upper.startswith("POINT(") or not upper.endswith(")"):
            return None
        inner = coords_str.strip()[6:-1]  # slice using original case to preserve numeric precision
        lon_str, lat_str = inner.split()
        return float(lon_str), float(lat_str)
    except Exception:
        return None


def _parse_inception(value: str | None) -> int | None:
    if not value:
        return None
    try:
        # Wikidata returns ISO8601: "+1066-01-01T00:00:00Z" or "-0044-..."
        raw = value.lstrip("+")
        year = int(raw.split("-")[0]) if not raw.startswith("-") else -int(raw.lstrip("-").split("-")[0])
        return year
    except Exception:
        return None


def _wikipedia_title_from_url(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return urlparse(url).path.lstrip("/wiki/")
    except Exception:
        return None


class WikidataAdapter:
    def __init__(self, config: dict) -> None:
        self._config = config
        self._clients: list[tuple[str, SPARQLWrapper]] = []
        for name, url in _SPARQL_ENDPOINTS:
            client = SPARQLWrapper(url)
            client.addCustomHttpHeader("User-Agent", _USER_AGENT)
            client.setReturnFormat(JSON)
            client.setTimeout(60)
            self._clients.append((name, client))

    def fetch_all(self) -> list[PlaceRecord]:
        scope = self._config.get("geographic_scope", {})
        scope_clause = _scope_filter(scope)
        seen: dict[str, PlaceRecord] = {}
        categories = self._config["categories"]

        last_was_rate_limited = False
        for idx, cat in enumerate(categories):
            # Only delay if the previous query was 429-throttled
            if last_was_rate_limited and idx > 0:
                log.info("  Previous query was rate-limited — waiting %ds before next", _RATE_LIMIT_DELAY_S)
                time.sleep(_RATE_LIMIT_DELAY_S)

            key = cat["key"]
            qids = cat["qids"]
            log.info("  Querying category: %s (%d QIDs)", key, len(qids))
            results, last_was_rate_limited = self._run_query(key, qids, scope_clause)
            new = 0
            for r in results:
                if r.id not in seen:
                    seen[r.id] = r
                    new += 1
            log.info("  → %d new records (total so far: %d)", new, len(seen))

        return list(seen.values())

    def _run_query(self, category: str, qids: list[str], scope_clause: str) -> tuple[list[PlaceRecord], bool]:
        """Returns (records, was_rate_limited)."""
        records, was_rate_limited = self._run_query_once(category, qids, scope_clause)
        if records:
            return records, was_rate_limited

        if len(qids) == 1:
            log.error("All attempts failed for category %s — skipping", category)
            return [], was_rate_limited

        log.warning("Category %s failed as a combined query — retrying per QID", category)
        fallback_records: list[PlaceRecord] = []
        for qid in qids:
            qid_records, qid_rate_limited = self._run_query_once(category, [qid], scope_clause)
            fallback_records.extend(qid_records)
            was_rate_limited = was_rate_limited or qid_rate_limited

        if fallback_records:
            log.info("Recovered %d records for %s via per-QID fallback", len(fallback_records), category)
            return fallback_records, was_rate_limited

        log.error("All attempts failed for category %s — skipping", category)
        return [], was_rate_limited

    def _run_query_once(self, category: str, qids: list[str], scope_clause: str) -> tuple[list[PlaceRecord], bool]:
        """Try each endpoint in order per attempt. Returns on first success."""
        query = _build_query(qids, scope_clause)
        was_rate_limited = False
        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            for ep_name, client in self._clients:
                try:
                    client.setQuery(query)
                    raw = client.queryAndConvert()
                    if ep_name != _SPARQL_ENDPOINTS[0][0]:
                        log.info("    Succeeded via fallback endpoint %s", ep_name)
                    return self._parse_results(raw, category), was_rate_limited
                except Exception as exc:
                    if self._is_rate_limited(exc):
                        was_rate_limited = True
                    log.debug("Endpoint %s attempt %d/%d failed for %s: %s",
                               ep_name, attempt, _RETRY_ATTEMPTS, category, exc)
            log.warning("Query attempt %d/%d: all endpoints failed for %s",
                        attempt, _RETRY_ATTEMPTS, category)
            if attempt < _RETRY_ATTEMPTS:
                time.sleep(_RETRY_DELAY_S)
        return [], was_rate_limited

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "429" in msg or "rate-limit" in msg or "rate limit" in msg or "rate-limiting" in msg

    @staticmethod
    def _retry_delay_seconds(exc: Exception) -> int:
        msg = str(exc).lower()
        if "429" in msg or "rate-limit" in msg or "rate limit" in msg or "rate-limiting" in msg:
            return _RATE_LIMIT_DELAY_S
        return _RETRY_DELAY_S

    def _parse_results(self, raw: dict, category: str) -> list[PlaceRecord]:
        records = []
        for binding in raw.get("results", {}).get("bindings", []):
            qid = binding["place"]["value"].rsplit("/", 1)[-1]
            coords_raw = binding.get("coords", {}).get("value")
            if not coords_raw:
                continue
            parsed = _parse_coords(coords_raw)
            if parsed is None:
                continue
            lon, lat = parsed

            records.append(PlaceRecord(
                id=qid,
                name=binding.get("placeLabel", {}).get("value", ""),
                category=category,
                lon=lon,
                lat=lat,
                inception=_parse_inception(binding.get("inception", {}).get("value")),
                image_url=binding.get("image", {}).get("value") or None,
                description=binding.get("desc", {}).get("value") or None,
                wikipedia_url=binding.get("wpArticle", {}).get("value") or None,
            ))
        return records
