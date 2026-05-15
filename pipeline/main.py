"""
Build pipeline entry point.

Execution order:
  1. Load config
  2. Run Wikidata source adapter  → list[PlaceRecord]
  3. Enrich records               → list[PlaceRecord]
  4. Serialise to GeoJSON
  5. Run tippecanoe               → .pmtiles
  6. Upload to Cloudflare R2
"""

import logging
import sys
from pathlib import Path

from pipeline.config import load_config
from pipeline.source.wikidata import WikidataAdapter
from pipeline.enrichment import enrich
from pipeline.serialise import to_geojson
from pipeline.tiles import build_tiles
from pipeline.upload import upload_to_r2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


def main() -> None:
    config = load_config()
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Step 1/5 — querying Wikidata")
    adapter = WikidataAdapter(config)
    records = adapter.fetch_all()
    log.info("Fetched %d unique place records", len(records))

    log.info("Step 2/5 — enriching records")
    records = enrich(records, config)
    log.info("Enrichment complete")

    log.info("Step 3/5 — serialising to GeoJSON")
    geojson_path = output_dir / "places.geojson"
    to_geojson(records, geojson_path)
    log.info("Written %s", geojson_path)

    log.info("Step 4/5 — building PMTiles")
    pmtiles_path = output_dir / "places.pmtiles"
    build_tiles(geojson_path, pmtiles_path, config)
    log.info("Written %s", pmtiles_path)

    log.info("Step 5/5 — uploading to Cloudflare R2")
    upload_to_r2(pmtiles_path, config)
    log.info("Upload complete — pipeline finished")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Pipeline failed")
        sys.exit(1)
