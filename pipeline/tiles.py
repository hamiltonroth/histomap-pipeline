"""
Tile generation via tippecanoe (SR-P-18, SR-P-19, SR-P-20, SR-P-21).

tippecanoe must be installed and on PATH in the pipeline environment.
"""

import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

_MIN_ZOOM = 2
_MAX_ZOOM = 16


def build_tiles(geojson_path: Path, pmtiles_path: Path, config: dict) -> None:
    pmtiles_path.unlink(missing_ok=True)

    cmd = [
        "tippecanoe",
        f"--minimum-zoom={_MIN_ZOOM}",
        f"--maximum-zoom={_MAX_ZOOM}",
        "--output", str(pmtiles_path),
        # Retain the most-named features at low zoom rather than dropping uniformly (SR-P-20)
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        # Use the category property as the layer name so MapLibre can filter by it
        "--layer=places",
        # Preserve all properties
        "--include=id",
        "--include=name",
        "--include=category",
        "--include=inception",
        "--include=description",
        "--include=image",
        "--include=wikipedia",
        "--include=events",
        "--force",
        str(geojson_path),
    ]

    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.error("tippecanoe stderr:\n%s", result.stderr)
        raise RuntimeError(f"tippecanoe exited with code {result.returncode}")

    log.info("tippecanoe stdout:\n%s", result.stdout)
    size_mb = pmtiles_path.stat().st_size / 1_048_576
    log.info("PMTiles file size: %.1f MB", size_mb)
    if size_mb > 500:
        log.warning("PMTiles file exceeds 500 MB limit (SR-P-21): %.1f MB", size_mb)
