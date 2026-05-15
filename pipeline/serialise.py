"""
GeoJSON serialisation (SR-P-14, SR-P-15, SR-P-16, SR-P-17).

Writes a GeoJSON FeatureCollection to disk.
Schema version is embedded at the FeatureCollection level.
"""

import json
from pathlib import Path

from pipeline.models import PlaceRecord

SCHEMA_VERSION = "1.0"


def to_geojson(records: list[PlaceRecord], path: Path) -> None:
    features = [_to_feature(r) for r in records]
    collection = {
        "type": "FeatureCollection",
        "schema_version": SCHEMA_VERSION,
        "features": features,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(collection, f, ensure_ascii=False)


def _to_feature(r: PlaceRecord) -> dict:
    props: dict = {
        "id": r.id,
        "name": r.name,
        "category": r.category,
        "events": r.events,
    }
    if r.inception is not None:
        props["inception"] = r.inception
    if r.description:
        props["description"] = r.description
    if r.image_url:
        props["image"] = r.image_url
    if r.wikipedia_url:
        props["wikipedia"] = r.wikipedia_url

    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [r.lon, r.lat],
        },
        "properties": props,
    }
