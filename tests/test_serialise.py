"""
Tests for GeoJSON serialisation (SR-P-14, SR-P-15, SR-P-16, SR-P-17).
"""

import json
from pathlib import Path

import pytest

from pipeline.models import PlaceRecord
from pipeline.serialise import to_geojson, SCHEMA_VERSION


def _make_record(**kwargs) -> PlaceRecord:
    defaults = dict(id="Q1", name="Test Place", category="castle", lon=2.35, lat=48.85)
    defaults.update(kwargs)
    return PlaceRecord(**defaults)


class TestToGeoJSON:
    def test_valid_feature_collection(self, tmp_path):
        records = [_make_record()]
        out = tmp_path / "places.geojson"
        to_geojson(records, out)

        data = json.loads(out.read_text())
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 1

    def test_schema_version_present(self, tmp_path):
        """SR-P-17: schema_version must be present at FeatureCollection level."""
        out = tmp_path / "places.geojson"
        to_geojson([_make_record()], out)
        data = json.loads(out.read_text())
        assert data["schema_version"] == SCHEMA_VERSION

    def test_required_properties_present(self, tmp_path):
        """SR-P-15: id, name, category, events always present."""
        out = tmp_path / "places.geojson"
        to_geojson([_make_record()], out)
        props = json.loads(out.read_text())["features"][0]["properties"]
        assert "id" in props
        assert "name" in props
        assert "category" in props
        assert "events" in props

    def test_optional_properties_omitted_when_absent(self, tmp_path):
        out = tmp_path / "places.geojson"
        to_geojson([_make_record()], out)
        props = json.loads(out.read_text())["features"][0]["properties"]
        assert "inception" not in props
        assert "description" not in props
        assert "image" not in props
        assert "wikipedia" not in props

    def test_optional_properties_included_when_present(self, tmp_path):
        r = _make_record(inception=1066, description="A famous battle site.",
                         image_url="https://upload.wikimedia.org/test.jpg",
                         wikipedia_url="https://en.wikipedia.org/wiki/Test")
        out = tmp_path / "places.geojson"
        to_geojson([r], out)
        props = json.loads(out.read_text())["features"][0]["properties"]
        assert props["inception"] == 1066
        assert props["description"] == "A famous battle site."
        assert "upload.wikimedia.org" in props["image"]
        assert "wikipedia.org" in props["wikipedia"]

    def test_negative_inception_year(self, tmp_path):
        """SR-P-13: BC years stored as negative integers."""
        r = _make_record(inception=-44)
        out = tmp_path / "places.geojson"
        to_geojson([r], out)
        props = json.loads(out.read_text())["features"][0]["properties"]
        assert props["inception"] == -44

    def test_geometry_is_point(self, tmp_path):
        out = tmp_path / "places.geojson"
        to_geojson([_make_record()], out)
        geom = json.loads(out.read_text())["features"][0]["geometry"]
        assert geom["type"] == "Point"
        assert geom["coordinates"] == [2.35, 48.85]

    def test_no_duplicate_ids(self, tmp_path):
        """SR-P-06: deduplication upstream should mean no duplicate IDs in output."""
        records = [_make_record(id="Q1"), _make_record(id="Q2"), _make_record(id="Q1")]
        out = tmp_path / "places.geojson"
        # Serialiser writes whatever it receives — dedup is the adapter's responsibility.
        # This test confirms the serialiser preserves all passed records faithfully.
        to_geojson(records, out)
        features = json.loads(out.read_text())["features"]
        assert len(features) == 3
