"""
Tests for the source adapter utilities (no network calls).
"""

import pytest

from pipeline.source.wikidata import (
    _parse_coords,
    _parse_inception,
    _scope_filter,
    _build_query,
)


class TestParseCoords:
    def test_standard_point(self):
        lon, lat = _parse_coords("Point(2.3522 48.8566)")
        assert lon == pytest.approx(2.3522)
        assert lat == pytest.approx(48.8566)

    def test_negative_coords(self):
        lon, lat = _parse_coords("Point(-3.7038 40.4168)")
        assert lon == pytest.approx(-3.7038)
        assert lat == pytest.approx(40.4168)

    def test_invalid_returns_none(self):
        assert _parse_coords("invalid") is None

    def test_empty_returns_none(self):
        assert _parse_coords("") is None


class TestParseInception:
    def test_ad_date(self):
        assert _parse_inception("+1066-01-01T00:00:00Z") == 1066

    def test_bc_date(self):
        assert _parse_inception("-0044-01-01T00:00:00Z") == -44

    def test_none_input(self):
        assert _parse_inception(None) is None

    def test_malformed_returns_none(self):
        assert _parse_inception("not-a-date") is None


class TestScopeFilter:
    def test_empty_scope_returns_empty_string(self):
        assert _scope_filter({}) == ""

    def test_bounding_box_preferred(self):
        result = _scope_filter({"bounding_box": {"min_lat": 27, "max_lat": 72, "min_lon": -30, "max_lon": 45}})
        assert "geof:latitude" in result
        assert "geof:longitude" in result
        assert "27" in result
        assert "72" in result

    def test_country_qids_fallback(self):
        result = _scope_filter({"country_qids": ["Q142", "Q145"]})
        assert "wd:Q142" in result
        assert "wd:Q145" in result
        assert "wdt:P17" in result


class TestBuildQuery:
    def test_qids_in_query(self):
        query = _build_query(["Q23413", "Q1145776"], "")
        assert "wd:Q23413" in query
        assert "wd:Q1145776" in query

    def test_direct_instance_of_present(self):
        query = _build_query(["Q23413"], "")
        assert "wdt:P31" in query

    def test_timeout_hint_present(self):
        query = _build_query(["Q23413"], "")
        assert "#TIMEOUT" in query

    def test_coordinates_filter_present(self):
        query = _build_query(["Q23413"], "")
        assert "wdt:P625" in query
