"""
Tests for the enrichment stage (no network calls — external calls are mocked).
"""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.enrichment import _resolve_thumbnail, _fetch_wikipedia_summary, _md5_prefix
from pipeline.models import PlaceRecord


class TestResolveThumbnail:
    def test_special_filepath_url(self):
        url = "https://commons.wikimedia.org/wiki/Special:FilePath/PragueCastle.jpg"
        result = _resolve_thumbnail(url)
        assert "upload.wikimedia.org" in result
        assert "800px" in result
        assert "PragueCastle.jpg" in result

    def test_already_upload_url_returned_as_is(self):
        url = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Foo.jpg/640px-Foo.jpg"
        assert _resolve_thumbnail(url) == url

    def test_unknown_url_returned_as_is(self):
        url = "https://example.com/image.jpg"
        assert _resolve_thumbnail(url) == url


class TestMd5Prefix:
    def test_known_value(self):
        # Wikimedia uses MD5 of the normalised filename
        prefix = _md5_prefix("PragueCastle.jpg")
        assert len(prefix) == 4   # "x/xy" format — e.g. "2/2d"
        assert "/" in prefix


class TestFetchWikipediaSummary:
    def test_returns_extract_on_success(self):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"extract": "Prague Castle is the largest ancient castle."}
        mock_session.get.return_value = mock_resp

        result = _fetch_wikipedia_summary("https://en.wikipedia.org/wiki/Prague_Castle", mock_session)
        assert result == "Prague Castle is the largest ancient castle."

    def test_returns_none_on_404(self):
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_session.get.return_value = mock_resp

        result = _fetch_wikipedia_summary("https://en.wikipedia.org/wiki/Nonexistent", mock_session)
        assert result is None

    def test_returns_none_on_exception(self):
        mock_session = MagicMock()
        mock_session.get.side_effect = ConnectionError("timeout")

        result = _fetch_wikipedia_summary("https://en.wikipedia.org/wiki/Foo", mock_session)
        assert result is None

    def test_pipeline_continues_on_failure(self):
        """SR-P-12: single-record enrichment failure must not abort the pipeline."""
        from pipeline.enrichment import enrich

        records = [
            PlaceRecord(id="Q1", name="Castle A", category="castle", lon=2.0, lat=48.0,
                        wikipedia_url="https://en.wikipedia.org/wiki/Castle_A"),
            PlaceRecord(id="Q2", name="Castle B", category="castle", lon=3.0, lat=49.0,
                        wikipedia_url="https://en.wikipedia.org/wiki/Castle_B"),
        ]

        with patch("pipeline.enrichment.requests.Session") as mock_session_cls:
            session = MagicMock()
            mock_session_cls.return_value = session

            def side_effect(url, **kwargs):
                if "Castle_A" in url:
                    raise ConnectionError("simulated failure")
                resp = MagicMock()
                resp.status_code = 200
                resp.json.return_value = {"extract": "Summary of B"}
                return resp

            session.get.side_effect = side_effect

            result = enrich(records, {})

        assert len(result) == 2
        assert result[0].description is None   # failed record — no description
        assert result[1].description == "Summary of B"
