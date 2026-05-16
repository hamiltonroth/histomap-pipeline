"""
Tests for the enrichment stage (no network calls — external calls are mocked).
"""

from unittest.mock import MagicMock, patch

import pytest

from pipeline.enrichment import _resolve_thumbnail, _fetch_wikipedia_summary, _md5_prefix, enrich
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


class TestEnrich:
    def _make_record(self, qid, name, **kwargs):
        return PlaceRecord(id=qid, name=name, category="castle", lon=2.0, lat=48.0, **kwargs)

    def test_skips_wikipedia_when_description_already_present(self):
        """Records with a Wikidata description must not trigger any HTTP call."""
        record = self._make_record("Q1", "Castle A",
                                   description="Already has a description",
                                   wikipedia_url="https://en.wikipedia.org/wiki/Castle_A")
        with patch("pipeline.enrichment.requests.Session") as mock_session_cls:
            enrich([record], {})
            mock_session_cls.assert_not_called()

        assert record.description == "Already has a description"

    def test_batch_fetch_called_for_records_without_description(self):
        """Records with a wikipedia_url but no description must be batch-enriched."""
        records = [
            self._make_record("Q1", "Castle A",
                              wikipedia_url="https://en.wikipedia.org/wiki/Castle_A"),
            self._make_record("Q2", "Castle B",
                              wikipedia_url="https://en.wikipedia.org/wiki/Castle_B"),
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "query": {
                "pages": {
                    "1": {"title": "Castle A", "extract": "Summary of A"},
                    "2": {"title": "Castle B", "extract": "Summary of B"},
                }
            }
        }
        with patch("pipeline.enrichment.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.get.return_value = mock_resp
            mock_session_cls.return_value = session
            enrich(records, {})

        assert records[0].description == "Summary of A"
        assert records[1].description == "Summary of B"

    def test_pipeline_continues_on_batch_failure(self):
        """SR-P-12: a failed batch must not abort the pipeline."""
        records = [
            self._make_record("Q1", "Castle A",
                              wikipedia_url="https://en.wikipedia.org/wiki/Castle_A"),
        ]
        with patch("pipeline.enrichment.requests.Session") as mock_session_cls:
            session = MagicMock()
            session.get.side_effect = ConnectionError("simulated failure")
            mock_session_cls.return_value = session
            result = enrich(records, {})

        assert len(result) == 1
        assert result[0].description is None  # failed batch — no description set
