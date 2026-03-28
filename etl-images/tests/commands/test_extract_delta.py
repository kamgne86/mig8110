import os
import gzip
import json
import pytest
from unittest.mock import Mock, patch
from commands.extract_delta import handle, _download_and_filter, _matches_country

BASE_URL = "https://static.openfoodfacts.org/data/delta/"
FILENAME = "openfoodfacts_products_1770673073_1772050745.json.gz"


class TestMatchesCountry:

    def test_exact_tag(self):
        assert _matches_country(["en:canada", "en:france"], "canada") is True

    def test_no_match(self):
        assert _matches_country(["en:france", "en:usa"], "canada") is False

    def test_case_insensitive(self):
        assert _matches_country(["en:Canada"], "canada") is True

    def test_not_a_list(self):
        assert _matches_country(None, "canada") is False
        assert _matches_country("en:canada", "canada") is False


class TestDownloadAndFilter:

    def _make_gzip(self, records):
        jsonl = "\n".join(json.dumps(r) for r in records)
        return gzip.compress(jsonl.encode("utf-8"))

    def _mock_session(self, content):
        mock_session = Mock()
        mock_head = Mock()
        mock_head.headers = {"Content-Length": str(len(content))}
        mock_session.head.return_value = mock_head
        mock_get = Mock()
        mock_get.content = content
        mock_session.get.return_value = mock_get
        return mock_session

    def test_returns_only_matching_country(self):
        """Only records matching the given country are returned."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
            {"code": "3", "countries_tags": ["en:canada", "en:usa"]},
        ]
        session = self._mock_session(self._make_gzip(records))

        result = _download_and_filter(session, BASE_URL + FILENAME, "canada")

        assert len(result) == 2
        assert {r["code"] for r in result} == {"1", "3"}

    def test_different_country(self):
        """Works with a country other than canada."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
        ]
        session = self._mock_session(self._make_gzip(records))

        result = _download_and_filter(session, BASE_URL + FILENAME, "france")

        assert len(result) == 1
        assert result[0]["code"] == "2"

    def test_empty_result_when_no_match(self):
        records = [{"code": "1", "countries_tags": ["en:france"]}]
        session = self._mock_session(self._make_gzip(records))

        result = _download_and_filter(session, BASE_URL + FILENAME, "canada")

        assert result == []


class TestExtractDelta:

    @pytest.fixture
    def mock_env_vars(self):
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars

    def test_uploads_parquet_with_correct_records(self, mock_env_vars):
        """Filtered records are uploaded as parquet to the given S3 key."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:canada"]},
        ]

        with patch("commands.extract_delta._download_and_filter", return_value=records), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle(FILENAME, "delta/output.parquet", BASE_URL)

            mock_s3_instance.upload_dataframe.assert_called_once()
            result_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert len(result_df) == 2
            assert set(result_df["code"]) == {"1", "2"}

    def test_complex_columns_serialized_to_json_strings(self, mock_env_vars):
        """Complex columns (lists, dicts) are serialized to JSON strings for parquet compatibility."""
        records = [
            {
                "code": "1",
                "countries_tags": ["en:canada"],
                "images": {"selected": {"front": {"en": {"rev": "42"}}}},
                "nutriments": {"energy-kcal_100g": 539},
            }
        ]

        with patch("commands.extract_delta._download_and_filter", return_value=records), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle(FILENAME, "delta/output.parquet", BASE_URL)

            result_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert isinstance(result_df["images"].iloc[0], str)
            assert isinstance(result_df["nutriments"].iloc[0], str)
            assert isinstance(result_df["countries_tags"].iloc[0], str)
            assert json.loads(result_df["images"].iloc[0]) == records[0]["images"]

    def test_mixed_type_scalar_columns_serialized_to_strings(self, mock_env_vars):
        """Columns with mixed scalar types (e.g. int and str) are cast to string."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"], "max_imgid": 5},
            {"code": "2", "countries_tags": ["en:canada"], "max_imgid": "7"},
        ]

        with patch("commands.extract_delta._download_and_filter", return_value=records), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle(FILENAME, "delta/output.parquet", BASE_URL)

            result_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert result_df["max_imgid"].iloc[0] == "5"
            assert result_df["max_imgid"].iloc[1] == "7"

    def test_empty_parquet_uploaded_when_no_records(self, mock_env_vars):
        """An empty parquet is uploaded when no records match the country filter."""
        with patch("commands.extract_delta._download_and_filter", return_value=[]), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle(FILENAME, "delta/output.parquet", BASE_URL)

            mock_s3_instance.upload_dataframe.assert_called_once()
            result_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert len(result_df) == 0

    def test_missing_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle(FILENAME, "delta/output.jsonl", BASE_URL)
