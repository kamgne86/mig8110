import os
import gzip
import json
import pytest
import tempfile
import pandas as pd
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
        """Only records matching the given country are written to parquet."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
            {"code": "3", "countries_tags": ["en:canada", "en:usa"]},
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(session, BASE_URL + FILENAME, "canada")
        try:
            df = pd.read_parquet(path)
            assert count == 2
            assert set(df["code"]) == {"1", "3"}
        finally:
            os.unlink(path)

    def test_different_country(self):
        """Works with a country other than canada."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(session, BASE_URL + FILENAME, "france")
        try:
            df = pd.read_parquet(path)
            assert count == 1
            assert df["code"].iloc[0] == "2"
        finally:
            os.unlink(path)

    def test_empty_result_when_no_match(self):
        """Returns count=0 when no records match."""
        records = [{"code": "1", "countries_tags": ["en:france"]}]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(session, BASE_URL + FILENAME, "canada")
        try:
            assert count == 0
        finally:
            os.unlink(path)

    def test_complex_columns_serialized_to_json_strings(self):
        """Complex columns (lists, dicts) are serialized to JSON strings."""
        records = [
            {
                "code": "1",
                "countries_tags": ["en:canada"],
                "images": {"selected": {"front": {"en": {"rev": "42"}}}},
                "nutriments": {"energy-kcal_100g": 539},
            }
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(session, BASE_URL + FILENAME, "canada")
        try:
            df = pd.read_parquet(path)
            assert isinstance(df["images"].iloc[0], str)
            assert isinstance(df["nutriments"].iloc[0], str)
            assert isinstance(df["countries_tags"].iloc[0], str)
            assert json.loads(df["images"].iloc[0]) == records[0]["images"]
        finally:
            os.unlink(path)


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

    def _make_parquet(self, records):
        df = pd.DataFrame(records)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.to_parquet(tmp.name, index=False)
        tmp.close()
        return tmp.name

    def test_uploads_parquet_to_correct_s3_key(self, mock_env_vars):
        """handle() uploads the parquet to the given S3 key via upload_from_memory."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:canada"]},
        ]
        parquet_path = self._make_parquet(records)

        with patch("commands.extract_delta._download_and_filter", return_value=(parquet_path, 2)), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle(FILENAME, "delta/output.parquet", BASE_URL)

            mock_s3_instance.upload_from_memory.assert_called_once()
            _, s3_key = mock_s3_instance.upload_from_memory.call_args[0]
            assert s3_key == "delta/output.parquet"

    def test_temp_parquet_deleted_after_upload(self, mock_env_vars):
        """The local temp parquet file is deleted after upload."""
        records = [{"code": "1", "countries_tags": ["en:canada"]}]
        parquet_path = self._make_parquet(records)

        with patch("commands.extract_delta._download_and_filter", return_value=(parquet_path, 1)), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3.return_value = Mock()
            handle(FILENAME, "delta/output.parquet", BASE_URL)

            assert not os.path.exists(parquet_path)

    def test_empty_parquet_uploaded_when_no_records(self, mock_env_vars):
        """upload_from_memory is still called when no records match."""
        parquet_path = self._make_parquet([])

        with patch("commands.extract_delta._download_and_filter", return_value=(parquet_path, 0)), \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle(FILENAME, "delta/output.parquet", BASE_URL)

            mock_s3_instance.upload_from_memory.assert_called_once()

    def test_missing_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle(FILENAME, "delta/output.jsonl", BASE_URL)
