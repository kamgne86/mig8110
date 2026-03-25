import os
import gzip
import json
import pytest
from io import BytesIO
from unittest.mock import Mock, patch
from commands.extract_delta import handle, _get_delta_filenames, _download_delta

INDEX_URL = "https://static.openfoodfacts.org/data/delta/index.txt"
BASE_URL = "https://static.openfoodfacts.org/data/delta/"


class TestExtractDelta:
    """Tests for extract delta command"""

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

    @pytest.fixture
    def delta_records(self):
        """Sample JSONL records as they would appear in a delta file."""
        return [
            {"code": "123", "product_name": "Test Product A", "countries_tags": ["en:canada"]},
            {"code": "456", "product_name": "Test Product B", "countries_tags": ["en:canada", "en:france"]},
        ]

    @pytest.fixture
    def gzipped_jsonl(self, delta_records):
        """Create gzipped JSONL content from sample records."""
        jsonl = "\n".join(json.dumps(r) for r in delta_records)
        return gzip.compress(jsonl.encode("utf-8"))

    def test_get_delta_filenames(self):
        """Test fetching delta index."""
        index_content = "file1.json.gz\nfile2.json.gz\nfile3.json.gz\n"

        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = index_content
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response

        filenames = _get_delta_filenames(mock_session, INDEX_URL)

        assert filenames == ["file1.json.gz", "file2.json.gz", "file3.json.gz"]
        mock_session.get.assert_called_once_with(INDEX_URL, timeout=30)

    def _mock_stream_response(self, gzipped_content):
        """Create a mock streaming response with .raw as BytesIO."""
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.raw = BytesIO(gzipped_content)
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        return mock_response

    def test_download_delta(self, gzipped_jsonl, delta_records):
        """Test downloading and decompressing a single delta file."""
        mock_session = Mock()
        mock_session.get.return_value = self._mock_stream_response(gzipped_jsonl)

        records = _download_delta(mock_session, BASE_URL, "test_file.json.gz")

        assert len(records) == 2
        assert records[0]["code"] == "123"
        assert records[1]["product_name"] == "Test Product B"

    def test_download_delta_filters_non_canadian(self):
        """Test that _download_delta only returns Canadian products."""
        mixed_records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
            {"code": "3", "countries_tags": ["en:canada", "en:usa"]},
        ]
        jsonl = "\n".join(json.dumps(r) for r in mixed_records)
        content = gzip.compress(jsonl.encode("utf-8"))

        mock_session = Mock()
        mock_session.get.return_value = self._mock_stream_response(content)

        records = _download_delta(mock_session, BASE_URL, "test.json.gz")

        assert len(records) == 2
        assert {r["code"] for r in records} == {"1", "3"}

    def test_handle_success(self, mock_env_vars, gzipped_jsonl):
        """Test successful delta extraction with all files."""
        index_content = "1000_1100.json.gz\n1100_1200.json.gz\n"

        with patch("commands.extract_delta.requests.Session") as mock_session_cls, \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_session = Mock()
            mock_session_cls.return_value = mock_session

            def side_effect(url, **kwargs):
                if "index.txt" in url:
                    mock_response = Mock()
                    mock_response.raise_for_status = Mock()
                    mock_response.text = index_content
                    return mock_response
                return self._mock_stream_response(gzipped_jsonl)

            mock_session.get.side_effect = side_effect
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle("output/delta.parquet", INDEX_URL)

            mock_s3.assert_called_once_with(
                "test-bucket",
                "https://s3.example.com",
                "test-key",
                "test-secret",
            )
            mock_s3_instance.upload_from_memory.assert_called_once()

    def test_handle_empty_index(self, mock_env_vars):
        """Test behaviour when no delta files are available."""
        with patch("commands.extract_delta.requests.Session") as mock_session_cls, \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_session = Mock()
            mock_session_cls.return_value = mock_session
            mock_response = Mock()
            mock_response.text = ""
            mock_response.raise_for_status = Mock()
            mock_session.get.return_value = mock_response

            handle("output/delta.parquet", INDEX_URL)

            mock_s3.assert_not_called()

    def test_handle_filters_canadian_products(self, mock_env_vars):
        """Test that only Canadian products are kept."""
        canadian_only = [
            {"code": "1", "product_name": "Canadian", "countries_tags": ["en:canada"]},
            {"code": "3", "product_name": "Multi", "countries_tags": ["en:canada", "en:usa"]},
        ]

        with patch("commands.extract_delta._get_delta_filenames", return_value=["f.json.gz"]), \
             patch("commands.extract_delta._download_delta", return_value=canadian_only), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle("output/delta.parquet", INDEX_URL)

            mock_s3_instance.upload_from_memory.assert_called_once()
            uploaded = mock_s3_instance.upload_from_memory.call_args[0][0]
            lines = uploaded.read().decode("utf-8").strip().split("\n")
            records = [json.loads(line) for line in lines]
            assert len(records) == 2
            assert {r["code"] for r in records} == {"1", "3"}

    def test_handle_no_canadian_products(self, mock_env_vars):
        """Test that nothing is uploaded when no Canadian products exist."""
        with patch("commands.extract_delta._get_delta_filenames", return_value=["f.json.gz"]), \
             patch("commands.extract_delta._download_delta", return_value=[]), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle("output/delta.parquet", INDEX_URL)

            mock_s3_instance.upload_from_memory.assert_not_called()

    def test_handle_missing_env_var(self):
        """Test handling of missing environment variables."""
        env = {k: v for k, v in os.environ.items() if k not in ("S3_BUCKET", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")}
        with patch.dict(os.environ, env, clear=True), \
             patch("commands.extract_delta._get_delta_filenames", return_value=["file.json.gz"]), \
             patch("commands.extract_delta._download_delta", return_value=[{"code": "1"}]):
            with pytest.raises(KeyError):
                handle("output/delta.parquet", INDEX_URL)
