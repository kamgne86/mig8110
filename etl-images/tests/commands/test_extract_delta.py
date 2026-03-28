import os
import gzip
import json
import pytest
from unittest.mock import Mock, patch
from commands.extract_delta import handle, _get_delta_filenames, _download_delta, _matches_country

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

    def _mock_download_session(self, gzipped_content):
        """Create a mock session for Range-request based download (HEAD + GET per chunk)."""
        mock_session = Mock()

        mock_head = Mock()
        mock_head.raise_for_status = Mock()
        mock_head.headers = {"Content-Length": str(len(gzipped_content))}
        mock_session.head.return_value = mock_head

        mock_get = Mock()
        mock_get.raise_for_status = Mock()
        mock_get.content = gzipped_content
        mock_session.get.return_value = mock_get

        return mock_session

    def test_matches_country_exact_tag(self):
        assert _matches_country(["en:canada", "en:france"], "canada") is True

    def test_matches_country_no_match(self):
        assert _matches_country(["en:france", "en:usa"], "canada") is False

    def test_matches_country_case_insensitive(self):
        assert _matches_country(["en:Canada"], "canada") is True

    def test_matches_country_not_a_list(self):
        assert _matches_country(None, "canada") is False
        assert _matches_country("en:canada", "canada") is False

    def test_download_delta(self, gzipped_jsonl, delta_records):
        """Test downloading via Range requests and decompressing a single delta file."""
        mock_session = self._mock_download_session(gzipped_jsonl)

        records = _download_delta(mock_session, BASE_URL, "test_file.json.gz", "canada")

        mock_session.head.assert_called_once()
        assert len(records) == 2
        assert records[0]["code"] == "123"
        assert records[1]["product_name"] == "Test Product B"

    def test_download_delta_filters_by_country(self):
        """Test that _download_delta only returns records matching the given country."""
        mixed_records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
            {"code": "3", "countries_tags": ["en:canada", "en:usa"]},
        ]
        jsonl = "\n".join(json.dumps(r) for r in mixed_records)
        content = gzip.compress(jsonl.encode("utf-8"))

        records = _download_delta(self._mock_download_session(content), BASE_URL, "test.json.gz", "canada")

        assert len(records) == 2
        assert {r["code"] for r in records} == {"1", "3"}

    def test_download_delta_different_country(self):
        """Test that _download_delta works with a different country."""
        mixed_records = [
            {"code": "1", "countries_tags": ["en:canada"]},
            {"code": "2", "countries_tags": ["en:france"]},
        ]
        jsonl = "\n".join(json.dumps(r) for r in mixed_records)
        content = gzip.compress(jsonl.encode("utf-8"))

        records = _download_delta(self._mock_download_session(content), BASE_URL, "test.json.gz", "france")

        assert len(records) == 1
        assert records[0]["code"] == "2"

    def test_handle_success(self, mock_env_vars):
        """Test successful delta extraction with all files."""
        canadian_records = [{"code": "1", "countries_tags": ["en:canada"]}]

        with patch("commands.extract_delta._get_delta_filenames", return_value=["1000_1100.json.gz", "1100_1200.json.gz"]), \
             patch("commands.extract_delta._download_delta", return_value=canadian_records), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle("output/delta.jsonl", INDEX_URL)

            mock_s3.assert_called_once_with("test-bucket", "https://s3.example.com", "test-key", "test-secret")
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

    def test_handle_uploads_jsonl(self, mock_env_vars):
        """Test that uploaded content is valid JSONL with the expected records."""
        canadian_records = [
            {"code": "1", "product_name": "Canadian", "countries_tags": ["en:canada"]},
            {"code": "3", "product_name": "Multi",    "countries_tags": ["en:canada", "en:usa"]},
        ]
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.extract_delta._get_delta_filenames", return_value=["f.json.gz"]), \
             patch("commands.extract_delta._download_delta", return_value=canadian_records), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("output/delta.jsonl", INDEX_URL)

        lines = uploaded["output/delta.jsonl"].decode("utf-8").strip().split("\n")
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

    # --- Tests for last_processed_file ---

    def test_last_processed_file_filters_older_files(self, mock_env_vars):
        """Only files strictly after last_processed_file are processed."""
        all_files = ["1000_1100.json.gz", "1100_1200.json.gz", "1200_1300.json.gz"]
        processed = []

        def fake_download(session, base_url, filename, country):
            processed.append(filename)
            return [{"code": "1", "countries_tags": ["en:canada"]}]

        with patch("commands.extract_delta._get_delta_filenames", return_value=all_files), \
             patch("commands.extract_delta._download_delta", side_effect=fake_download), \
             patch("commands.extract_delta.S3FileHandler"):

            handle("output/delta.parquet", INDEX_URL, last_processed_file="1100_1200.json.gz")

        assert processed == ["1200_1300.json.gz"]

    def test_last_processed_file_no_new_files(self, mock_env_vars):
        """Nothing is uploaded when all files have already been processed."""
        all_files = ["1000_1100.json.gz", "1100_1200.json.gz"]

        with patch("commands.extract_delta._get_delta_filenames", return_value=all_files), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            handle("output/delta.parquet", INDEX_URL, last_processed_file="1100_1200.json.gz")

            mock_s3_instance.upload_from_memory.assert_not_called()

    def test_last_processed_file_takes_precedence_over_num_files(self, mock_env_vars):
        """last_processed_file takes precedence over num_files."""
        all_files = ["1000_1100.json.gz", "1100_1200.json.gz", "1200_1300.json.gz"]
        processed = []

        def fake_download(session, base_url, filename, country):
            processed.append(filename)
            return [{"code": "1", "countries_tags": ["en:canada"]}]

        with patch("commands.extract_delta._get_delta_filenames", return_value=all_files), \
             patch("commands.extract_delta._download_delta", side_effect=fake_download), \
             patch("commands.extract_delta.S3FileHandler"):

            # num_files=2 would take the last 2 files, but last_processed_file should win
            handle("output/delta.parquet", INDEX_URL, num_files=2, last_processed_file="1000_1100.json.gz")

        assert processed == ["1100_1200.json.gz", "1200_1300.json.gz"]

    def test_last_processed_file_none_falls_back_to_num_files(self, mock_env_vars):
        """When last_processed_file is None, num_files is applied."""
        all_files = ["1000_1100.json.gz", "1100_1200.json.gz", "1200_1300.json.gz"]
        processed = []

        def fake_download(session, base_url, filename, country):
            processed.append(filename)
            return [{"code": "1", "countries_tags": ["en:canada"]}]

        with patch("commands.extract_delta._get_delta_filenames", return_value=all_files), \
             patch("commands.extract_delta._download_delta", side_effect=fake_download), \
             patch("commands.extract_delta.S3FileHandler"):

            handle("output/delta.parquet", INDEX_URL, num_files=1)

        assert processed == ["1200_1300.json.gz"]
