import os
import gzip
import json
import pytest
import tempfile
import pandas as pd
from unittest.mock import Mock, patch
import pyarrow as pa
from commands.extract_delta import (
    handle,
    _download_and_filter,
    _matches_country,
    _serialize_record,
    _build_schema,
    _batch_to_table,
    _parse_keep_columns,
)

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


class TestSerializeRecord:

    def test_list_serialized_to_json_string(self):
        record = {"tags": ["en:canada", "en:france"]}
        result = _serialize_record(record)
        assert result["tags"] == '["en:canada", "en:france"]'

    def test_dict_serialized_to_json_string(self):
        record = {"images": {"front": {"rev": "42"}}}
        result = _serialize_record(record)
        assert result["images"] == '{"front": {"rev": "42"}}'

    def test_none_stays_none(self):
        record = {"code": None}
        result = _serialize_record(record)
        assert result["code"] is None

    def test_nan_becomes_none(self):
        record = {"score": float("nan")}
        result = _serialize_record(record)
        assert result["score"] is None

    def test_int_stays_int(self):
        record = {"max_imgid": 5}
        result = _serialize_record(record)
        assert result["max_imgid"] == 5

    def test_str_int_becomes_str(self):
        record = {"max_imgid": "5"}
        result = _serialize_record(record)
        assert result["max_imgid"] == "5"

    def test_bool_stays_bool(self):
        record = {"obsolete": True}
        result = _serialize_record(record)
        assert result["obsolete"] is True


class TestBuildSchema:

    def test_all_columns_are_large_utf8(self):
        schema = _build_schema({"code", "brands", "max_imgid"})
        for field in schema:
            assert field.type == pa.large_utf8()

    def test_columns_sorted(self):
        schema = _build_schema({"z_col", "a_col", "m_col"})
        assert schema.names == ["a_col", "m_col", "z_col"]


class TestBatchToTable:

    def test_all_values_cast_to_string(self):
        schema = _build_schema({"code", "max_imgid"})
        batch = [
            {"code": "1", "max_imgid": 3},
            {"code": "2", "max_imgid": "7"},
        ]
        table = _batch_to_table(batch, schema)
        assert table["max_imgid"][0].as_py() == "3"
        assert table["max_imgid"][1].as_py() == "7"

    def test_missing_column_filled_with_none(self):
        schema = _build_schema({"code", "brands"})
        batch = [{"code": "1"}]  # brands absent
        table = _batch_to_table(batch, schema)
        assert table["brands"][0].as_py() is None

    def test_table_schema_matches_input_schema(self):
        schema = _build_schema({"code", "brands"})
        batch = [{"code": "1", "brands": "Nestlé"}]
        table = _batch_to_table(batch, schema)
        assert table.schema == schema


class TestParseKeepColumns:

    def test_none_returns_none(self):
        """None input means keep all columns."""
        assert _parse_keep_columns(None) is None

    def test_empty_string_returns_none(self):
        """Empty string is treated as no filter."""
        assert _parse_keep_columns("") is None

    def test_simple_comma_separated(self):
        """Comma-separated columns are each added to the set."""
        result = _parse_keep_columns("code,brands,product_name")
        assert result == {"code", "brands", "product_name"}

    def test_pipe_syntax_both_sides_kept(self):
        """Pipe syntax keeps both primary and fallback column names."""
        result = _parse_keep_columns("ecoscore_score|environmental_score_score")
        assert result == {"ecoscore_score", "environmental_score_score"}

    def test_combined_comma_and_pipe(self):
        """Mix of comma and pipe syntax is handled correctly."""
        result = _parse_keep_columns("code,brands,ecoscore_score|environmental_score_score")
        assert result == {"code", "brands", "ecoscore_score", "environmental_score_score"}

    def test_whitespace_around_column_names_stripped(self):
        """Extra spaces around column names are stripped."""
        result = _parse_keep_columns(" code , brands ")
        assert result == {"code", "brands"}

    def test_returns_set(self):
        """Return type is always a set (for O(1) membership testing)."""
        result = _parse_keep_columns("code,code,brands")
        assert isinstance(result, set)
        assert result == {"code", "brands"}


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

    def test_mixed_type_scalar_columns_cast_to_string(self):
        """Columns with mixed scalar types (e.g. int and str) are cast to string across batches."""
        records = [
            {"code": "1", "countries_tags": ["en:canada"], "max_imgid": 3},
            {"code": "2", "countries_tags": ["en:canada"], "max_imgid": "7"},
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(session, BASE_URL + FILENAME, "canada")
        try:
            df = pd.read_parquet(path)
            assert df["max_imgid"].iloc[0] == "3"
            assert df["max_imgid"].iloc[1] == "7"
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

    def test_columns_filter_keeps_only_specified_columns(self):
        """When columns= is given, the parquet only contains those columns."""
        records = [
            {
                "code": "1",
                "countries_tags": ["en:canada"],
                "brands": "Nestlé",
                "product_name": "Choco",
                "ingredients_text": "sugar, cocoa",
            }
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(
            session, BASE_URL + FILENAME, "canada",
            columns="code,brands,countries_tags"
        )
        try:
            df = pd.read_parquet(path)
            assert set(df.columns) == {"code", "brands", "countries_tags"}
            assert "product_name" not in df.columns
            assert "ingredients_text" not in df.columns
        finally:
            os.unlink(path)

    def test_columns_filter_pipe_syntax_keeps_both_alternatives(self):
        """Pipe syntax (primary|fallback) keeps both column names if present."""
        records = [
            {
                "code": "1",
                "countries_tags": ["en:canada"],
                "ecoscore_score": "80",
                "environmental_score_score": "75",
                "brands": "Nestlé",
            }
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(
            session, BASE_URL + FILENAME, "canada",
            columns="code,ecoscore_score|environmental_score_score"
        )
        try:
            df = pd.read_parquet(path)
            assert "ecoscore_score" in df.columns
            assert "environmental_score_score" in df.columns
            assert "brands" not in df.columns
        finally:
            os.unlink(path)

    def test_columns_none_keeps_all_columns(self):
        """When columns=None (default), all columns are preserved."""
        records = [
            {
                "code": "1",
                "countries_tags": ["en:canada"],
                "brands": "Nestlé",
                "product_name": "Choco",
            }
        ]
        session = self._mock_session(self._make_gzip(records))

        path, count = _download_and_filter(
            session, BASE_URL + FILENAME, "canada", columns=None
        )
        try:
            df = pd.read_parquet(path)
            assert "brands" in df.columns
            assert "product_name" in df.columns
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

    def test_columns_param_forwarded_to_download(self, mock_env_vars):
        """handle() forwards the columns parameter to _download_and_filter."""
        parquet_path = self._make_parquet([{"code": "1"}])

        with patch("commands.extract_delta._download_and_filter", return_value=(parquet_path, 1)) as mock_dl, \
             patch("commands.extract_delta.requests.Session"), \
             patch("commands.extract_delta.S3FileHandler") as mock_s3:

            mock_s3.return_value = Mock()
            handle(FILENAME, "delta/output.parquet", BASE_URL, columns="code,brands")

            _, call_kwargs = mock_dl.call_args
            assert call_kwargs.get("columns") == "code,brands" or mock_dl.call_args[0][3] == "code,brands"

    def test_missing_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle(FILENAME, "delta/output.jsonl", BASE_URL)
