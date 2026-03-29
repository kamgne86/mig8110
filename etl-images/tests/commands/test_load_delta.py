import os
import pytest
from unittest.mock import Mock, patch
from commands.load_delta import handle


class TestLoadDelta:

    @pytest.fixture
    def mock_env_vars(self):
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
            "DUCKDB_TOKEN": "test-token",
            "DUCKDB_DB": "test-db",
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars

    def test_upsert_sql_sequence(self, mock_env_vars):
        """CREATE TABLE IF NOT EXISTS → DELETE → INSERT are issued in order."""
        with patch("commands.load_delta.S3FileHandler") as mock_s3_cls, \
             patch("commands.load_delta.duckdb.connect") as mock_connect:

            mock_s3_cls.return_value = Mock()
            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("delta/transformed.parquet", "source_transformed", "staging")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            assert any("CREATE SCHEMA IF NOT EXISTS staging" in s for s in sql_calls)
            assert any("CREATE TABLE IF NOT EXISTS staging.source_transformed" in s for s in sql_calls)
            assert any("DELETE FROM staging.source_transformed" in s for s in sql_calls)
            assert any("INSERT INTO staging.source_transformed" in s for s in sql_calls)

    def test_upsert_order(self, mock_env_vars):
        """DELETE must come before INSERT to avoid removing newly inserted rows."""
        with patch("commands.load_delta.S3FileHandler") as mock_s3_cls, \
             patch("commands.load_delta.duckdb.connect") as mock_connect:

            mock_s3_cls.return_value = Mock()
            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("delta/transformed.parquet", "source_transformed", "staging")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            delete_idx = next(i for i, s in enumerate(sql_calls) if "DELETE" in s)
            insert_idx = next(i for i, s in enumerate(sql_calls) if "INSERT" in s)
            assert delete_idx < insert_idx

    def test_reads_parquet_not_jsonl(self, mock_env_vars):
        """load_delta reads parquet files, not JSONL."""
        with patch("commands.load_delta.S3FileHandler") as mock_s3_cls, \
             patch("commands.load_delta.duckdb.connect") as mock_connect:

            mock_s3_cls.return_value = Mock()
            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("delta/transformed.parquet", "source_transformed", "staging")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            assert all("read_json_auto" not in s for s in sql_calls)
            assert any("read_parquet" in s for s in sql_calls)

    def test_connection_closed(self, mock_env_vars):
        """DuckDB connection is always closed after the operation."""
        with patch("commands.load_delta.S3FileHandler") as mock_s3_cls, \
             patch("commands.load_delta.duckdb.connect") as mock_connect:

            mock_s3_cls.return_value = Mock()
            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("delta/transformed.parquet", "source_transformed", "staging")

            mock_con.close.assert_called_once()

    def test_missing_s3_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("delta/transformed.parquet", "source_transformed", "staging")

    def test_missing_duckdb_env_var(self):
        """KeyError is raised when DuckDB environment variables are missing."""
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(KeyError):
                handle("delta/transformed.parquet", "source_transformed", "staging")
