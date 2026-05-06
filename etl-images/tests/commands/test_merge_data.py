import os
import pytest
from unittest.mock import Mock, patch, call
from commands.merge_data import handle


class TestMergeData:
    """Tests for merge_data command (DELETE + INSERT UPSERT into Silver)."""

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

    def test_handle_success(self, mock_env_vars):
        """Test that DELETE then INSERT are executed in the correct order."""
        with patch("commands.merge_data.S3FileHandler") as mock_s3, \
             patch("commands.merge_data.duckdb.connect") as mock_connect:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("silver/delta_transformed.parquet", "source_transformed", "staging")

            mock_s3.assert_called_once_with(
                "test-bucket",
                "https://s3.example.com",
                "test-key",
                "test-secret",
            )
            mock_s3_instance.download.assert_called_once()
            mock_connect.assert_called_once_with("md:test-db?motherduck_token=test-token")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            assert any("DELETE FROM staging.source_transformed" in s for s in sql_calls)
            assert any("INSERT INTO staging.source_transformed" in s for s in sql_calls)

    def test_delete_before_insert(self, mock_env_vars):
        """DELETE must be called before INSERT."""
        with patch("commands.merge_data.S3FileHandler"), \
             patch("commands.merge_data.duckdb.connect") as mock_connect:

            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("silver/delta_transformed.parquet", "source_transformed", "staging")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            delete_idx = next(i for i, s in enumerate(sql_calls) if "DELETE" in s)
            insert_idx = next(i for i, s in enumerate(sql_calls) if "INSERT" in s)
            assert delete_idx < insert_idx

    def test_connection_is_closed(self, mock_env_vars):
        """DuckDB connection must always be closed."""
        with patch("commands.merge_data.S3FileHandler"), \
             patch("commands.merge_data.duckdb.connect") as mock_connect:

            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("silver/delta_transformed.parquet", "source_transformed", "staging")

            mock_con.close.assert_called_once()

    def test_missing_s3_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        env = {k: v for k, v in os.environ.items()
               if k not in ("S3_BUCKET", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(KeyError):
                handle("silver/delta_transformed.parquet", "source_transformed", "staging")

    def test_missing_duckdb_env_var(self):
        """KeyError is raised when DuckDB environment variables are missing."""
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
        }
        env = {k: v for k, v in os.environ.items() if k not in ("DUCKDB_TOKEN", "DUCKDB_DB")}
        env.update(env_vars)
        with patch.dict(os.environ, env, clear=True), \
             patch("commands.merge_data.S3FileHandler") as mock_s3:
            mock_s3.return_value = Mock()
            with pytest.raises(KeyError):
                handle("silver/delta_transformed.parquet", "source_transformed", "staging")
