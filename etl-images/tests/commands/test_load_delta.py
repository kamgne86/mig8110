import os
import json
import pytest
from unittest.mock import Mock, patch, call
from commands.load_delta import handle


class TestLoadDelta:
    """Tests for load delta command"""

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
        """Test successful JSONL load into DuckDB."""
        with patch("commands.load_delta.S3FileHandler") as mock_s3, \
             patch("commands.load_delta.duckdb.connect") as mock_connect:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("delta.jsonl", "products_delta", "off")

            mock_s3.assert_called_once_with(
                "test-bucket",
                "https://s3.example.com",
                "test-key",
                "test-secret",
            )
            mock_s3_instance.download.assert_called_once()
            mock_connect.assert_called_once_with("md:test-db?motherduck_token=test-token")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            assert any("CREATE SCHEMA IF NOT EXISTS off" in s for s in sql_calls)
            assert any("read_json_auto" in s and "off.products_delta" in s for s in sql_calls)

            mock_con.close.assert_called_once()

    def test_handle_missing_s3_env_var(self):
        """Test handling of missing S3 environment variables."""
        env = {k: v for k, v in os.environ.items()
               if k not in ("S3_BUCKET", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(KeyError):
                handle("delta.jsonl", "products_delta", "off")

    def test_handle_missing_duckdb_env_var(self):
        """Test handling of missing DuckDB environment variables."""
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
        }
        env = {k: v for k, v in os.environ.items()
               if k not in ("DUCKDB_TOKEN", "DUCKDB_DB")}
        env.update(env_vars)
        with patch.dict(os.environ, env, clear=True), \
             patch("commands.load_delta.S3FileHandler") as mock_s3:
            mock_s3.return_value = Mock()
            with pytest.raises(KeyError):
                handle("delta.jsonl", "products_delta", "off")
