import os
import pytest
from unittest.mock import Mock, patch
from commands.load_data import handle


class TestLoadData:
    """Tests for load_data command"""

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
        """Test successful parquet load into DuckDB — table is replaced, not appended."""
        with patch("commands.load_data.S3FileHandler") as mock_s3, \
             patch("commands.load_data.duckdb.connect") as mock_connect:

            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            mock_con = Mock()
            mock_connect.return_value = mock_con

            handle("staging/products.parquet", "canada_products", "staging")

            mock_s3.assert_called_once_with(
                "test-bucket",
                "https://s3.example.com",
                "test-key",
                "test-secret",
            )
            mock_s3_instance.download.assert_called_once()
            mock_connect.assert_called_once_with("md:test-db?motherduck_token=test-token")

            sql_calls = [c[0][0] for c in mock_con.sql.call_args_list]
            assert any("CREATE OR REPLACE TABLE staging.canada_products" in s for s in sql_calls)
            assert any("read_parquet" in s for s in sql_calls)

            mock_con.close.assert_called_once()

    def test_handle_missing_s3_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("staging/products.parquet", "canada_products", "staging")

    def test_handle_missing_duckdb_env_var(self):
        """KeyError is raised when DuckDB environment variables are missing."""
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(KeyError):
                handle("staging/products.parquet", "canada_products", "staging")
