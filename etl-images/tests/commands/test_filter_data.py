import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.filter_data import handle


class TestFilterData:
    """Tests for filter_data command"""

    @pytest.fixture
    def mock_env_vars(self):
        """Mock environment variables"""
        env_vars = {
            "S3_BUCKET": "test-bucket",
            "S3_ENDPOINT": "https://s3.example.com",
            "S3_ACCESS_KEY": "test-key",
            "S3_SECRET_KEY": "test-secret",
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars

    @pytest.fixture
    def parquet_data(self):
        """Create a sample parquet file with multiple columns"""
        df = pd.DataFrame({
            "code": [1, 2, 3],
            "product_name": ["a", "b", "c"],
            "brands": ["x", "y", "z"],
            "irrelevant_column": [True, False, True],
        })
        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)
        return parquet_bytes, df

    def test_handle_success(self, mock_env_vars, parquet_data):
        """Test that only requested columns are kept"""
        parquet_bytes, _ = parquet_data
        columns = "code,product_name,brands"

        with patch("commands.filter_data.S3FileHandler") as mock_s3_cls:
            mock_s3 = Mock()
            mock_s3_cls.return_value = mock_s3
            mock_s3.download_to_memory.return_value = parquet_bytes

            handle("raw/input.parquet", "raw/filtered.parquet", columns)

            mock_s3.download_to_memory.assert_called_once_with("raw/input.parquet")
            mock_s3.upload_dataframe.assert_called_once()

            result_df = mock_s3.upload_dataframe.call_args[0][0]
            assert list(result_df.columns) == ["code", "product_name", "brands"]
            assert "irrelevant_column" not in result_df.columns

    def test_handle_columns_with_spaces(self, mock_env_vars, parquet_data):
        """Test that spaces around column names are stripped"""
        parquet_bytes, _ = parquet_data
        columns = "code, product_name, brands"

        with patch("commands.filter_data.S3FileHandler") as mock_s3_cls:
            mock_s3 = Mock()
            mock_s3_cls.return_value = mock_s3
            mock_s3.download_to_memory.return_value = parquet_bytes

            handle("raw/input.parquet", "raw/filtered.parquet", columns)

            result_df = mock_s3.upload_dataframe.call_args[0][0]
            assert list(result_df.columns) == ["code", "product_name", "brands"]

    def test_handle_missing_env_var(self, parquet_data):
        """Test handling of missing environment variables"""
        parquet_bytes, _ = parquet_data

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("raw/input.parquet", "raw/filtered.parquet", "code,product_name")
