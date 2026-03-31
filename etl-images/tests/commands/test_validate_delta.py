import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.validate_delta import handle


def _df_to_parquet_bytes(df):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf


@pytest.fixture
def mock_env_vars():
    env_vars = {
        "S3_BUCKET": "test-bucket",
        "S3_ENDPOINT": "https://s3.example.com",
        "S3_ACCESS_KEY": "test-key",
        "S3_SECRET_KEY": "test-secret",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def sample_df():
    """DataFrame with 2 valid records and 3 invalid records."""
    nutrition = {"aggregated_set": {"nutrients": {"energy-kcal": {"value": 100.0}}}}
    ingredients = [{"id": "en:water", "text": "Water"}]
    categories = ["en:beverages"]
    return pd.DataFrame({
        "code":             ["111",   "222",   None,    "",      "555"],
        "product_name": [
            "Product A",
            "Product B",
            "Product C",
            "Product D",
            None,
        ],
        "brands":           ["Brand A", "Brand B", "Brand C", "Brand D", "Brand E"],
        "nutriscore_grade": ["a",     "b",     "c",     "d",     "e"],
        "ecoscore_grade":   ["a",     "b",     "c",     "d",     "e"],
        "nutrition":        [nutrition, nutrition, nutrition, nutrition, nutrition],
        "ingredients":      [ingredients, ingredients, ingredients, ingredients, ingredients],
        "categories_tags":  [categories, categories, categories, categories, categories],
    })


class TestValidateDelta:
    """Tests for validate_delta command"""

    def test_valid_records_are_uploaded(self, mock_env_vars, sample_df):
        """Valid records (non-null, non-empty code + non-null product_name + non-null nutrition) go to output_file_key."""
        with patch("commands.validate_delta.S3FileHandler") as mock_s3, \
             patch("commands.validate_delta.record_run"):
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data.parquet", "data_valid.parquet", "data_invalid.parquet")

            uploaded_dfs = {
                call[0][1]: call[0][0]
                for call in mock_s3_instance.upload_dataframe.call_args_list
            }
            assert list(uploaded_dfs["data_valid.parquet"]["code"]) == ["111", "222"]

    def test_invalid_records_are_uploaded(self, mock_env_vars, sample_df):
        """Invalid records (null/empty code or null product_name) go to invalid_file_key."""
        with patch("commands.validate_delta.S3FileHandler") as mock_s3, \
             patch("commands.validate_delta.record_run"):
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data.parquet", "data_valid.parquet", "data_invalid.parquet")

            uploaded_dfs = {
                call[0][1]: call[0][0]
                for call in mock_s3_instance.upload_dataframe.call_args_list
            }
            assert len(uploaded_dfs["data_invalid.parquet"]) == 3

    def test_both_files_are_uploaded(self, mock_env_vars, sample_df):
        """Both valid and invalid files are always uploaded."""
        with patch("commands.validate_delta.S3FileHandler") as mock_s3, \
             patch("commands.validate_delta.record_run"):
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data.parquet", "data_valid.parquet", "data_invalid.parquet")

            assert mock_s3_instance.upload_dataframe.call_count == 2
            uploaded_keys = [call[0][1] for call in mock_s3_instance.upload_dataframe.call_args_list]
            assert "data_valid.parquet" in uploaded_keys
            assert "data_invalid.parquet" in uploaded_keys

    def test_all_valid(self, mock_env_vars):
        """When all records are valid, invalid file is empty."""
        nutrition = {"aggregated_set": {"nutrients": {"energy-kcal": {"value": 100.0}}}}
        df = pd.DataFrame({
            "code":         ["111", "222"],
            "product_name": ["Product A", "Product B"],
            "nutrition":    [nutrition, nutrition],
        })

        with patch("commands.validate_delta.S3FileHandler") as mock_s3, \
             patch("commands.validate_delta.record_run"):
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(df)

            handle("data.parquet", "data_valid.parquet", "data_invalid.parquet")

            uploaded_dfs = {
                call[0][1]: call[0][0]
                for call in mock_s3_instance.upload_dataframe.call_args_list
            }
            assert len(uploaded_dfs["data_invalid.parquet"]) == 0

    def test_missing_env_var(self, sample_df):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("data.parquet", "data_valid.parquet", "data_invalid.parquet")
