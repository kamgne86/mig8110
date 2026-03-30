import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.filter_delta import handle, _resolve_columns


def _df_to_parquet_bytes(df):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf


SAMPLE_DF = pd.DataFrame({
    "code": ["111", "222"],
    "product_name": ["Product A", "Product B"],
    "brands": ["Brand A", "Brand B"],
    "environmental_score_grade": ["a", "b"],
    "environmental_score_score": [10.0, 20.0],
    "nutriments": ['{"fat_100g": 5.0}', '{"fat_100g": 3.0}'],
    "irrelevant": ["x", "y"],
})


class TestResolveColumns:

    def test_simple_column(self):
        df = pd.DataFrame({"code": [1], "brands": [2]})
        resolved = _resolve_columns(df, ["code", "brands"])
        assert resolved == {"code": "code", "brands": "brands"}

    def test_primary_takes_precedence_over_fallback(self):
        df = pd.DataFrame({"ecoscore_grade": ["a"], "environmental_score_grade": ["b"]})
        resolved = _resolve_columns(df, ["ecoscore_grade|environmental_score_grade"])
        assert resolved == {"ecoscore_grade": "ecoscore_grade"}

    def test_fallback_used_when_primary_missing(self):
        df = pd.DataFrame({"environmental_score_grade": ["a"]})
        resolved = _resolve_columns(df, ["ecoscore_grade|environmental_score_grade"])
        assert resolved == {"ecoscore_grade": "environmental_score_grade"}

    def test_missing_column_not_in_result(self):
        df = pd.DataFrame({"code": [1]})
        resolved = _resolve_columns(df, ["code", "nonexistent"])
        assert "nonexistent" not in resolved
        assert "code" in resolved


class TestFilterDelta:

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

    def test_keeps_simple_columns(self, mock_env_vars):
        """Simple columns without fallback are kept as-is."""
        with patch("commands.filter_delta.S3FileHandler") as mock_s3_cls:
            mock_s3 = Mock()
            mock_s3_cls.return_value = mock_s3
            mock_s3.download_to_memory.return_value = _df_to_parquet_bytes(SAMPLE_DF)

            handle("delta/raw.parquet", "delta/filtered.parquet", "code,product_name")

            result_df = mock_s3.upload_dataframe.call_args[0][0]
            assert list(result_df.columns) == ["code", "product_name"]
            assert "irrelevant" not in result_df.columns

    def test_fallback_column_renamed_to_target(self, mock_env_vars):
        """Fallback column is renamed to the target name."""
        with patch("commands.filter_delta.S3FileHandler") as mock_s3_cls:
            mock_s3 = Mock()
            mock_s3_cls.return_value = mock_s3
            mock_s3.download_to_memory.return_value = _df_to_parquet_bytes(SAMPLE_DF)

            handle("delta/raw.parquet", "delta/filtered.parquet",
                   "code,ecoscore_grade|environmental_score_grade")

            result_df = mock_s3.upload_dataframe.call_args[0][0]
            assert "ecoscore_grade" in result_df.columns
            assert "environmental_score_grade" not in result_df.columns
            assert list(result_df["ecoscore_grade"]) == ["a", "b"]

    def test_missing_column_filled_with_none(self, mock_env_vars):
        """Columns absent from the file (no fallback either) are included with None values."""
        with patch("commands.filter_delta.S3FileHandler") as mock_s3_cls:
            mock_s3 = Mock()
            mock_s3_cls.return_value = mock_s3
            mock_s3.download_to_memory.return_value = _df_to_parquet_bytes(SAMPLE_DF)

            handle("delta/raw.parquet", "delta/filtered.parquet", "code,nonexistent_column")

            result_df = mock_s3.upload_dataframe.call_args[0][0]
            assert "code" in result_df.columns
            assert "nonexistent_column" in result_df.columns
            assert result_df["nonexistent_column"].isna().all()

    def test_missing_env_var(self):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("delta/raw.parquet", "delta/filtered.parquet", "code")
