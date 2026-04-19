import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.filter_data import handle, _matches_country, _matches_lang


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


class TestMatchesLang:

    def test_exact_match(self):
        assert _matches_lang("fr", "fr") is True

    def test_no_match(self):
        assert _matches_lang("en", "fr") is False

    def test_case_insensitive(self):
        assert _matches_lang("FR", "fr") is True

    def test_not_a_string(self):
        assert _matches_lang(None, "fr") is False


class TestFilterData:
    """Tests for filter_data command"""

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
    def parquet_data(self):
        df = pd.DataFrame({
            "code": [1, 2, 3],
            "product_name": ["a", "b", "c"],
            "brands": ["x", "y", "z"],
            "irrelevant_column": [True, False, True],
            "lang": ["fr", "en", "fr"],
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

    def test_handle_lang_filter(self, mock_env_vars, parquet_data):
        """Test that rows are filtered by lang"""
        parquet_bytes, _ = parquet_data

        with patch("commands.filter_data.S3FileHandler") as mock_s3_cls:
            mock_s3 = Mock()
            mock_s3_cls.return_value = mock_s3
            mock_s3.download_to_memory.return_value = parquet_bytes

            handle("raw/input.parquet", "raw/filtered.parquet", "code,product_name,lang", lang="fr")

            result_df = mock_s3.upload_dataframe.call_args[0][0]
            assert len(result_df) == 2
            assert list(result_df["code"]) == [1, 3]

    def test_handle_missing_env_var(self, parquet_data):
        """Test handling of missing environment variables"""
        parquet_bytes, _ = parquet_data

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("raw/input.parquet", "raw/filtered.parquet", "code,product_name")
