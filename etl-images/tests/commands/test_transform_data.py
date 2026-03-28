import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.transform_data import (
    handle,
    _extract_product_name,
    _build_code_path,
    _extract_image_url,
    _extract_nutriment,
)


def _df_to_parquet_bytes(df):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf


def _parquet_bytes_to_df(buf):
    buf.seek(0)
    return pd.read_parquet(buf)


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
    """DataFrame with one valid record containing all complex columns."""
    return pd.DataFrame({
        "code": ["3017620422003"],
        "product_name": [[
            {"lang": "fr",   "text": "Nutella français"},
            {"lang": "main", "text": "Nutella"},
        ]],
        "brands": ["Ferrero"],
        "nutriscore_grade": ["e"],
        "ecoscore_grade":   ["b"],
        "images": [[
            {"key": "front_en",       "rev": 42},
            {"key": "ingredients_en", "rev": 10},
            {"key": "nutrition_en",   "rev": 8},
            {"key": "packaging_en",   "rev": 5},
        ]],
        "nutriments": [[
            {"name": "energy-kcal",   "100g": 539.0},
            {"name": "fat",           "100g": 30.9},
            {"name": "saturated-fat", "100g": 10.6},
            {"name": "sugars",        "100g": 56.3},
            {"name": "proteins",      "100g": 6.3},
            {"name": "salt",          "100g": 0.107},
        ]],
    })


class TestExtractProductName:

    def test_extracts_main_language(self):
        lst = [{"lang": "fr", "text": "Nutella fr"}, {"lang": "main", "text": "Nutella"}]
        assert _extract_product_name(lst) == "Nutella"

    def test_returns_none_when_no_main(self):
        lst = [{"lang": "fr", "text": "Nutella fr"}]
        assert _extract_product_name(lst) is None

    def test_returns_none_for_null(self):
        assert _extract_product_name(None) is None

    def test_returns_none_for_empty_list(self):
        assert _extract_product_name([]) is None


class TestBuildCodePath:

    def test_pads_short_code(self):
        assert _build_code_path("12345") == "000/000/001/2345"

    def test_splits_13_char_code(self):
        assert _build_code_path("3017620422003") == "301/762/042/2003"


class TestExtractImageUrl:

    def test_builds_correct_url(self):
        images = [{"key": "front_en", "rev": 42}]
        url = _extract_image_url(images, "3017620422003", "front_en")
        assert url == "https://images.openfoodfacts.org/images/products/301/762/042/2003/front_en.42.400.jpg"

    def test_returns_none_when_key_missing(self):
        images = [{"key": "front_en", "rev": 42}]
        assert _extract_image_url(images, "3017620422003", "nutrition_en") is None

    def test_returns_none_for_null_images(self):
        assert _extract_image_url(None, "3017620422003", "front_en") is None


class TestExtractNutriment:

    def test_extracts_correct_value(self):
        lst = [{"name": "fat", "100g": 30.9123}, {"name": "energy-kcal", "100g": 539.456}]
        assert _extract_nutriment(lst, "energy-kcal") == 539.46
        assert _extract_nutriment(lst, "fat") == 30.91

    def test_returns_none_when_nutriment_missing(self):
        lst = [{"name": "fat", "100g": 30.9}]
        assert _extract_nutriment(lst, "energy-kcal") is None

    def test_returns_none_for_null(self):
        assert _extract_nutriment(None, "energy-kcal") is None


class TestTransformData:

    def test_product_name_is_extracted(self, mock_env_vars, sample_df):
        """product_name list is replaced by the 'main' text."""
        with patch("commands.transform_data.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data_valid.parquet", "data_transformed.parquet")

            df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert df["product_name"].iloc[0] == "Nutella"

    def test_image_urls_are_built(self, mock_env_vars, sample_df):
        """Image URL columns are added from images list."""
        with patch("commands.transform_data.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data_valid.parquet", "data_transformed.parquet")

            df = mock_s3_instance.upload_dataframe.call_args[0][0]
            expected = "https://images.openfoodfacts.org/images/products/301/762/042/2003/front_en.42.400.jpg"
            assert df["front_url"].iloc[0] == expected

    def test_nutriments_are_extracted(self, mock_env_vars, sample_df):
        """Nutriment columns are added from nutriments list."""
        with patch("commands.transform_data.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data_valid.parquet", "data_transformed.parquet")

            df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert df["energy_kcal_100g"].iloc[0] == 539.0
            assert df["fat_100g"].iloc[0] == 30.9

    def test_output_is_uploaded_once(self, mock_env_vars, sample_df):
        """Exactly one file is uploaded."""
        with patch("commands.transform_data.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("data_valid.parquet", "data_transformed.parquet")

            mock_s3_instance.upload_dataframe.assert_called_once()
            assert mock_s3_instance.upload_dataframe.call_args[0][1] == "data_transformed.parquet"

    def test_invalid_nutriscore_grade_set_to_null(self, mock_env_vars):
        """nutriscore_grade values outside the valid whitelist are replaced by None."""
        df = pd.DataFrame({
            "code": ["111", "222", "333"],
            "product_name": [[{"lang": "main", "text": "P"}]] * 3,
            "nutriscore_grade": ["a", "unknown", "not-applicable"],
            "ecoscore_grade":   ["b", "b",       "b"],
            "images":     [[], [], []],
            "nutriments": [[], [], []],
        })

        with patch("commands.transform_data.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(df)

            handle("data_valid.parquet", "data_transformed.parquet")

            result = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert result["nutriscore_grade"].iloc[0] == "a"
            assert result["nutriscore_grade"].iloc[1] is None or pd.isna(result["nutriscore_grade"].iloc[1])
            assert result["nutriscore_grade"].iloc[2] is None or pd.isna(result["nutriscore_grade"].iloc[2])

    def test_invalid_ecoscore_grade_set_to_null(self, mock_env_vars):
        """ecoscore_grade values outside the valid whitelist are replaced by None.
        La valeur 'a-plus' est valide et doit être conservée."""
        df = pd.DataFrame({
            "code": ["111", "222", "333"],
            "product_name": [[{"lang": "main", "text": "P"}]] * 3,
            "nutriscore_grade": ["a", "a",       "a"],
            "ecoscore_grade":   ["a-plus", "unknown", "not-applicable"],
            "images":     [[], [], []],
            "nutriments": [[], [], []],
        })

        with patch("commands.transform_data.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(df)

            handle("data_valid.parquet", "data_transformed.parquet")

            result = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert result["ecoscore_grade"].iloc[0] == "a-plus"
            assert result["ecoscore_grade"].iloc[1] is None or pd.isna(result["ecoscore_grade"].iloc[1])
            assert result["ecoscore_grade"].iloc[2] is None or pd.isna(result["ecoscore_grade"].iloc[2])

    def test_missing_env_var(self, sample_df):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("data_valid.parquet", "data_transformed.parquet")
