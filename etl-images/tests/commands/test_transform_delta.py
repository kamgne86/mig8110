import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.transform_delta import (
    handle,
    _build_code_path,
    _extract_image_url,
    _extract_nutriment,
)


def _df_to_jsonl_bytes(df):
    buf = BytesIO()
    buf.write(df.to_json(orient="records", lines=True, force_ascii=False).encode("utf-8"))
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
    """DataFrame mimicking the delta JSONL format as read by pd.read_json(lines=True).

    images and nutriments are native Python dicts — no serialization needed
    since the bronze layer stores raw JSONL.
    """
    return pd.DataFrame({
        "code": ["3017620422003"],
        "product_name": ["Nutella"],
        "brands": ["Ferrero"],
        "nutriscore_grade": ["e"],
        "ecoscore_grade":   ["b"],
        "images": [{
            "selected": {
                "front":        {"en": {"rev": "42"}},
                "ingredients":  {"en": {"rev": "10"}},
                "nutrition":    {"en": {"rev": "8"}},
                "packaging":    {"en": {"rev": "5"}},
            }
        }],
        "nutrition": [{
            "aggregated_set": {
                "nutrients": {
                    "energy-kcal":   {"value": 539.0},
                    "fat":           {"value": 30.9},
                    "saturated-fat": {"value": 10.6},
                    "sugars":        {"value": 56.3},
                    "proteins":      {"value": 6.3},
                    "salt":          {"value": 0.107},
                }
            }
        }],
    })


class TestBuildCodePath:

    def test_pads_short_code(self):
        assert _build_code_path("12345") == "000/000/001/2345"

    def test_splits_13_char_code(self):
        assert _build_code_path("3017620422003") == "301/762/042/2003"


class TestExtractImageUrl:

    def _make_images(self, selected_key, rev):
        return {"selected": {selected_key: {"en": {"rev": rev}}}}

    def test_builds_correct_url(self):
        images = self._make_images("front", "42")
        url = _extract_image_url(images, "3017620422003", "front", "front_en")
        assert url == "https://images.openfoodfacts.org/images/products/301/762/042/2003/front_en.42.400.jpg"

    def test_strips_quoted_rev(self):
        """Rev values like '"7"' (with surrounding quotes) must be stripped."""
        images = self._make_images("front", '"7"')
        url = _extract_image_url(images, "3017620422003", "front", "front_en")
        assert url == "https://images.openfoodfacts.org/images/products/301/762/042/2003/front_en.7.400.jpg"

    def test_returns_none_when_selected_key_missing(self):
        images = self._make_images("front", "42")
        assert _extract_image_url(images, "3017620422003", "nutrition", "nutrition_en") is None

    def test_returns_none_for_null_rev(self):
        images = {"selected": {"front": {"en": {"rev": None}}}}
        assert _extract_image_url(images, "3017620422003", "front", "front_en") is None

    def test_returns_none_for_null_images(self):
        assert _extract_image_url(None, "3017620422003", "front", "front_en") is None


class TestExtractNutriment:

    def test_extracts_correct_value(self):
        nutrition = {"aggregated_set": {"nutrients": {"energy-kcal": {"value": 539.0}, "fat": {"value": 30.9}}}}
        assert _extract_nutriment(nutrition, "energy-kcal") == 539.0

    def test_rounds_to_2_decimals(self):
        nutrition = {"aggregated_set": {"nutrients": {"fat": {"value": 30.9456789}}}}
        assert _extract_nutriment(nutrition, "fat") == 30.95

    def test_returns_none_when_nutriment_missing(self):
        nutrition = {"aggregated_set": {"nutrients": {"fat": {"value": 30.9}}}}
        assert _extract_nutriment(nutrition, "energy-kcal") is None

    def test_returns_none_for_null(self):
        assert _extract_nutriment(None, "energy-kcal") is None


class TestTransformDelta:

    def test_product_name_is_preserved(self, mock_env_vars, sample_df):
        """product_name is already a VARCHAR in delta — must be left as-is."""
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(sample_df)
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("delta_valid.jsonl", "delta_transformed.parquet")

        df = _parquet_bytes_to_df(BytesIO(uploaded["delta_transformed.parquet"]))
        assert df["product_name"].iloc[0] == "Nutella"

    def test_image_urls_are_built(self, mock_env_vars, sample_df):
        """Image URL columns are built from images.selected.{type}.en.rev."""
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(sample_df)
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("delta_valid.jsonl", "delta_transformed.parquet")

        df = _parquet_bytes_to_df(BytesIO(uploaded["delta_transformed.parquet"]))
        assert df["front_url"].iloc[0] == "https://images.openfoodfacts.org/images/products/301/762/042/2003/front_en.42.400.jpg"
        assert df["nutrition_url"].iloc[0] == "https://images.openfoodfacts.org/images/products/301/762/042/2003/nutrition_en.8.400.jpg"

    def test_nutriments_are_extracted(self, mock_env_vars, sample_df):
        """Nutriment columns are populated from nutrition.aggregated_set.nutrients."""
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(sample_df)
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("delta_valid.jsonl", "delta_transformed.parquet")

        df = _parquet_bytes_to_df(BytesIO(uploaded["delta_transformed.parquet"]))
        assert df["energy_kcal_100g"].iloc[0] == 539.0
        assert df["fat_100g"].iloc[0] == 30.9

    def test_invalid_nutriscore_grade_set_to_null(self, mock_env_vars):
        """nutriscore_grade values outside the valid whitelist are replaced by None."""
        df = pd.DataFrame({
            "code": ["1", "2", "3"],
            "product_name": ["P", "P", "P"],
            "nutriscore_grade": ["a", "unknown", "not-applicable"],
            "ecoscore_grade":   ["b", "b", "b"],
            "images":     [None, None, None],
            "nutrition": [None, None, None],
        })
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(df)
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("delta_valid.jsonl", "delta_transformed.parquet")

        result = _parquet_bytes_to_df(BytesIO(uploaded["delta_transformed.parquet"]))
        assert result["nutriscore_grade"].iloc[0] == "a"
        assert result["nutriscore_grade"].iloc[1] is None or pd.isna(result["nutriscore_grade"].iloc[1])
        assert result["nutriscore_grade"].iloc[2] is None or pd.isna(result["nutriscore_grade"].iloc[2])

    def test_invalid_ecoscore_grade_set_to_null(self, mock_env_vars):
        """ecoscore_grade values outside the valid whitelist are replaced by None."""
        df = pd.DataFrame({
            "code": ["1", "2", "3"],
            "product_name": ["P", "P", "P"],
            "nutriscore_grade": ["a", "a", "a"],
            "ecoscore_grade":   ["a-plus", "unknown", "not-applicable"],
            "images":     [None, None, None],
            "nutrition": [None, None, None],
        })
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(df)
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("delta_valid.jsonl", "delta_transformed.parquet")

        result = _parquet_bytes_to_df(BytesIO(uploaded["delta_transformed.parquet"]))
        assert result["ecoscore_grade"].iloc[0] == "a-plus"
        assert result["ecoscore_grade"].iloc[1] is None or pd.isna(result["ecoscore_grade"].iloc[1])
        assert result["ecoscore_grade"].iloc[2] is None or pd.isna(result["ecoscore_grade"].iloc[2])

    def test_output_matches_target_columns(self, mock_env_vars, sample_df):
        """Output DataFrame must contain exactly the TARGET_COLUMNS."""
        from config.target_columns import TARGET_COLUMNS
        uploaded = {}

        def capture_upload(buf, key):
            uploaded[key] = buf.read()

        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(sample_df)
            mock_s3_instance.upload_from_memory.side_effect = capture_upload

            handle("delta_valid.jsonl", "delta_transformed.parquet")

        df = _parquet_bytes_to_df(BytesIO(uploaded["delta_transformed.parquet"]))
        assert list(df.columns) == TARGET_COLUMNS

    def test_output_is_uploaded_once(self, mock_env_vars, sample_df):
        """Exactly one file is uploaded (the transformed parquet)."""
        with patch("commands.transform_delta.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_jsonl_bytes(sample_df)

            handle("delta_valid.jsonl", "delta_transformed.parquet")

            mock_s3_instance.upload_from_memory.assert_called_once()
            assert mock_s3_instance.upload_from_memory.call_args[0][1] == "delta_transformed.parquet"

    def test_missing_env_var(self, sample_df):
        """KeyError is raised when S3 environment variables are missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("delta_valid.jsonl", "delta_transformed.parquet")
