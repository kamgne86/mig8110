import os
import pytest
import numpy as np
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.normalize_ingredients import (
    _to_list,
    _parse_taxonomy,
    _normalize_tags,
    _build_ingredients_table,
    _stable_id,
    handle,
)


def _df_to_parquet_bytes(df):
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf


@pytest.fixture
def mock_env_vars():
    env_vars = {
        "S3_BUCKET":     "test-bucket",
        "S3_ENDPOINT":   "https://s3.example.com",
        "S3_ACCESS_KEY": "test-key",
        "S3_SECRET_KEY": "test-secret",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def sample_df():
    """DataFrame with ingredients_tags as real Python lists."""
    return pd.DataFrame({
        "code":             ["001", "002", "003"],
        "product_name":     ["Maple Syrup", "Oat Milk", "Crackers"],
        "ingredients_tags": [
            ["en:sugar", "en:water"],
            ["en:oat", "en:water"],
            ["en:wheat-flour", "en:salt"],
        ],
    })


@pytest.fixture
def taxonomy_text():
    """Extrait minimal de ingredients.txt pour les tests."""
    return """
en:sugar, Sugars
< en:sweeteners

en:water, Drinking water

en:oat, Oats

en:wheat-flour, Wheat flour

en:salt, Salt, Sea salt
"""


# ---------------------------------------------------------------------------
# Tests _to_list
# ---------------------------------------------------------------------------

class TestToList:

    def test_none_returns_empty(self):
        assert _to_list(None) == []

    def test_nan_returns_empty(self):
        assert _to_list(float("nan")) == []

    def test_numpy_nan_returns_empty(self):
        assert _to_list(np.nan) == []

    def test_empty_string_returns_empty(self):
        assert _to_list("") == []

    def test_empty_list_string_returns_empty(self):
        assert _to_list("[]") == []

    def test_string_repr_of_list(self):
        assert _to_list("['en:sugar', 'en:water']") == ["en:sugar", "en:water"]

    def test_real_list_passthrough(self):
        tags = ["en:sugar", "en:water"]
        assert _to_list(tags) == tags

    def test_invalid_string_returns_empty(self):
        assert _to_list("not-a-list") == []

    def test_numpy_array(self):
        arr = np.array(["en:sugar", "en:water"])
        assert _to_list(arr) == ["en:sugar", "en:water"]


# ---------------------------------------------------------------------------
# Tests _parse_taxonomy
# ---------------------------------------------------------------------------

class TestParseTaxonomy:

    def test_canonical_tag_in_map(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        assert "en:sugar" in canonical_map
        assert canonical_map["en:sugar"] == "en:sugar"

    def test_synonym_maps_to_canonical(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        assert canonical_map.get("en:sea-salt") == "en:salt"

    def test_empty_text_returns_empty_dict(self):
        canonical_map = _parse_taxonomy("")
        assert canonical_map == {}


# ---------------------------------------------------------------------------
# Tests _normalize_tags
# ---------------------------------------------------------------------------

class TestNormalizeTags:

    def test_known_tag_mapped_to_canonical(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags(["en:sugar"], canonical_map)
        assert result == ["en:sugar"]

    def test_unknown_tag_kept_as_is(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags(["en:unknown-ingredient"], canonical_map)
        assert result == ["en:unknown-ingredient"]

    def test_duplicates_removed(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags(["en:salt", "en:salt"], canonical_map)
        assert result == ["en:salt"]

    def test_none_returns_empty(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        assert _normalize_tags(None, canonical_map) == []

    def test_unknown_tag_logs_warning(self, taxonomy_text):
        canonical_map = _parse_taxonomy(taxonomy_text)
        with patch("commands.normalize_ingredients.logger") as mock_logger:
            _normalize_tags(["en:unknown-ingredient"], canonical_map)
            mock_logger.warning.assert_called_once()


# ---------------------------------------------------------------------------
# Tests _build_ingredients_table
# ---------------------------------------------------------------------------

class TestBuildIngredientsTable:

    def test_ingredient_names_are_unique(self):
        df, _ = _build_ingredients_table({"en:sugar", "en:water", "en:salt"})
        assert df["ingredient_name"].nunique() == len(df)

    def test_ingredient_id_is_stable_hash(self):
        df, _ = _build_ingredients_table({"en:sugar"})
        row = df[df["ingredient_name"] == "en:sugar"].iloc[0]
        assert row["ingredient_id"] == _stable_id("en:sugar")

    def test_same_name_same_id(self):
        _, tag_to_id_1 = _build_ingredients_table({"en:sugar", "en:water"})
        _, tag_to_id_2 = _build_ingredients_table({"en:sugar"})
        assert tag_to_id_1["en:sugar"] == tag_to_id_2["en:sugar"]

    def test_tag_to_id_returned(self):
        _, tag_to_id = _build_ingredients_table({"en:sugar", "en:water"})
        assert "en:sugar" in tag_to_id
        assert tag_to_id["en:sugar"] == _stable_id("en:sugar")

    def test_empty_tags_returns_empty_df_with_columns(self):
        df, tag_to_id = _build_ingredients_table(set())
        assert len(df) == 0
        assert list(df.columns) == ["ingredient_id", "ingredient_name"]
        assert tag_to_id == {}

    def test_df_sorted_by_name(self):
        df, _ = _build_ingredients_table({"en:water", "en:salt", "en:oat"})
        names = list(df["ingredient_name"])
        assert names == sorted(names)


# ---------------------------------------------------------------------------
# Tests handle
# ---------------------------------------------------------------------------

class TestHandle:

    def test_two_parquets_uploaded(self, mock_env_vars, sample_df):
        """handle() doit uploader exactement 2 parquets (ingredients + product_ingredients).
        La table products est produite par finalize_products."""
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_ingredients_txt") as mock_dl:
            mock_dl.return_value = """
en:sugar
en:water
en:oat
en:wheat-flour
en:salt
"""
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet")

            assert mock_s3_instance.upload_dataframe.call_count == 2
            keys = [call[0][1] for call in mock_s3_instance.upload_dataframe.call_args_list]
            assert "ingredients.parquet" in keys
            assert "product_ingredients.parquet" in keys

    def test_product_ingredients_fk_integrity(self, mock_env_vars, sample_df):
        """Tous les ingredient_id dans product_ingredients existent dans ingredients."""
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_ingredients_txt") as mock_dl:
            mock_dl.return_value = """
en:sugar
en:water
en:oat
en:wheat-flour
en:salt
"""
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet")

            uploaded = {call[0][1]: call[0][0] for call in mock_s3_instance.upload_dataframe.call_args_list}
            ing_ids = set(uploaded["ingredients.parquet"]["ingredient_id"])
            junc_ids = set(uploaded["product_ingredients.parquet"]["ingredient_id"])
            assert junc_ids.issubset(ing_ids)

    def test_missing_env_var_raises(self, sample_df):
        """KeyError si les variables S3 sont absentes."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError, match="S3_BUCKET"):
                handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet")

    def test_missing_ingredients_tags_column(self, mock_env_vars):
        """Si ingredients_tags est absente, 2 parquets vides sont quand même uploadés."""
        df_without_col = pd.DataFrame({"code": ["001"], "product_name": ["X"]})
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_ingredients_txt"):
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(df_without_col)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet")
            assert mock_s3_instance.upload_dataframe.call_count == 2
            uploaded = {call[0][1]: call[0][0] for call in mock_s3_instance.upload_dataframe.call_args_list}
            assert list(uploaded["ingredients.parquet"].columns) == ["ingredient_id", "ingredient_name"]
            assert list(uploaded["product_ingredients.parquet"].columns) == ["code", "ingredient_id"]
