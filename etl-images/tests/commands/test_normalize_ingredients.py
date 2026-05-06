import json
import os
import pytest
import numpy as np
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch, call

from commands.normalize_ingredients import (
    _stable_id,
    _id_to_name,
    _clean_text,
    _slugify,
    _parse_ingredients_value,
    _normalize_ingredient,
    _build_id_mapping,
    _flatten_tree,
    _build_ingredients_df,
    _build_product_ingredients_df,
    _build_sous_ingredients_df,
    _build_alias_df,
    handle,
)


# ===========================================================================
# HELPERS
# ===========================================================================

def _df_to_parquet_bytes(df: pd.DataFrame) -> BytesIO:
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return buf


MINIMAL_TAXONOMY = """
en:sugar, Sugars
< en:sweeteners

en:water, Drinking water

en:oat, Oats

en:wheat-flour, Wheat flour

en:salt, Salt, Sea salt

en:chocolate
"""

EMPTY_TAXONOMY = ""


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
    """DataFrame avec la colonne 'ingredients' au format arbre JSON."""
    return pd.DataFrame({
        "code": ["001", "002", "003"],
        "product_name": ["Maple Syrup", "Oat Milk", "Crackers"],
        "ingredients": [
            json.dumps([
                {"id": "en:sugar", "text": "Sugar"},
                {"id": "en:water", "text": "Water"},
            ]),
            json.dumps([
                {"id": "en:oat", "text": "Oat"},
                {"id": "en:water", "text": "Water"},
            ]),
            json.dumps([
                {
                    "id": "en:wheat-flour",
                    "text": "Wheat flour",
                    "ingredients": [
                        {"id": "en:salt", "text": "Salt"},
                    ],
                },
            ]),
        ],
    })


# ===========================================================================
# _stable_id
# ===========================================================================

class TestStableId:

    def test_same_input_same_output(self):
        assert _stable_id("en:sugar") == _stable_id("en:sugar")

    def test_different_inputs_different_outputs(self):
        assert _stable_id("en:sugar") != _stable_id("en:water")

    def test_returns_positive_int(self):
        assert _stable_id("en:sugar") > 0

    def test_within_bigint_range(self):
        assert _stable_id("en:sugar") < 2 ** 63

    def test_stable_across_calls(self):
        ids = [_stable_id("en:salt") for _ in range(5)]
        assert len(set(ids)) == 1


# ===========================================================================
# _id_to_name
# ===========================================================================

class TestIdToName:

    def test_removes_language_prefix(self):
        assert _id_to_name("en:sugar") == "sugar"

    def test_replaces_hyphens_with_spaces(self):
        assert _id_to_name("en:coconut-cream") == "coconut cream"

    def test_french_prefix(self):
        assert _id_to_name("fr:beurre-de-cacao") == "beurre de cacao"

    def test_no_prefix(self):
        assert _id_to_name("sugar") == "sugar"

    def test_additive_code(self):
        assert _id_to_name("en:e150a") == "e150a"


# ===========================================================================
# _clean_text / _slugify
# ===========================================================================

class TestCleanText:

    def test_removes_accents(self):
        assert _clean_text("café") == "cafe"

    def test_strips_html_tags(self):
        assert _clean_text("<b>Sugar</b>") == "Sugar"

    def test_replaces_underscores_with_spaces(self):
        assert _clean_text("_Soy_") == "Soy"

    def test_empty_string_returns_empty(self):
        assert _clean_text("") == ""

    def test_non_string_returns_empty(self):
        assert _clean_text(None) == ""
        assert _clean_text(42) == ""


class TestSlugify:

    def test_lowercases(self):
        assert _slugify("Sugar") == "sugar"

    def test_replaces_spaces_with_hyphens(self):
        assert _slugify("whole wheat flour") == "whole-wheat-flour"

    def test_multiple_spaces_collapsed(self):
        assert _slugify("sea  salt") == "sea-salt"


# ===========================================================================
# _parse_ingredients_value
# ===========================================================================

class TestParseIngredientsValue:

    def test_none_returns_empty(self):
        assert _parse_ingredients_value(None) == []

    def test_nan_returns_empty(self):
        assert _parse_ingredients_value(float("nan")) == []

    def test_numpy_nan_returns_empty(self):
        assert _parse_ingredients_value(np.nan) == []

    def test_empty_string_returns_empty(self):
        assert _parse_ingredients_value("") == []

    def test_list_passthrough(self):
        items = [{"id": "en:sugar", "text": "Sugar"}]
        assert _parse_ingredients_value(items) == items

    def test_valid_json_string(self):
        items = [{"id": "en:sugar", "text": "Sugar"}]
        assert _parse_ingredients_value(json.dumps(items)) == items

    def test_invalid_json_returns_empty(self):
        assert _parse_ingredients_value("not-json") == []

    def test_json_dict_returns_empty(self):
        assert _parse_ingredients_value('{"key": "value"}') == []


# ===========================================================================
# _normalize_ingredient
# ===========================================================================

class TestNormalizeIngredient:

    def test_resolves_via_taxonomy(self):
        canonical_map = {"en:sugar": "en:sugar", "en:sucre": "en:sugar"}
        result = _normalize_ingredient({"id": "en:sugar", "text": "Sugar"}, canonical_map)
        assert result["tag_id"] == "en:sugar"

    def test_fallback_to_id_when_not_in_taxonomy(self):
        result = _normalize_ingredient({"id": "en:unknown", "text": "Unknown"}, {})
        assert result["tag_id"] == "en:unknown"

    def test_fallback_to_text_when_no_id(self):
        result = _normalize_ingredient({"text": "Vanilla extract"}, {})
        assert result["tag_id"] == "en:vanilla-extract"

    def test_returns_none_for_empty_item(self):
        assert _normalize_ingredient({}, {}) is None

    def test_returns_none_for_non_dict(self):
        assert _normalize_ingredient("not-a-dict", {}) is None

    def test_raw_text_preserved(self):
        result = _normalize_ingredient({"id": "en:sugar", "text": "Raw Cane Sugar"}, {})
        assert result["raw_text"] == "Raw Cane Sugar"


# ===========================================================================
# _build_id_mapping
# ===========================================================================

class TestBuildIdMapping:

    def test_collects_tags_from_product_rows(self):
        product_rows = [{"tag_id": "en:sugar"}, {"tag_id": "en:water"}]
        mapping = _build_id_mapping(product_rows, [], [])
        assert "en:sugar" in mapping
        assert "en:water" in mapping

    def test_collects_tags_from_component_rows(self):
        component_rows = [{"parent_tag_id": "en:chocolate", "sous_tag_id": "en:cocoa"}]
        mapping = _build_id_mapping([], component_rows, [])
        assert "en:chocolate" in mapping
        assert "en:cocoa" in mapping

    def test_collects_tags_from_alias_rows(self):
        alias_rows = [{"tag_id": "en:salt"}]
        mapping = _build_id_mapping([], [], alias_rows)
        assert "en:salt" in mapping

    def test_ids_are_stable(self):
        product_rows = [{"tag_id": "en:sugar"}]
        mapping = _build_id_mapping(product_rows, [], [])
        assert mapping["en:sugar"] == _stable_id("en:sugar")

    def test_deduplicates_tags(self):
        product_rows = [{"tag_id": "en:sugar"}, {"tag_id": "en:sugar"}]
        mapping = _build_id_mapping(product_rows, [], [])
        assert len(mapping) == 1


# ===========================================================================
# _flatten_tree
# ===========================================================================

class TestFlattenTree:

    def setup_method(self):
        self.canonical_map = {
            "en:sugar": "en:sugar",
            "en:water": "en:water",
            "en:chocolate": "en:chocolate",
            "en:cocoa": "en:cocoa",
        }
        self.additive_role_map = {}

    def test_level1_goes_to_product_rows(self):
        ingredients = [{"id": "en:sugar", "text": "Sugar"}]
        product_rows, component_rows, alias_rows = [], [], []
        _flatten_tree("001", ingredients, self.canonical_map, self.additive_role_map,
                      product_rows, component_rows, alias_rows)
        assert len(product_rows) == 1
        assert product_rows[0]["code"] == "001"
        assert product_rows[0]["tag_id"] == "en:sugar"
        assert product_rows[0]["ingredient_order"] == 1

    def test_level2_goes_to_component_rows_with_code(self):
        ingredients = [
            {
                "id": "en:chocolate",
                "text": "Chocolate",
                "ingredients": [{"id": "en:cocoa", "text": "Cocoa"}],
            }
        ]
        product_rows, component_rows, alias_rows = [], [], []
        _flatten_tree("001", ingredients, self.canonical_map, self.additive_role_map,
                      product_rows, component_rows, alias_rows)
        assert len(component_rows) == 1
        assert component_rows[0]["code"] == "001"
        assert component_rows[0]["parent_tag_id"] == "en:chocolate"
        assert component_rows[0]["sous_tag_id"] == "en:cocoa"
        assert component_rows[0]["rang"] == 1

    def test_ingredient_order_increments(self):
        ingredients = [
            {"id": "en:sugar", "text": "Sugar"},
            {"id": "en:water", "text": "Water"},
        ]
        product_rows, component_rows, alias_rows = [], [], []
        _flatten_tree("001", ingredients, self.canonical_map, self.additive_role_map,
                      product_rows, component_rows, alias_rows)
        orders = [r["ingredient_order"] for r in product_rows]
        assert orders == [1, 2]

    def test_alias_created_when_text_differs_from_name(self):
        ingredients = [{"id": "en:sugar", "text": "Raw Cane Sugar"}]
        product_rows, component_rows, alias_rows = [], [], []
        _flatten_tree("001", ingredients, self.canonical_map, self.additive_role_map,
                      product_rows, component_rows, alias_rows)
        assert len(alias_rows) == 1
        assert alias_rows[0]["alias_name"] == "raw cane sugar"

    def test_no_alias_when_text_matches_name(self):
        ingredients = [{"id": "en:sugar", "text": "sugar"}]
        product_rows, component_rows, alias_rows = [], [], []
        _flatten_tree("001", ingredients, self.canonical_map, self.additive_role_map,
                      product_rows, component_rows, alias_rows)
        assert len(alias_rows) == 0

    def test_empty_ingredients_produces_no_rows(self):
        product_rows, component_rows, alias_rows = [], [], []
        _flatten_tree("001", [], self.canonical_map, self.additive_role_map,
                      product_rows, component_rows, alias_rows)
        assert product_rows == []
        assert component_rows == []


# ===========================================================================
# _build_ingredients_df
# ===========================================================================

class TestBuildIngredientsDF:

    def test_correct_columns(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        df = _build_ingredients_df(tag_to_id)
        assert list(df.columns) == ["ingredient_id", "ingredient_name"]

    def test_ingredient_name_derived_from_tag(self):
        tag_to_id = {"en:coconut-cream": _stable_id("en:coconut-cream")}
        df = _build_ingredients_df(tag_to_id)
        assert df.iloc[0]["ingredient_name"] == "coconut cream"

    def test_ingredient_id_type_is_int64(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        df = _build_ingredients_df(tag_to_id)
        assert df["ingredient_id"].dtype == pd.Int64Dtype()

    def test_empty_mapping_returns_empty_df(self):
        df = _build_ingredients_df({})
        assert len(df) == 0
        assert "ingredient_id" in df.columns


# ===========================================================================
# _build_product_ingredients_df
# ===========================================================================

class TestBuildProductIngredientsDF:

    def test_correct_columns(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        rows = [{"code": "001", "tag_id": "en:sugar", "ingredient_order": 1, "role": None}]
        df = _build_product_ingredients_df(rows, tag_to_id)
        assert list(df.columns) == ["code", "ingredient_id", "ingredient_name", "ingredient_order", "role"]

    def test_ingredient_name_included(self):
        tag_to_id = {"en:coconut-cream": _stable_id("en:coconut-cream")}
        rows = [{"code": "001", "tag_id": "en:coconut-cream", "ingredient_order": 1, "role": None}]
        df = _build_product_ingredients_df(rows, tag_to_id)
        assert df.iloc[0]["ingredient_name"] == "coconut cream"

    def test_fk_integrity(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar"), "en:water": _stable_id("en:water")}
        rows = [
            {"code": "001", "tag_id": "en:sugar", "ingredient_order": 1, "role": None},
            {"code": "001", "tag_id": "en:water", "ingredient_order": 2, "role": None},
        ]
        df = _build_product_ingredients_df(rows, tag_to_id)
        assert set(df["ingredient_id"]).issubset(set(tag_to_id.values()))

    def test_unknown_tag_excluded(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        rows = [{"code": "001", "tag_id": "en:unknown", "ingredient_order": 1, "role": None}]
        df = _build_product_ingredients_df(rows, tag_to_id)
        assert len(df) == 0

    def test_ingredient_id_type_is_int64(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        rows = [{"code": "001", "tag_id": "en:sugar", "ingredient_order": 1, "role": None}]
        df = _build_product_ingredients_df(rows, tag_to_id)
        assert df["ingredient_id"].dtype == pd.Int64Dtype()


# ===========================================================================
# _build_sous_ingredients_df
# ===========================================================================

class TestBuildSousIngredientsDF:

    def test_correct_columns(self):
        tag_to_id = {
            "en:chocolate": _stable_id("en:chocolate"),
            "en:cocoa": _stable_id("en:cocoa"),
        }
        rows = [{"code": "001", "parent_tag_id": "en:chocolate", "sous_tag_id": "en:cocoa", "rang": 1}]
        df = _build_sous_ingredients_df(rows, tag_to_id)
        assert list(df.columns) == ["code", "ingredient_id", "sous_ingredient_id", "sous_ingredient_name", "rang"]

    def test_code_column_present(self):
        tag_to_id = {
            "en:chocolate": _stable_id("en:chocolate"),
            "en:cocoa": _stable_id("en:cocoa"),
        }
        rows = [{"code": "001", "parent_tag_id": "en:chocolate", "sous_tag_id": "en:cocoa", "rang": 1}]
        df = _build_sous_ingredients_df(rows, tag_to_id)
        assert df.iloc[0]["code"] == "001"

    def test_sous_ingredient_name_derived_from_tag(self):
        tag_to_id = {
            "en:chocolate": _stable_id("en:chocolate"),
            "en:cocoa-butter": _stable_id("en:cocoa-butter"),
        }
        rows = [{"code": "001", "parent_tag_id": "en:chocolate", "sous_tag_id": "en:cocoa-butter", "rang": 1}]
        df = _build_sous_ingredients_df(rows, tag_to_id)
        assert df.iloc[0]["sous_ingredient_name"] == "cocoa butter"

    def test_excludes_rows_with_unknown_tags(self):
        tag_to_id = {"en:chocolate": _stable_id("en:chocolate")}
        rows = [{"code": "001", "parent_tag_id": "en:chocolate", "sous_tag_id": "en:unknown", "rang": 1}]
        df = _build_sous_ingredients_df(rows, tag_to_id)
        assert len(df) == 0

    def test_id_types_are_int64(self):
        tag_to_id = {
            "en:chocolate": _stable_id("en:chocolate"),
            "en:cocoa": _stable_id("en:cocoa"),
        }
        rows = [{"code": "001", "parent_tag_id": "en:chocolate", "sous_tag_id": "en:cocoa", "rang": 1}]
        df = _build_sous_ingredients_df(rows, tag_to_id)
        assert df["ingredient_id"].dtype == pd.Int64Dtype()
        assert df["sous_ingredient_id"].dtype == pd.Int64Dtype()


# ===========================================================================
# _build_alias_df
# ===========================================================================

class TestBuildAliasDF:

    def test_correct_columns(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        rows = [{"tag_id": "en:sugar", "alias_name": "raw cane sugar"}]
        df = _build_alias_df(rows, tag_to_id)
        assert list(df.columns) == ["ingredient_id", "alias_name"]

    def test_alias_name_preserved(self):
        tag_to_id = {"en:sugar": _stable_id("en:sugar")}
        rows = [{"tag_id": "en:sugar", "alias_name": "raw cane sugar"}]
        df = _build_alias_df(rows, tag_to_id)
        assert df.iloc[0]["alias_name"] == "raw cane sugar"

    def test_unknown_tag_excluded(self):
        rows = [{"tag_id": "en:unknown", "alias_name": "something"}]
        df = _build_alias_df(rows, {})
        assert len(df) == 0


# ===========================================================================
# handle — tests d'intégration
# ===========================================================================

class TestHandle:

    def _make_mock_s3(self, df):
        mock_s3_instance = Mock()
        mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(df)
        return mock_s3_instance

    def test_four_parquets_uploaded(self, mock_env_vars, sample_df):
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(sample_df)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            assert mock_s3.return_value.upload_dataframe.call_count == 4
            keys = [c[0][1] for c in mock_s3.return_value.upload_dataframe.call_args_list]
            assert "ingredients.parquet" in keys
            assert "product_ingredients.parquet" in keys
            assert "sous_ingredients.parquet" in keys
            assert "ingredient_alias.parquet" in keys

    def test_product_ingredients_fk_integrity(self, mock_env_vars, sample_df):
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(sample_df)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            uploaded = {c[0][1]: c[0][0] for c in mock_s3.return_value.upload_dataframe.call_args_list}
            ing_ids = set(uploaded["ingredients.parquet"]["ingredient_id"])
            prod_ids = set(uploaded["product_ingredients.parquet"]["ingredient_id"])
            assert prod_ids.issubset(ing_ids)

    def test_sous_ingredients_has_code_column(self, mock_env_vars, sample_df):
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(sample_df)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            uploaded = {c[0][1]: c[0][0] for c in mock_s3.return_value.upload_dataframe.call_args_list}
            sous_df = uploaded["sous_ingredients.parquet"]
            assert "code" in sous_df.columns

    def test_product_ingredients_has_ingredient_name_column(self, mock_env_vars, sample_df):
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(sample_df)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            uploaded = {c[0][1]: c[0][0] for c in mock_s3.return_value.upload_dataframe.call_args_list}
            prod_df = uploaded["product_ingredients.parquet"]
            assert "ingredient_name" in prod_df.columns

    def test_missing_ingredients_column_uploads_empty_dfs(self, mock_env_vars):
        df_no_col = pd.DataFrame({"code": ["001"], "product_name": ["X"]})
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(df_no_col)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            assert mock_s3.return_value.upload_dataframe.call_count == 4
            uploaded = {c[0][1]: c[0][0] for c in mock_s3.return_value.upload_dataframe.call_args_list}
            assert len(uploaded["ingredients.parquet"]) == 0
            assert len(uploaded["product_ingredients.parquet"]) == 0
            assert len(uploaded["sous_ingredients.parquet"]) == 0
            assert len(uploaded["ingredient_alias.parquet"]) == 0

    def test_missing_env_var_raises(self, sample_df):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError, match="S3_BUCKET"):
                handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                       "sous_ingredients.parquet", "ingredient_alias.parquet")

    def test_ids_are_stable_across_runs(self, mock_env_vars, sample_df):
        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(sample_df)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            uploaded_1 = {c[0][1]: c[0][0] for c in mock_s3.return_value.upload_dataframe.call_args_list}

        with patch("commands.normalize_ingredients.S3FileHandler") as mock_s3, \
             patch("commands.normalize_ingredients._download_taxonomy", return_value=MINIMAL_TAXONOMY):
            mock_s3.return_value = self._make_mock_s3(sample_df)
            handle("input.parquet", "ingredients.parquet", "product_ingredients.parquet",
                   "sous_ingredients.parquet", "ingredient_alias.parquet")
            uploaded_2 = {c[0][1]: c[0][0] for c in mock_s3.return_value.upload_dataframe.call_args_list}

        ids_1 = set(uploaded_1["ingredients.parquet"]["ingredient_id"])
        ids_2 = set(uploaded_2["ingredients.parquet"]["ingredient_id"])
        assert ids_1 == ids_2
