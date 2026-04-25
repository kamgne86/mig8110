import os
import pytest
import numpy as np
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.normalize_categories import (
    _to_list,
    _parse_taxonomy,
    _normalize_tags,
    _build_categories_table,
    _build_ancetre_categories,
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
    """DataFrame with categories_tags as real Python lists."""
    return pd.DataFrame({
        "code":          ["001", "002", "003"],
        "product_name":  ["Maple Syrup", "Oat Milk", "Crackers"],
        "categories_tags": [
            ["en:sweeteners", "en:syrups", "en:maple-syrups"],
            ["en:plant-based-beverages", "en:beverages"],
            ["en:snacks", "en:salty-snacks"],
        ],
    })


@pytest.fixture
def taxonomy_text():
    """Extrait minimal de categories.txt pour les tests."""
    return """
en:beverages
< en:food

en:plant-based-beverages, Plant-based drinks
< en:beverages

en:sweeteners
< en:food

en:syrups
< en:sweeteners

en:maple-syrups, Maple syrup, Pure maple syrup
< en:syrups

en:snacks
< en:food

en:salty-snacks
< en:snacks

en:food
"""


@pytest.fixture
def taxonomy_text_multi_parent():
    """Taxonomie avec plusieurs lignes < pour tester la sélection du bon parent.

    en:simple-syrups a deux parents dans categories.txt :
        < en:sweeteners  (premier  — plus général)
        < en:syrups      (dernier  — plus spécifique → doit être retenu)

    Ce cas reproduit exactement le bug trouvé en production où
    en:maple-syrups ne remontait pas jusqu'à en:syrups.
    """
    return """
en:food

en:sweeteners
< en:food

en:syrups
< en:sweeteners

en:simple-syrups
< en:sweeteners
< en:syrups

en:maple-syrups
< en:simple-syrups
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
        assert _to_list("['en:syrups', 'en:sweeteners']") == ["en:syrups", "en:sweeteners"]

    def test_real_list_passthrough(self):
        tags = ["en:syrups", "en:sweeteners"]
        assert _to_list(tags) == tags

    def test_invalid_string_returns_empty(self):
        assert _to_list("not-a-list") == []

    def test_numpy_array(self):
        arr = np.array(["en:syrups", "en:sweeteners"])
        assert _to_list(arr) == ["en:syrups", "en:sweeteners"]


# ---------------------------------------------------------------------------
# Tests _parse_taxonomy
# ---------------------------------------------------------------------------

class TestParseTaxonomy:

    def test_canonical_map_contains_canonical_tag(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        assert "en:maple-syrups" in canonical_map
        assert canonical_map["en:maple-syrups"] == "en:maple-syrups"

    def test_synonym_maps_to_canonical(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        assert canonical_map.get("en:pure-maple-syrup") == "en:maple-syrups"

    def test_parent_map_contains_parent(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        assert parent_map["en:maple-syrups"] == "en:syrups"
        assert parent_map["en:syrups"] == "en:sweeteners"

    def test_root_category_has_no_parent(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        assert parent_map.get("en:food") is None

    def test_empty_text_returns_empty_dicts(self):
        canonical_map, parent_map = _parse_taxonomy("")
        assert canonical_map == {}
        assert parent_map == {}

    def test_multi_parent_keeps_last_parent(self, taxonomy_text_multi_parent):
        """Quand une catégorie a plusieurs lignes <, le dernier parent doit être retenu.

        En:simple-syrups a < en:sweeteners puis < en:syrups dans categories.txt.
        Le dernier (en:syrups) est le plus spécifique et doit être le parent retenu
        pour permettre la remontée correcte de la hiérarchie.
        """
        _, parent_map = _parse_taxonomy(taxonomy_text_multi_parent)
        assert parent_map["en:simple-syrups"] == "en:syrups"

    def test_parent_tag_spaces_normalized_to_hyphens(self):
        """Les espaces dans les lignes < doivent être convertis en tirets.

        categories.txt peut contenir '< en:Simple syrups' (avec espace).
        Sans normalisation, 'en:simple syrups' != 'en:simple-syrups' → chaîne cassée.
        """
        text = """
en:sweeteners

en:simple syrups
< en:sweeteners

en:maple-syrups
< en:Simple syrups
"""
        _, parent_map = _parse_taxonomy(text)
        assert parent_map.get("en:maple-syrups") == "en:simple-syrups"


# ---------------------------------------------------------------------------
# Tests _normalize_tags
# ---------------------------------------------------------------------------

class TestNormalizeTags:

    def test_known_tag_mapped_to_canonical(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags(["en:maple-syrups"], canonical_map)
        assert result == ["en:maple-syrups"]

    def test_unknown_tag_kept_as_is(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags(["en:unknown-tag"], canonical_map)
        assert result == ["en:unknown-tag"]

    def test_duplicates_removed(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags(["en:syrups", "en:syrups"], canonical_map)
        assert result == ["en:syrups"]

    def test_none_returns_empty(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        assert _normalize_tags(None, canonical_map) == []

    def test_string_repr_parsed(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        result = _normalize_tags("['en:syrups']", canonical_map)
        assert result == ["en:syrups"]

    def test_unknown_tag_logs_warning(self, taxonomy_text):
        canonical_map, _ = _parse_taxonomy(taxonomy_text)
        with patch("commands.normalize_categories.logger") as mock_logger:
            _normalize_tags(["en:unknown-tag"], canonical_map)
            mock_logger.warning.assert_called_once()


# ---------------------------------------------------------------------------
# Tests _build_categories_table
# ---------------------------------------------------------------------------

class TestBuildCategoriesTable:

    def test_category_names_are_unique(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        df, _ = _build_categories_table({"en:maple-syrups", "en:syrups"}, parent_map)
        assert df["category_name"].nunique() == len(df)

    def test_ancestors_included(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        df, _ = _build_categories_table({"en:maple-syrups"}, parent_map)
        names = set(df["category_name"])
        assert "en:syrups" in names
        assert "en:sweeteners" in names

    def test_full_chain_included_even_if_intermediate_already_present(self, taxonomy_text):
        """Si un ancêtre intermédiaire est déjà dans all_tags, la remontée ne doit
        pas s'arrêter — tous les ancêtres jusqu'à la racine doivent être inclus.

        Cas : all_tags = {en:maple-syrups, en:syrups}
        En:syrups est déjà présent mais en:sweeteners et en:food doivent
        quand même être ajoutés via la remontée depuis en:maple-syrups.
        """
        _, parent_map = _parse_taxonomy(taxonomy_text)
        df, _ = _build_categories_table({"en:maple-syrups", "en:syrups"}, parent_map)
        names = set(df["category_name"])
        assert "en:sweeteners" in names
        assert "en:food" in names

    def test_category_id_is_stable_hash(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        df, _ = _build_categories_table({"en:maple-syrups"}, parent_map)
        row = df[df["category_name"] == "en:maple-syrups"].iloc[0]
        assert row["category_id"] == _stable_id("en:maple-syrups")

    def test_tag_to_id_returned(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        _, tag_to_id = _build_categories_table({"en:maple-syrups"}, parent_map)
        assert "en:maple-syrups" in tag_to_id
        assert tag_to_id["en:maple-syrups"] == _stable_id("en:maple-syrups")

    def test_empty_tags_returns_empty_df_with_columns(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        df, tag_to_id = _build_categories_table(set(), parent_map)
        assert len(df) == 0
        assert list(df.columns) == ["category_id", "category_name"]
        assert tag_to_id == {}


# ---------------------------------------------------------------------------
# Tests _build_ancetre_categories
# ---------------------------------------------------------------------------

class TestBuildAncetreCategories:

    def test_parent_relationship(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        _, tag_to_id = _build_categories_table({"en:maple-syrups"}, parent_map)
        df = _build_ancetre_categories(tag_to_id, parent_map)
        row = df[(df["category_id"] == _stable_id("en:maple-syrups")) & (df["distance"] == 1)]
        assert len(row) == 1
        assert row.iloc[0]["category_id_parent"] == _stable_id("en:syrups")

    def test_full_chain_distances(self, taxonomy_text):
        """La chaîne complète en:maple-syrups → en:syrups → en:sweeteners → en:food
        doit produire les distances 1, 2, 3 correctement.
        """
        _, parent_map = _parse_taxonomy(taxonomy_text)
        _, tag_to_id = _build_categories_table({"en:maple-syrups"}, parent_map)
        df = _build_ancetre_categories(tag_to_id, parent_map)

        maple_rows = df[df["category_id"] == _stable_id("en:maple-syrups")].copy()
        maple_rows = maple_rows.set_index("distance")

        assert maple_rows.loc[1, "category_id_parent"] == _stable_id("en:syrups")
        assert maple_rows.loc[2, "category_id_parent"] == _stable_id("en:sweeteners")
        assert maple_rows.loc[3, "category_id_parent"] == _stable_id("en:food")

    def test_root_has_no_ancestors(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        _, tag_to_id = _build_categories_table({"en:food"}, parent_map)
        df = _build_ancetre_categories(tag_to_id, parent_map)
        root_rows = df[df["category_id"] == _stable_id("en:food")]
        assert len(root_rows) == 0

    def test_empty_input_returns_empty_df(self, taxonomy_text):
        _, parent_map = _parse_taxonomy(taxonomy_text)
        df = _build_ancetre_categories({}, parent_map)
        assert len(df) == 0
        assert list(df.columns) == ["category_id", "category_id_parent", "distance"]

    def test_multi_parent_chain(self, taxonomy_text_multi_parent):
        """Vérifie la chaîne complète quand une catégorie a plusieurs lignes <.

        en:maple-syrups → en:simple-syrups (d=1) → en:syrups (d=2) → en:sweeteners (d=3)
        """
        _, parent_map = _parse_taxonomy(taxonomy_text_multi_parent)
        _, tag_to_id = _build_categories_table({"en:maple-syrups"}, parent_map)
        df = _build_ancetre_categories(tag_to_id, parent_map)

        maple_rows = df[df["category_id"] == _stable_id("en:maple-syrups")].copy()
        maple_rows = maple_rows.set_index("distance")

        assert maple_rows.loc[1, "category_id_parent"] == _stable_id("en:simple-syrups")
        assert maple_rows.loc[2, "category_id_parent"] == _stable_id("en:syrups")
        assert maple_rows.loc[3, "category_id_parent"] == _stable_id("en:sweeteners")


# ---------------------------------------------------------------------------
# Tests handle
# ---------------------------------------------------------------------------

class TestHandle:

    def test_three_parquets_uploaded(self, mock_env_vars, sample_df):
        """handle() doit uploader exactement 3 parquets : categories, ancetre_categories, categorie_principale."""
        with patch("commands.normalize_categories.S3FileHandler") as mock_s3, \
             patch("commands.normalize_categories._download_categories_txt") as mock_dl:
            mock_dl.return_value = """
en:sweeteners

en:syrups
< en:sweeteners

en:maple-syrups
< en:syrups

en:beverages

en:plant-based-beverages
< en:beverages

en:snacks

en:salty-snacks
< en:snacks
"""
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("input.parquet", "categories.parquet", "ancetre_categories.parquet", "categorie_principale.parquet")

            assert mock_s3_instance.upload_dataframe.call_count == 3
            keys = [call[0][1] for call in mock_s3_instance.upload_dataframe.call_args_list]
            assert "categories.parquet" in keys
            assert "ancetre_categories.parquet" in keys
            assert "categorie_principale.parquet" in keys

    def test_categorie_principale_fk_integrity(self, mock_env_vars, sample_df):
        """Toutes les categorie_principale non-nulles existent dans categories.category_id."""
        with patch("commands.normalize_categories.S3FileHandler") as mock_s3, \
             patch("commands.normalize_categories._download_categories_txt") as mock_dl:
            mock_dl.return_value = """
en:sweeteners

en:syrups
< en:sweeteners

en:maple-syrups
< en:syrups

en:beverages

en:plant-based-beverages
< en:beverages

en:snacks

en:salty-snacks
< en:snacks
"""
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(sample_df)

            handle("input.parquet", "categories.parquet", "ancetre_categories.parquet", "categorie_principale.parquet")

            uploaded = {call[0][1]: call[0][0] for call in mock_s3_instance.upload_dataframe.call_args_list}
            cat_ids = set(uploaded["categories.parquet"]["category_id"])
            cp_ids = set(uploaded["categorie_principale.parquet"]["categorie_principale"].dropna())
            assert cp_ids.issubset(cat_ids)

    def test_missing_env_var_raises(self, sample_df):
        """KeyError si les variables S3 sont absentes."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("input.parquet", "categories.parquet", "ancetre_categories.parquet", "categorie_principale.parquet")

    def test_missing_categories_tags_column(self, mock_env_vars):
        """Si categories_tags est absente, 3 parquets vides sont quand même uploadés."""
        df_without_col = pd.DataFrame({"code": ["001"], "product_name": ["X"]})
        with patch("commands.normalize_categories.S3FileHandler") as mock_s3, \
             patch("commands.normalize_categories._download_categories_txt"):
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.return_value = _df_to_parquet_bytes(df_without_col)
            handle("input.parquet", "categories.parquet", "ancetre_categories.parquet", "categorie_principale.parquet")
            assert mock_s3_instance.upload_dataframe.call_count == 3
            uploaded = {call[0][1]: call[0][0] for call in mock_s3_instance.upload_dataframe.call_args_list}
            assert list(uploaded["categories.parquet"].columns) == ["category_id", "category_name"]
            assert list(uploaded["ancetre_categories.parquet"].columns) == ["category_id", "category_id_parent", "distance"]
            assert list(uploaded["categorie_principale.parquet"].columns) == ["code", "categorie_principale"]
