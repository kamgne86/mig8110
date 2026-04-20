import os
import pytest
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.finalize_products import handle


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


def _make_cp_parquet(codes):
    """Parquet categorie_principale minimal pour les tests."""
    df = pd.DataFrame({"code": codes, "categorie_principale": [None] * len(codes)})
    return _df_to_parquet_bytes(df)


class TestFinalizeProducts:

    def test_categories_tags_dropped(self, mock_env_vars):
        """categories_tags est supprimée du parquet de sortie."""
        df = pd.DataFrame({
            "code": ["001"],
            "product_name": ["Maple Syrup"],
            "categories_tags": [["en:syrups"]],
            "ingredients_tags": [["en:sugar"]],
        })
        with patch("commands.finalize_products.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.side_effect = [
                _df_to_parquet_bytes(df), _make_cp_parquet(["001"])
            ]
            handle("input.parquet", "cp.parquet", "products.parquet")
            uploaded_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert "categories_tags" not in uploaded_df.columns

    def test_ingredients_tags_dropped(self, mock_env_vars):
        """ingredients_tags est supprimée du parquet de sortie."""
        df = pd.DataFrame({
            "code": ["001"],
            "product_name": ["Maple Syrup"],
            "categories_tags": [["en:syrups"]],
            "ingredients_tags": [["en:sugar"]],
        })
        with patch("commands.finalize_products.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.side_effect = [
                _df_to_parquet_bytes(df), _make_cp_parquet(["001"])
            ]
            handle("input.parquet", "cp.parquet", "products.parquet")
            uploaded_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert "ingredients_tags" not in uploaded_df.columns

    def test_other_columns_preserved(self, mock_env_vars):
        """Les autres colonnes (code, product_name, ingredients_n, ...) sont conservées."""
        df = pd.DataFrame({
            "code": ["001"],
            "product_name": ["Maple Syrup"],
            "ingredients_n": [5],
            "categories_tags": [["en:syrups"]],
            "ingredients_tags": [["en:sugar"]],
        })
        with patch("commands.finalize_products.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.side_effect = [
                _df_to_parquet_bytes(df), _make_cp_parquet(["001"])
            ]
            handle("input.parquet", "cp.parquet", "products.parquet")
            uploaded_df = mock_s3_instance.upload_dataframe.call_args[0][0]
            assert "code" in uploaded_df.columns
            assert "product_name" in uploaded_df.columns
            assert "ingredients_n" in uploaded_df.columns

    def test_columns_absent_do_not_raise(self, mock_env_vars):
        """Si categories_tags ou ingredients_tags sont absentes, aucune erreur n'est levée."""
        df = pd.DataFrame({"code": ["001"], "product_name": ["Maple Syrup"]})
        with patch("commands.finalize_products.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.side_effect = [
                _df_to_parquet_bytes(df), _make_cp_parquet(["001"])
            ]
            handle("input.parquet", "cp.parquet", "products.parquet")
            mock_s3_instance.upload_dataframe.assert_called_once()

    def test_uploads_to_correct_s3_key(self, mock_env_vars):
        """Le parquet est uploadé à la bonne clé S3."""
        df = pd.DataFrame({"code": ["001"]})
        with patch("commands.finalize_products.S3FileHandler") as mock_s3:
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance
            mock_s3_instance.download_to_memory.side_effect = [
                _df_to_parquet_bytes(df), _make_cp_parquet(["001"])
            ]
            handle("input.parquet", "cp.parquet", "silver/products_final.parquet")
            _, s3_key = mock_s3_instance.upload_dataframe.call_args[0]
            assert s3_key == "silver/products_final.parquet"

    def test_missing_env_var_raises(self):
        """KeyError si les variables S3 sont absentes."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                handle("input.parquet", "cp.parquet", "products.parquet")
