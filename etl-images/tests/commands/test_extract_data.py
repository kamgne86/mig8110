import os
import pytest
import zipfile
import pandas as pd
from io import BytesIO
from unittest.mock import Mock, patch
from commands.extract_data import handle


class TestFullLoad:
    """Tests for extract data command"""

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
    def data(self):
        """Create a sample parquet file in a zip archive"""
        df = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})
        
        # Create parquet file in memory
        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes, index=False)
        parquet_bytes.seek(0)
        
        # Create zip file with parquet
        zip_bytes = BytesIO()
        with zipfile.ZipFile(zip_bytes, "w") as zip_file:
            zip_file.writestr("data.parquet", parquet_bytes.getvalue())
        
        zip_bytes.seek(0)
        return zip_bytes.getvalue(), df

    def test_handle_success(self, mock_env_vars, data):
        """Test successful full load operation"""
        zip_content, expected_df = data
        url = "https://example.com/data.parquet.zip"
        output_key = "test_output.parquet"

        with patch("commands.extract_data.requests.Session") as mock_session_cls, \
             patch("commands.extract_data.S3FileHandler") as mock_s3:

            # Mock HTTP response
            mock_session = Mock()
            mock_session_cls.return_value = mock_session
            mock_response = Mock()
            mock_response.content = zip_content
            mock_session.get.return_value = mock_response

            # Mock S3 handler
            mock_s3_instance = Mock()
            mock_s3.return_value = mock_s3_instance

            # Execute
            handle(output_key, url)

            # Assertions
            mock_session.get.assert_called_once_with(url, timeout=(10, 60))
            mock_s3.assert_called_once_with(
                "test-bucket",
                "https://s3.example.com",
                "test-key",
                "test-secret"
            )
            mock_s3_instance.upload_from_memory.assert_called_once()

    def test_handle_invalid_zip(self, mock_env_vars):
        """Test handling of invalid zip file"""
        url = "https://example.com/invalid.zip"
        output_key = "test_output.parquet"

        with patch("commands.extract_data.requests.Session") as mock_session_cls:
            mock_session = Mock()
            mock_session_cls.return_value = mock_session
            mock_response = Mock()
            mock_response.content = b"invalid zip content"
            mock_session.get.return_value = mock_response

            # Should raise BadZipFile error
            with pytest.raises(zipfile.BadZipFile):
                handle(output_key, url)

    def test_handle_no_parquet_in_zip(self, mock_env_vars):
        """Test handling of zip with no parquet file"""
        url = "https://example.com/no_parquet.zip"
        output_key = "test_output.parquet"

        # Create a zip with a non-parquet file
        zip_bytes = BytesIO()
        with zipfile.ZipFile(zip_bytes, "w") as zip_file:
            zip_file.writestr("data.csv", "col1,col2\n1,a")
        zip_bytes.seek(0)

        with patch("commands.extract_data.requests.Session") as mock_session_cls:
            mock_session = Mock()
            mock_session_cls.return_value = mock_session
            mock_response = Mock()
            mock_response.content = zip_bytes.getvalue()
            mock_session.get.return_value = mock_response

            with pytest.raises(ValueError, match="No parquet file found in zip"):
                handle(output_key, url)

    def test_handle_missing_env_var(self, data):
        """Test handling of missing environment variables"""
        zip_content, _ = data
        url = "https://example.com/data.parquet.zip"
        output_key = "test_output.parquet"

        with patch("commands.extract_data.requests.Session") as mock_session_cls:
            mock_session = Mock()
            mock_session_cls.return_value = mock_session
            mock_response = Mock()
            mock_response.content = zip_content
            mock_session.get.return_value = mock_response

            # Clear environment to trigger KeyError
            with patch.dict(os.environ, {}, clear=True):
                with pytest.raises(KeyError):
                    handle(output_key, url)
