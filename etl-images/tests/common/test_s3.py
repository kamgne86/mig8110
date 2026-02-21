import pytest
import tempfile
import os
from io import BytesIO
from unittest.mock import Mock, patch, MagicMock
from common.s3 import S3FileHandler


class TestS3FileHandler:
    """Tests for S3FileHandler class"""

    @pytest.fixture
    def s3_handler(self):
        """Create S3FileHandler instance"""
        return S3FileHandler(
            s3_bucket="test-bucket",
            s3_endpoint="https://s3.example.com",
            s3_access_key="test-key",
            s3_secret_key="test-secret"
        )

    def test_init(self):
        """Test S3FileHandler initialization"""
        handler = S3FileHandler(
            s3_bucket="my-bucket",
            s3_endpoint="https://s3.example.com",
            s3_access_key="key",
            s3_secret_key="secret"
        )
        
        assert handler.s3_bucket == "my-bucket"
        assert handler.s3_endpoint == "https://s3.example.com"
        assert handler.s3_access_key == "key"
        assert handler.s3_secret_key == "secret"

    def test_upload_success(self, s3_handler):
        """Test successful upload of a local file"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"test content")
            tmp_file_path = tmp_file.name
        
        try:
            with patch("boto3.client") as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                
                s3_handler.upload(tmp_file_path, "test_key.txt")
                
                # Assertions
                mock_boto3.assert_called_once()
                mock_s3.upload_file.assert_called_once_with(
                    tmp_file_path, "test-bucket", "test_key.txt"
                )
        finally:
            os.unlink(tmp_file_path)

    def test_upload_file_not_found(self, s3_handler):
        """Test upload with non-existent file"""
        with pytest.raises(FileNotFoundError):
            s3_handler.upload("/nonexistent/file.txt", "test_key.txt")

    def test_upload_from_memory_success(self, s3_handler):
        """Test successful upload from memory"""
        file_obj = BytesIO(b"test data")
        
        with patch("boto3.client") as mock_boto3:
            mock_s3 = Mock()
            mock_boto3.return_value = mock_s3
            
            s3_handler.upload_from_memory(file_obj, "test_key.txt")
            
            # Assertions
            mock_boto3.assert_called_once()
            mock_s3.upload_fileobj.assert_called_once_with(
                file_obj, "test-bucket", "test_key.txt"
            )

    def test_download_success(self, s3_handler):
        """Test successful download of a file"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file_path = os.path.join(tmp_dir, "downloaded.txt")
            
            with patch("boto3.client") as mock_boto3:
                mock_s3 = Mock()
                mock_boto3.return_value = mock_s3
                
                s3_handler.download("test_key.txt", local_file_path)
                
                # Assertions
                mock_boto3.assert_called_once()
                mock_s3.download_file.assert_called_once_with(
                    "test-bucket", "test_key.txt", local_file_path
                )

    def test_boto3_client_configuration(self, s3_handler):
        """Test that boto3 client is configured correctly"""
        with patch("boto3.client") as mock_boto3:
            mock_s3 = Mock()
            mock_boto3.return_value = mock_s3
            
            file_obj = BytesIO(b"test")
            s3_handler.upload_from_memory(file_obj, "key")
            
            # Verify boto3.client was called with correct parameters
            call_args = mock_boto3.call_args
            assert call_args[1]["endpoint_url"] == "https://s3.example.com"
            assert call_args[1]["aws_access_key_id"] == "test-key"
            assert call_args[1]["aws_secret_access_key"] == "test-secret"
