import pytest
import tempfile
import os
from io import BytesIO
from unittest.mock import Mock, patch
from common.s3 import S3FileHandler


class TestS3FileHandler:
    """Tests for S3FileHandler class"""

    @pytest.fixture
    def mock_s3_client(self):
        """Mock boto3 client created at __init__"""
        with patch("boto3.client") as mock_boto3:
            mock_client = Mock()
            mock_boto3.return_value = mock_client
            yield mock_boto3, mock_client

    @pytest.fixture
    def s3_handler(self, mock_s3_client):
        """Create S3FileHandler instance with mocked boto3 client"""
        return S3FileHandler(
            s3_bucket="test-bucket",
            s3_endpoint="https://s3.example.com",
            s3_access_key="test-key",
            s3_secret_key="test-secret"
        )

    def test_init(self, mock_s3_client):
        """Test S3FileHandler initialization"""
        mock_boto3, _ = mock_s3_client
        handler = S3FileHandler(
            s3_bucket="my-bucket",
            s3_endpoint="https://s3.example.com",
            s3_access_key="key",
            s3_secret_key="secret"
        )

        assert handler.s3_bucket == "my-bucket"

    def test_boto3_client_configuration(self, mock_s3_client):
        """Test that boto3 client is configured correctly at init"""
        mock_boto3, _ = mock_s3_client
        S3FileHandler(
            s3_bucket="my-bucket",
            s3_endpoint="https://s3.example.com",
            s3_access_key="key",
            s3_secret_key="secret"
        )

        call_kwargs = mock_boto3.call_args[1]
        assert call_kwargs["endpoint_url"] == "https://s3.example.com"
        assert call_kwargs["aws_access_key_id"] == "key"
        assert call_kwargs["aws_secret_access_key"] == "secret"

    def test_upload_success(self, s3_handler, mock_s3_client):
        """Test successful upload of a local file"""
        _, mock_client = mock_s3_client

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"test content")
            tmp_file_path = tmp_file.name

        try:
            s3_handler.upload(tmp_file_path, "test_key.txt")

            mock_client.upload_file.assert_called_once_with(
                tmp_file_path, "test-bucket", "test_key.txt"
            )
        finally:
            os.unlink(tmp_file_path)

    def test_upload_file_not_found(self, s3_handler):
        """Test upload with non-existent file"""
        with pytest.raises(FileNotFoundError):
            s3_handler.upload("/nonexistent/file.txt", "test_key.txt")

    def test_upload_from_memory_success(self, s3_handler, mock_s3_client):
        """Test successful upload from memory"""
        _, mock_client = mock_s3_client
        file_obj = BytesIO(b"test data")

        s3_handler.upload_from_memory(file_obj, "test_key.txt")

        mock_client.upload_fileobj.assert_called_once_with(
            file_obj, "test-bucket", "test_key.txt"
        )

    def test_download_success(self, s3_handler, mock_s3_client):
        """Test successful download of a file"""
        _, mock_client = mock_s3_client

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file_path = os.path.join(tmp_dir, "downloaded.txt")

            s3_handler.download("test_key.txt", local_file_path)

            mock_client.download_file.assert_called_once_with(
                "test-bucket", "test_key.txt", local_file_path
            )
