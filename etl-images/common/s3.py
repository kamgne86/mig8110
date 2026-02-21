import os
import boto3


class S3FileHandler:
    def __init__(self, s3_bucket, s3_endpoint, s3_access_key, s3_secret_key):
        self.s3_bucket = s3_bucket
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key

    def upload(self, local_file_path, s3_key):
        if not os.path.isfile(local_file_path):
            raise FileNotFoundError(f"File {local_file_path} not found.")
        
        s3 = boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
        )

        s3.upload_file(local_file_path, self.s3_bucket, s3_key)

    def upload_from_memory(self, file_obj, s3_key):
        s3 = boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
        )

        s3.upload_fileobj(file_obj, self.s3_bucket, s3_key)

    def download(self, s3_key, local_file_path):
        s3 = boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
        )

        s3.download_file(self.s3_bucket, s3_key, local_file_path)
