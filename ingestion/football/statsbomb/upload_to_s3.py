import os
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig


# -----------------------------
# Configuration
# -----------------------------

BUCKET_NAME = "sports-intel-platform-data"
AWS_REGION = "ap-south-2"

LOCAL_DIRECTORY = Path(
    "storage/bronze/football/statsbomb/world-cup"
)

S3_PREFIX = "bronze/football/statsbomb/world-cup"


# -----------------------------
# S3 Client
# -----------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# -----------------------------
# Upload configuration
# -----------------------------

config = TransferConfig(
    multipart_threshold=100 * 1024 * 1024,  # 100 MB
    multipart_chunksize=25 * 1024 * 1024,   # 25 MB
    max_concurrency=10,
    use_threads=True
)


# -----------------------------
# Upload function
# -----------------------------

def upload_directory():

    if not LOCAL_DIRECTORY.exists():
        raise FileNotFoundError(
            f"Directory not found: {LOCAL_DIRECTORY}"
        )

    files_uploaded = 0

    for file_path in LOCAL_DIRECTORY.rglob("*"):

        if not file_path.is_file():
            continue

        # Don't upload Git placeholder files
        if file_path.name == ".gitkeep":
            continue

        # Path relative to the Bronze World Cup directory
        relative_path = file_path.relative_to(
            LOCAL_DIRECTORY
        )

        # S3 object path
        s3_key = (
            f"{S3_PREFIX}/{relative_path.as_posix()}"
        )

        print(f"Uploading: {file_path}")
        print(f"       S3: s3://{BUCKET_NAME}/{s3_key}")

        s3.upload_file(
            str(file_path),
            BUCKET_NAME,
            s3_key,
            Config=config
        )

        files_uploaded += 1

        print("Uploaded successfully.\n")

    print(
        f"Finished. {files_uploaded} file(s) uploaded."
    )


# -----------------------------
# Main
# -----------------------------

if __name__ == "__main__":
    upload_directory()