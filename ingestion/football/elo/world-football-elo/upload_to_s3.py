import io
import time

import boto3
import requests
from botocore.exceptions import ClientError


# ============================================================
# Configuration
# ============================================================

BUCKET_NAME = "sports-intel-platform-data"
AWS_REGION = "ap-south-2"

S3_BASE_PATH = "bronze/football/elo/world-football-elo"

# We want historical ratings around the 2018 and 2022
# World Cups.
YEARS = range(2018, 2027)

BASE_URL = "https://www.eloratings.net"


# ============================================================
# AWS S3 client
# ============================================================

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# Check whether file already exists
# ============================================================

def s3_object_exists(s3_key):

    try:

        s3.head_object(
            Bucket=BUCKET_NAME,
            Key=s3_key
        )

        return True

    except ClientError as error:

        error_code = error.response["Error"]["Code"]

        if error_code in ["404", "NoSuchKey"]:

            return False

        raise


# ============================================================
# Download TSV with retries
# ============================================================

def download_tsv(year, retries=3):

    url = f"{BASE_URL}/{year}.tsv"

    for attempt in range(1, retries + 1):

        try:

            print()
            print(
                f"Downloading {year}.tsv "
                f"(attempt {attempt}/{retries})"
            )

            response = requests.get(
                url,
                timeout=60
            )

            response.raise_for_status()

            print(
                f"Downloaded {len(response.content):,} bytes"
            )

            return response.content

        except requests.RequestException as error:

            print(
                f"Download failed: {error}"
            )

            if attempt < retries:

                print("Retrying in 5 seconds...")

                time.sleep(5)

            else:

                raise


# ============================================================
# Upload one year
# ============================================================

def upload_year(year):

    s3_key = (
        f"{S3_BASE_PATH}/"
        f"ratings/{year}.tsv"
    )

    print()
    print("=" * 60)
    print(f"PROCESSING ELO DATA: {year}")
    print("=" * 60)

    # --------------------------------------------------------
    # Resume support
    # --------------------------------------------------------

    if s3_object_exists(s3_key):

        print(
            "Already exists in S3. Skipping."
        )

        return

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    content = download_tsv(year)

    # --------------------------------------------------------
    # Upload directly from memory
    # --------------------------------------------------------

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=content,
        ContentType="text/tab-separated-values"
    )

    print(
        f"Uploaded:"
        f" s3://{BUCKET_NAME}/{s3_key}"
    )


# ============================================================
# Main
# ============================================================

def main():

    print()
    print("=" * 70)
    print("WORLD FOOTBALL ELO → AWS S3 BRONZE")
    print("=" * 70)

    for year in YEARS:

        try:

            upload_year(year)

        except Exception as error:

            print()
            print(
                f"FAILED: {year}.tsv"
            )

            print(error)

            print(
                "Continuing with the next year..."
            )

    print()
    print("=" * 70)
    print("ELO INGESTION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()