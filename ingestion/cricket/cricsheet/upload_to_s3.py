import io
import zipfile
import time

import boto3
import requests
from botocore.exceptions import ClientError


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BUCKET_NAME = "sports-intel-platform-data"
AWS_REGION = "ap-south-2"

S3_BASE_PATH = "bronze/cricket/cricsheet"

CRICSHEET_URLS = {
    "tests": "https://cricsheet.org/downloads/tests_json.zip",
    "odis": "https://cricsheet.org/downloads/odis_json.zip",
    "t20is": "https://cricsheet.org/downloads/t20s_json.zip",
}


# --------------------------------------------------
# AWS S3 client
# --------------------------------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# --------------------------------------------------
# Check whether an object already exists in S3
# --------------------------------------------------

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


# --------------------------------------------------
# Download ZIP
# --------------------------------------------------

def download_zip(url, retries=3):

    for attempt in range(1, retries + 1):

        try:

            print(
                f"Downloading "
                f"(attempt {attempt}/{retries})..."
            )

            response = requests.get(
                url,
                timeout=300
            )

            response.raise_for_status()

            return response.content

        except requests.RequestException as error:

            print(
                f"Download failed: {error}"
            )

            if attempt < retries:

                print(
                    "Retrying in 5 seconds..."
                )

                time.sleep(5)

            else:

                raise


# --------------------------------------------------
# Upload one dataset
# --------------------------------------------------

def upload_dataset(
    dataset_name,
    url
):

    print()
    print("=" * 70)
    print(
        f"PROCESSING: {dataset_name.upper()}"
    )
    print("=" * 70)

    # ----------------------------------------------
    # Download dataset
    # ----------------------------------------------

    zip_content = download_zip(url)

    print(
        f"Downloaded: "
        f"{len(zip_content) / (1024 * 1024):.2f} MB"
    )

    # ----------------------------------------------
    # Open ZIP directly from memory
    # ----------------------------------------------

    zip_data = io.BytesIO(zip_content)

    with zipfile.ZipFile(zip_data) as archive:

        json_files = [
            name
            for name in archive.namelist()
            if name.endswith(".json")
        ]

        total_files = len(json_files)

        print(
            f"JSON files found: {total_files}"
        )

        uploaded = 0
        skipped = 0
        failed = 0

        # ------------------------------------------
        # Process each JSON file
        # ------------------------------------------

        for index, file_name in enumerate(
            json_files,
            start=1
        ):

            filename = file_name.split("/")[-1]

            s3_key = (
                f"{S3_BASE_PATH}/"
                f"{dataset_name}/"
                f"{filename}"
            )

            print(
                f"[{index}/{total_files}] "
                f"{filename}"
            )

            # --------------------------------------
            # Check S3 first
            # --------------------------------------

            if s3_object_exists(s3_key):

                print("  → Already in S3. Skipping.")

                skipped += 1

                continue

            # --------------------------------------
            # Read JSON from ZIP
            # --------------------------------------

            file_content = archive.read(
                file_name
            )

            # --------------------------------------
            # Upload with retry
            # --------------------------------------

            success = False

            for attempt in range(1, 4):

                try:

                    s3.upload_fileobj(
                        io.BytesIO(file_content),
                        BUCKET_NAME,
                        s3_key
                    )

                    success = True

                    print("  → Uploaded.")

                    uploaded += 1

                    break

                except Exception as error:

                    print(
                        f"  → Upload failed "
                        f"(attempt {attempt}/3): "
                        f"{error}"
                    )

                    if attempt < 3:

                        time.sleep(5)

            if not success:

                failed += 1

                print(
                    "  → FAILED after 3 attempts."
                )

    # ----------------------------------------------
    # Summary
    # ----------------------------------------------

    print()
    print(
        f"{dataset_name.upper()} COMPLETE"
    )

    print(
        f"Uploaded : {uploaded}"
    )

    print(
        f"Skipped  : {skipped}"
    )

    print(
        f"Failed   : {failed}"
    )

    return failed


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    total_failed = 0

    for dataset_name, url in CRICSHEET_URLS.items():

        failed = upload_dataset(
            dataset_name,
            url
        )

        total_failed += failed

    print()
    print("=" * 70)
    print("CRICKET BRONZE INGESTION COMPLETE")
    print("=" * 70)

    if total_failed == 0:

        print(
            "All files uploaded successfully "
            "or were already present in S3."
        )

    else:

        print(
            f"{total_failed} file(s) failed."
        )

        print(
            "Run the script again to retry them."
        )


if __name__ == "__main__":
    main()