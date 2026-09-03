# ============================================================
# THE STATS API - FIFA WORLD CUP 2026 INGESTION
# ============================================================

import os
import json
import time
from pathlib import Path

import requests
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURATION
# ============================================================

# Project root:
# sports-intel-platform/
# ├── ingestion/
# ├── storage/
# └── .env

PROJECT_ROOT = Path(__file__).resolve().parents[4]

load_dotenv(PROJECT_ROOT / ".env")


# ------------------------------------------------------------
# TheStatsAPI
# ------------------------------------------------------------

THE_STATS_API_KEY = os.getenv("THE_STATS_API_KEY")

if not THE_STATS_API_KEY:
    raise ValueError(
        "THE_STATS_API_KEY not found in .env file."
    )


BASE_URL = "https://api.thestatsapi.com/api/football"

COMPETITION_ID = "comp_6107"
SEASON_ID = "sn_118868"


HEADERS = {
    "Authorization": f"Bearer {THE_STATS_API_KEY}"
}


# ------------------------------------------------------------
# AWS
# ------------------------------------------------------------

S3_BUCKET = os.getenv(
    "SPORTS_INTEL_S3_BUCKET",
    "sports-intel-platform-data"
)

AWS_REGION = os.getenv(
    "AWS_REGION",
    "ap-south-2"
)


# ------------------------------------------------------------
# Local Bronze paths
# ------------------------------------------------------------

BRONZE_ROOT = (
    PROJECT_ROOT
    / "storage"
    / "bronze"
    / "football"
    / "the-stats-api"
    / "world-cup-2026"
)

MATCHES_DIR = BRONZE_ROOT / "matches"

PLAYER_STATS_DIR = BRONZE_ROOT / "player-statistics"


MATCHES_DIR.mkdir(
    parents=True,
    exist_ok=True
)

PLAYER_STATS_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ------------------------------------------------------------
# S3
# ------------------------------------------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)


# ============================================================
# API REQUEST
# ============================================================

def get_api_data(endpoint, params=None):
    """
    Fetch data from TheStatsAPI.
    """

    url = f"{BASE_URL}{endpoint}"

    response = requests.get(
        url,
        headers=HEADERS,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# FETCH ALL MATCHES
# ============================================================

def fetch_matches():

    print("\nFetching FIFA World Cup 2026 matches...")

    all_matches = []

    page = 1
    per_page = 100

    while True:

        print(f"\nRequesting page {page}...")

        params = {
            "competition_id": COMPETITION_ID,
            "season_id": SEASON_ID,
            "page": page,
            "per_page": per_page
        }

        result = get_api_data(
            "/matches",
            params
        )

        matches = result.get(
            "data",
            []
        )

        if not matches:
            break

        all_matches.extend(matches)

        meta = result.get(
            "meta",
            {}
        )

        total = meta.get(
            "total"
        )

        total_pages = meta.get(
            "total_pages"
        )

        print(
            f"Page {page}: "
            f"{len(matches)} matches | "
            f"Total collected: {len(all_matches)}"
        )

        print(
            f"API total matches: {total}"
        )

        print(
            f"API total pages: {total_pages}"
        )

        if total_pages and page >= total_pages:
            break

        if len(matches) < per_page:
            break

        page += 1

    return all_matches


# ============================================================
# S3 OBJECT EXISTS
# ============================================================

def s3_object_exists(key):

    try:

        s3.head_object(
            Bucket=S3_BUCKET,
            Key=key
        )

        return True

    except ClientError as e:

        error_code = e.response.get(
            "Error",
            {}
        ).get(
            "Code"
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):
            return False

        raise


# ============================================================
# SAVE JSON LOCALLY
# ============================================================

def save_json_local(data, filepath):

    with open(
        filepath,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2
        )


# ============================================================
# UPLOAD JSON TO S3
# ============================================================

def upload_to_s3(
    local_file,
    s3_key
):

    if s3_object_exists(s3_key):

        print(
            f"SKIP S3: {s3_key}"
        )

        return False

    print(
        f"Uploading: {s3_key}"
    )

    s3.upload_file(
        str(local_file),
        S3_BUCKET,
        s3_key
    )

    print(
        f"Uploaded: {s3_key}"
    )

    return True


# ============================================================
# INGEST MATCH
# ============================================================

def ingest_match(match):

    match_id = match["id"]

    home_team = match["home_team"]["name"]
    away_team = match["away_team"]["name"]

    print("\n" + "=" * 60)

    print(
        f"Match: {home_team} vs {away_team}"
    )

    print(
        f"Match ID: {match_id}"
    )

    # --------------------------------------------------------
    # MATCH JSON
    # --------------------------------------------------------

    match_filename = (
        f"{match_id}.json"
    )

    match_local_path = (
        MATCHES_DIR
        / match_filename
    )

    match_s3_key = (
        "bronze/football/"
        "the-stats-api/"
        "world-cup-2026/"
        "matches/"
        f"{match_filename}"
    )

    # Save locally if not already present

    if match_local_path.exists():

        print(
            "Local match file exists - skipping API save."
        )

    else:

        save_json_local(
            match,
            match_local_path
        )

        print(
            f"Saved match locally: "
            f"{match_local_path}"
        )

    # Upload match

    upload_to_s3(
        match_local_path,
        match_s3_key
    )

    # --------------------------------------------------------
    # PLAYER STATISTICS
    # --------------------------------------------------------

    player_stats_filename = (
        f"{match_id}.json"
    )

    player_stats_local_path = (
        PLAYER_STATS_DIR
        / player_stats_filename
    )

    player_stats_s3_key = (
        "bronze/football/"
        "the-stats-api/"
        "world-cup-2026/"
        "player-statistics/"
        f"{player_stats_filename}"
    )

    # --------------------------------------------------------
    # IDMPOTENCY CHECK
    # --------------------------------------------------------

    if (
        player_stats_local_path.exists()
        or s3_object_exists(
            player_stats_s3_key
        )
    ):

        print(
            "Player statistics already exist - skipping."
        )

        return

    # --------------------------------------------------------
    # FETCH PLAYER STATISTICS
    # --------------------------------------------------------

    print(
        "Fetching player statistics..."
    )

    endpoint = (
        f"/matches/{match_id}/player-stats"
    )

    player_stats = get_api_data(
        endpoint
    )

    # --------------------------------------------------------
    # SAVE PLAYER STATISTICS
    # --------------------------------------------------------

    save_json_local(
        player_stats,
        player_stats_local_path
    )

    print(
        f"Saved player statistics: "
        f"{player_stats_local_path}"
    )

    # --------------------------------------------------------
    # UPLOAD PLAYER STATISTICS
    # --------------------------------------------------------

    upload_to_s3(
        player_stats_local_path,
        player_stats_s3_key
    )

    # Small delay to avoid unnecessarily hammering API

    time.sleep(0.2)


# ============================================================
# MAIN INGESTION
# ============================================================

def main():

    print(
        "\n"
        + "=" * 60
    )

    print(
        "THE STATS API - FIFA WORLD CUP 2026"
    )

    print(
        "BRONZE INGESTION"
    )

    print(
        "=" * 60
    )

    print(
        f"\nS3 Bucket: {S3_BUCKET}"
    )

    print(
        f"AWS Region: {AWS_REGION}"
    )

    print(
        f"Local Bronze: {BRONZE_ROOT}"
    )

    # --------------------------------------------------------
    # FETCH MATCHES
    # --------------------------------------------------------

    matches = fetch_matches()

    print(
        "\n"
        + "=" * 60
    )

    print(
        f"TOTAL MATCHES: {len(matches)}"
    )

    print(
        "=" * 60
    )

    # --------------------------------------------------------
    # INGEST EACH MATCH
    # --------------------------------------------------------

    successful = 0
    failed = 0

    for index, match in enumerate(
        matches,
        start=1
    ):

        print(
            f"\nProcessing "
            f"{index}/{len(matches)}"
        )

        try:

            ingest_match(
                match
            )

            successful += 1

        except Exception as e:

            failed += 1

            print(
                f"ERROR processing "
                f"{match.get('id')}: {e}"
            )

            # Continue with next match

            continue

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    print(
        "\n"
        + "=" * 60
    )

    print(
        "INGESTION COMPLETE"
    )

    print(
        "=" * 60
    )

    print(
        f"Total matches: {len(matches)}"
    )

    print(
        f"Successful: {successful}"
    )

    print(
        f"Failed: {failed}"
    )

    print(
        "\nLocal paths:"
    )

    print(
        f"Matches: {MATCHES_DIR}"
    )

    print(
        f"Player statistics: {PLAYER_STATS_DIR}"
    )

    print(
        "\nS3 paths:"
    )

    print(
        "s3://"
        f"{S3_BUCKET}/bronze/football/"
        "the-stats-api/world-cup-2026/"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()