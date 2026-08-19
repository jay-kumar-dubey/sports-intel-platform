import json
from pathlib import Path

import requests


# --------------------------------------------------
# Configuration
# --------------------------------------------------

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"

COMPETITION_ID = 43

SEASONS = {
    2018: 3,
    2022: 106,
}

BASE_OUTPUT_DIR = Path(
    "storage/bronze/football/statsbomb/world-cup"
)


# --------------------------------------------------
# Helper function
# --------------------------------------------------

def download_json(url: str, output_path: Path):
    """Download a JSON file and save it locally."""

    print(f"Downloading: {url}")

    response = requests.get(url, timeout=60)

    response.raise_for_status()

    data = response.json()

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with output_path.open(
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False
        )

    print(f"Saved: {output_path}")


# --------------------------------------------------
# Download matches
# --------------------------------------------------

def download_matches(year: int, season_id: int):

    url = (
        f"{BASE_URL}/matches/"
        f"{COMPETITION_ID}/{season_id}.json"
    )

    output_path = (
        BASE_OUTPUT_DIR
        / str(year)
        / "matches"
        / "matches.json"
    )

    download_json(
        url,
        output_path
    )


# --------------------------------------------------
# Read match IDs
# --------------------------------------------------

def get_match_ids(year: int):

    matches_file = (
        BASE_OUTPUT_DIR
        / str(year)
        / "matches"
        / "matches.json"
    )

    with matches_file.open(
        "r",
        encoding="utf-8"
    ) as file:

        matches = json.load(file)

    return [
        match["match_id"]
        for match in matches
    ]


# --------------------------------------------------
# Download match events
# --------------------------------------------------

def download_events(
    year: int,
    match_ids: list
):

    for match_id in match_ids:

        url = (
            f"{BASE_URL}/events/"
            f"{match_id}.json"
        )

        output_path = (
            BASE_OUTPUT_DIR
            / str(year)
            / "events"
            / f"{match_id}.json"
        )

        download_json(
            url,
            output_path
        )


# --------------------------------------------------
# Download lineups
# --------------------------------------------------

def download_lineups(
    year: int,
    match_ids: list
):

    for match_id in match_ids:

        url = (
            f"{BASE_URL}/lineups/"
            f"{match_id}.json"
        )

        output_path = (
            BASE_OUTPUT_DIR
            / str(year)
            / "lineups"
            / f"{match_id}.json"
        )

        download_json(
            url,
            output_path
        )


# --------------------------------------------------
# Download 360 data
# --------------------------------------------------

def download_360(
    year: int,
    match_ids: list
):

    for match_id in match_ids:

        url = (
            f"{BASE_URL}/three-sixty/"
            f"{match_id}.json"
        )

        output_path = (
            BASE_OUTPUT_DIR
            / str(year)
            / "three-sixty"
            / f"{match_id}.json"
        )

        response = requests.get(
            url,
            timeout=60
        )

        # Not every match has 360 data.
        if response.status_code == 404:

            print(
                f"No 360 data: {match_id}"
            )

            continue

        response.raise_for_status()

        data = response.json()

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        with output_path.open(
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                ensure_ascii=False
            )

        print(
            f"Saved 360 data: {output_path}"
        )


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    for year, season_id in SEASONS.items():

        print()
        print("=" * 60)
        print(f"Processing FIFA World Cup {year}")
        print("=" * 60)

        # 1. Download matches
        download_matches(
            year,
            season_id
        )

        # 2. Get match IDs
        match_ids = get_match_ids(year)

        print(
            f"Found {len(match_ids)} matches"
        )

        # 3. Download events
        download_events(
            year,
            match_ids
        )

        # 4. Download lineups
        download_lineups(
            year,
            match_ids
        )

        # 5. Download 360 data
        if year == 2022:

            download_360(
                year,
                match_ids
            )


if __name__ == "__main__":
    main()