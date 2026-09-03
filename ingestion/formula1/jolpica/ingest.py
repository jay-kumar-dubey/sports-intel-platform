import os
import requests
from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

API_KEY = os.getenv("THE_STATS_API_KEY")

BASE_URL = "https://api.thestatsapi.com/api"

COMPETITION_ID = "comp_6107"
SEASON_ID = "sn_118868"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}


# ============================================================
# VALIDATE API KEY
# ============================================================

if not API_KEY:
    raise ValueError(
        "THE_STATS_API_KEY not found in .env file."
    )

print("API key loaded:", bool(API_KEY))


# ============================================================
# API REQUEST FUNCTION
# ============================================================

def get_api_data(endpoint, params):
    """Fetch data from TheStatsAPI."""

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

        params = {
            "competition_id": COMPETITION_ID,
            "season_id": SEASON_ID,
            "page": page,
            "per_page": per_page
        }

        print(f"\nRequesting page {page}...")

        result = get_api_data(
            "/football/matches",
            params
        )

        matches = result.get("data", [])

        if not matches:
            print("No more matches returned.")
            break

        all_matches.extend(matches)

        print(
            f"Page {page}: "
            f"{len(matches)} matches | "
            f"Total collected: {len(all_matches)}"
        )

        # ----------------------------------------------------
        # Check API pagination metadata
        # ----------------------------------------------------

        meta = result.get("meta", {})

        total_pages = meta.get("total_pages")
        total = meta.get("total")

        if total:
            print(f"API total matches: {total}")

        if total_pages:
            print(f"API total pages: {total_pages}")

        # ----------------------------------------------------
        # Stop when final page is reached
        # ----------------------------------------------------

        if total_pages and page >= total_pages:
            break

        # ----------------------------------------------------
        # Safety check
        # ----------------------------------------------------

        if len(matches) < per_page:
            break

        page += 1

    return all_matches


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    matches = fetch_matches()

    print("\n" + "=" * 60)
    print("INGESTION TEST COMPLETE")
    print("=" * 60)

    print(f"Total matches fetched: {len(matches)}")

    # --------------------------------------------------------
    # Display first match as a sanity check
    # --------------------------------------------------------

    if matches:

        first_match = matches[0]

        print("\nFirst match:")
        print(
            f"{first_match['home_team']['name']} "
            f"vs "
            f"{first_match['away_team']['name']}"
        )

        print(
            f"Date: {first_match['utc_date']}"
        )

        print(
            f"Match ID: {first_match['id']}"
        )

    else:

        print("\nWARNING: No matches were returned.")