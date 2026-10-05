import io
from collections import defaultdict

import boto3
import pyarrow.parquet as pq


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "sports-intel-platform-data"

DATASETS = {
    "odi": {
        "matches_prefix": "silver/cricket/odi/matches/",
        "deliveries_prefix": "silver/cricket/odi/deliveries/",
    },
    "test": {
        "matches_prefix": "silver/cricket/test/matches/",
        "deliveries_prefix": "silver/cricket/test/deliveries/",
    },
}


s3 = boto3.client("s3")


# ============================================================
# S3 HELPERS
# ============================================================

def list_parquet_keys(prefix):
    keys = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".parquet"):
                keys.append(key)

    keys.sort()

    return keys


def read_parquet_from_s3(key):
    response = s3.get_object(
        Bucket=BUCKET,
        Key=key,
    )

    data = response["Body"].read()

    return pq.read_table(
        io.BytesIO(data)
    )


# ============================================================
# VALIDATION
# ============================================================

def validate_dataset(match_type, config):
    print()
    print("#" * 80)
    print(
        f"{match_type.upper()} SILVER VALIDATION"
    )
    print("#" * 80)

    matches_keys = list_parquet_keys(
        config["matches_prefix"]
    )

    deliveries_keys = list_parquet_keys(
        config["deliveries_prefix"]
    )

    print()
    print(
        f"Match files:     "
        f"{len(matches_keys)}"
    )

    print(
        f"Delivery files:  "
        f"{len(deliveries_keys)}"
    )

    if not matches_keys:
        raise RuntimeError(
            f"No match Parquet files found "
            f"for {match_type}."
        )

    if not deliveries_keys:
        raise RuntimeError(
            f"No delivery Parquet files found "
            f"for {match_type}."
        )

    # --------------------------------------------------------
    # State
    # --------------------------------------------------------

    all_match_ids = set()

    duplicate_match_ids = set()

    duplicate_event_ids = set()

    all_delivery_match_ids = set()

    total_matches = 0
    total_deliveries = 0

    matches_without_deliveries = set()

    null_counts = defaultdict(int)

    run_mismatches = 0

    orphan_delivery_count = 0

    # --------------------------------------------------------
    # Required fields
    # --------------------------------------------------------

    required_match_columns = [
        "match_id",
        "date",
        "team_1",
        "team_2",
    ]

    required_delivery_columns = [
        "match_id",
        "innings_number",
        "batter",
        "bowler",
        "batter_runs",
        "extra_runs",
        "total_runs",
        "event_id",
    ]

    # --------------------------------------------------------
    # Validate match files
    # --------------------------------------------------------

    print()
    print("Validating matches...")

    for file_number, key in enumerate(
        matches_keys,
        start=1,
    ):
        table = read_parquet_from_s3(
            key
        )

        columns = table.column_names

        # Schema check
        missing_columns = [
            column
            for column in required_match_columns
            if column not in columns
        ]

        if missing_columns:
            raise RuntimeError(
                f"Missing columns in "
                f"{key}: "
                f"{missing_columns}"
            )

        data = table.to_pydict()

        row_count = table.num_rows

        total_matches += row_count

        match_ids = data["match_id"]

        for value in match_ids:
            if value is None:
                null_counts[
                    "matches.match_id"
                ] += 1
                continue

            if value in all_match_ids:
                duplicate_match_ids.add(
                    value
                )
            else:
                all_match_ids.add(
                    value
                )

        # Required-field null checks
        for column in required_match_columns:
            values = data[column]

            null_count = sum(
                value is None
                for value in values
            )

            if null_count:
                null_counts[
                    f"matches.{column}"
                ] += null_count

        if (
            file_number % 5 == 0
            or file_number
            == len(matches_keys)
        ):
            print(
                f"  Processed "
                f"{file_number}/"
                f"{len(matches_keys)} "
                f"match files"
            )

    # --------------------------------------------------------
    # Validate delivery files
    # --------------------------------------------------------

    print()
    print("Validating deliveries...")

    for file_number, key in enumerate(
        deliveries_keys,
        start=1,
    ):
        table = read_parquet_from_s3(
            key
        )

        columns = table.column_names

        missing_columns = [
            column
            for column in required_delivery_columns
            if column not in columns
        ]

        if missing_columns:
            raise RuntimeError(
                f"Missing columns in "
                f"{key}: "
                f"{missing_columns}"
            )

        data = table.to_pydict()

        row_count = table.num_rows

        total_deliveries += row_count

        match_ids = data["match_id"]
        event_ids = data["event_id"]

        innings_numbers = data[
            "innings_number"
        ]

        batter_runs = data[
            "batter_runs"
        ]

        extra_runs = data[
            "extra_runs"
        ]

        total_runs = data[
            "total_runs"
        ]

        # ----------------------------------------------------
        # Delivery-level checks
        # ----------------------------------------------------

        for i in range(row_count):

            match_id = match_ids[i]
            event_id = event_ids[i]

            # Null checks
            if match_id is None:
                null_counts[
                    "deliveries.match_id"
                ] += 1
            else:
                all_delivery_match_ids.add(
                    match_id
                )

            if event_id is None:
                null_counts[
                    "deliveries.event_id"
                ] += 1
            else:
                if event_id in duplicate_event_ids:
                    duplicate_event_ids.add(
                        event_id
                    )
                else:
                    duplicate_event_ids.add(
                        event_id
                    )

            if innings_numbers[i] is None:
                null_counts[
                    "deliveries.innings_number"
                ] += 1

            if batter_runs[i] is None:
                null_counts[
                    "deliveries.batter_runs"
                ] += 1

            if extra_runs[i] is None:
                null_counts[
                    "deliveries.extra_runs"
                ] += 1

            if total_runs[i] is None:
                null_counts[
                    "deliveries.total_runs"
                ] += 1

            # Required string fields
            if data["batter"][i] is None:
                null_counts[
                    "deliveries.batter"
                ] += 1

            if data["bowler"][i] is None:
                null_counts[
                    "deliveries.bowler"
                ] += 1

            # Run consistency
            if (
                batter_runs[i] is not None
                and extra_runs[i] is not None
                and total_runs[i] is not None
            ):
                if (
                    batter_runs[i]
                    + extra_runs[i]
                    != total_runs[i]
                ):
                    run_mismatches += 1

        if (
            file_number % 5 == 0
            or file_number
            == len(deliveries_keys)
        ):
            print(
                f"  Processed "
                f"{file_number}/"
                f"{len(deliveries_keys)} "
                f"delivery files"
            )

    # --------------------------------------------------------
    # IMPORTANT:
    # The event_id duplicate logic above needs an actual
    # seen-event set.
    # --------------------------------------------------------

    # The previous loop used duplicate_event_ids as both
    # the seen set and duplicate set. Recalculate event
    # uniqueness cleanly from the delivery files.

    seen_event_ids = set()
    duplicate_event_ids = set()

    for key in deliveries_keys:
        table = read_parquet_from_s3(
            key
        )

        event_ids = table[
            "event_id"
        ].to_pylist()

        for event_id in event_ids:
            if event_id is None:
                continue

            if event_id in seen_event_ids:
                duplicate_event_ids.add(
                    event_id
                )
            else:
                seen_event_ids.add(
                    event_id
                )

    # --------------------------------------------------------
    # Orphan delivery check
    # --------------------------------------------------------

    orphan_match_ids = (
        all_delivery_match_ids
        - all_match_ids
    )

    # Count orphan delivery rows
    if orphan_match_ids:
        for key in deliveries_keys:
            table = read_parquet_from_s3(
                key
            )

            match_ids = table[
                "match_id"
            ].to_pylist()

            orphan_delivery_count += sum(
                1
                for match_id in match_ids
                if match_id in orphan_match_ids
            )

    # --------------------------------------------------------
    # Matches without deliveries
    # --------------------------------------------------------

    matches_with_deliveries = (
        all_delivery_match_ids
    )

    matches_without_deliveries = (
        all_match_ids
        - matches_with_deliveries
    )

    # --------------------------------------------------------
    # Results
    # --------------------------------------------------------

    print()
    print("-" * 80)
    print("RESULTS")
    print("-" * 80)

    print(
        f"Total matches:    "
        f"{total_matches}"
    )

    print(
        f"Total deliveries: "
        f"{total_deliveries}"
    )

    # Match uniqueness
    if not duplicate_match_ids:
        print(
            "[PASS] match_id unique "
            "in matches"
        )
    else:
        print(
            "[FAIL] duplicate match_id: "
            f"{len(duplicate_match_ids)}"
        )

    # Event uniqueness
    if not duplicate_event_ids:
        print(
            "[PASS] event_id unique "
            "in deliveries"
        )
    else:
        print(
            "[FAIL] duplicate event_id: "
            f"{len(duplicate_event_ids)}"
        )

    # Orphans
    if orphan_delivery_count == 0:
        print(
            "[PASS] every delivery's "
            "match_id exists in matches "
            "(orphans: 0)"
        )
    else:
        print(
            "[FAIL] orphan deliveries: "
            f"{orphan_delivery_count}"
        )

    # Matches without deliveries
    if not matches_without_deliveries:
        print(
            "[PASS] every match has "
            "deliveries "
            "(matches without "
            "deliveries: 0)"
        )
    else:
        print(
            "[FAIL] matches without "
            "deliveries: "
            f"{len(matches_without_deliveries)}"
        )

    # Required nulls
    required_nulls = {
        key: value
        for key, value in null_counts.items()
        if value > 0
    }

    if not required_nulls:
        print(
            "[PASS] required fields "
            "have no nulls"
        )
    else:
        print(
            "[FAIL] required-field "
            "nulls:"
        )

        for key, value in sorted(
            required_nulls.items()
        ):
            print(
                f"       {key}: {value}"
            )

    # Run consistency
    if run_mismatches == 0:
        print(
            "[PASS] total_runs == "
            "batter_runs + extra_runs "
            "(mismatches: 0)"
        )
    else:
        print(
            "[FAIL] run mismatches: "
            f"{run_mismatches}"
        )

    # --------------------------------------------------------
    # Overall status
    # --------------------------------------------------------

    passed = (
        not duplicate_match_ids
        and not duplicate_event_ids
        and orphan_delivery_count == 0
        and not matches_without_deliveries
        and not required_nulls
        and run_mismatches == 0
    )

    print()

    if passed:
        print(
            f"{match_type.upper()} "
            "SILVER VALIDATION: "
            "ALL CHECKS PASSED"
        )
    else:
        print(
            f"{match_type.upper()} "
            "SILVER VALIDATION: "
            "FAILED"
        )

        raise RuntimeError(
            f"{match_type.upper()} "
            "Silver validation failed."
        )


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print(
        "CRICKET ODI + TEST SILVER "
        "VALIDATION"
    )
    print("=" * 80)

    for match_type, config in DATASETS.items():
        validate_dataset(
            match_type,
            config,
        )

    print()
    print("=" * 80)
    print(
        "ALL ODI + TEST SILVER "
        "CHECKS PASSED"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()