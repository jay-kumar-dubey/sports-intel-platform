import json
import shutil
import time
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "sports-intel-platform-data"

BRONZE_PREFIXES = {
    "odi": "bronze/cricket/cricsheet/odis/",
    "test": "bronze/cricket/cricsheet/tests/",
}

SILVER_PREFIXES = {
    "odi": {
        "matches": "silver/cricket/odi/matches/",
        "deliveries": "silver/cricket/odi/deliveries/",
    },
    "test": {
        "matches": "silver/cricket/test/matches/",
        "deliveries": "silver/cricket/test/deliveries/",
    },
}

WORK_ROOT = Path("transformation/cricket/work_odi_test")
STATE_ROOT = Path("transformation/cricket/state_odi_test")

# 100 matches per batch.
BATCH_SIZE = 100

# ------------------------------------------------------------
# Processing range
# ------------------------------------------------------------
#
# Existing manifest contains ODI batches 1-10.
# Batch 11 failed.
#
# None = process through the end.
#
START_BATCH = 1
END_BATCH = None


s3 = boto3.client("s3")


# ============================================================
# EXPLICIT PARQUET SCHEMAS
# ============================================================

MATCHES_SCHEMA = pa.schema([
    pa.field("match_id", pa.string()),
    pa.field("date", pa.string()),
    pa.field("season", pa.string()),
    pa.field("gender", pa.string()),
    pa.field("match_type", pa.string()),
    pa.field("city", pa.string()),
    pa.field("venue", pa.string()),
    pa.field("team_1", pa.string()),
    pa.field("team_2", pa.string()),
    pa.field("toss_winner", pa.string()),
    pa.field("toss_decision", pa.string()),
    pa.field("winner", pa.string()),
    pa.field("result", pa.string()),
    pa.field("method", pa.string()),
    pa.field("win_by_runs", pa.int64()),
    pa.field("win_by_wickets", pa.int64()),
    pa.field("win_by_innings", pa.int64()),
    pa.field("player_of_match", pa.string()),
])


DELIVERIES_SCHEMA = pa.schema([
    pa.field("match_id", pa.string()),
    pa.field("innings_number", pa.int32()),
    pa.field("batting_team", pa.string()),
    pa.field("over_index", pa.int32()),
    pa.field("delivery_index", pa.int32()),
    pa.field("over_number", pa.int32()),
    pa.field("delivery_number", pa.string()),
    pa.field("batter", pa.string()),
    pa.field("bowler", pa.string()),
    pa.field("non_striker", pa.string()),
    pa.field("batter_runs", pa.int64()),
    pa.field("extra_runs", pa.int64()),
    pa.field("total_runs", pa.int64()),
    pa.field("extra_byes", pa.float64()),
    pa.field("extra_legbyes", pa.float64()),
    pa.field("extra_noballs", pa.float64()),
    pa.field("extra_wides", pa.float64()),
    pa.field("extra_penalty", pa.float64()),
    pa.field("wicket_kind", pa.string()),
    pa.field("player_out", pa.string()),
    pa.field("event_id", pa.string()),
])


# ============================================================
# MANIFEST
# ============================================================

def manifest_path(match_type):
    return STATE_ROOT / f"{match_type}_manifest.json"


def load_manifest(match_type):
    path = manifest_path(match_type)

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(match_type, manifest):
    STATE_ROOT.mkdir(parents=True, exist_ok=True)

    path = manifest_path(match_type)
    temp_path = path.with_suffix(".tmp")

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    temp_path.replace(path)


# ============================================================
# S3 DISCOVERY
# ============================================================

def list_s3_keys(prefix):
    keys = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".json"):
                keys.append(key)

    keys.sort()

    return keys


# ============================================================
# DOWNLOAD
# ============================================================

def download_json(s3_key, local_path):
    local_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    s3.download_file(
        BUCKET,
        s3_key,
        str(local_path),
    )


# ============================================================
# VALUE HELPERS
# ============================================================

def first_or_none(value):
    if isinstance(value, list) and value:
        return value[0]

    return None


def to_int(value):
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value):
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_string(value):
    if value is None:
        return None

    return str(value)


# ============================================================
# MATCH TRANSFORMATION
# ============================================================

def transform_match(match_id, data):
    info = data.get("info", {})
    outcome = info.get("outcome", {})

    teams = info.get("teams", [])

    team_1 = (
        teams[0]
        if len(teams) > 0
        else None
    )

    team_2 = (
        teams[1]
        if len(teams) > 1
        else None
    )

    dates = info.get("dates", [])

    date_value = first_or_none(dates)

    season = info.get("season")

    toss = info.get("toss", {})

    outcome_by = outcome.get("by", {})

    if not isinstance(outcome_by, dict):
        outcome_by = {}

    player_of_match = first_or_none(
        info.get("player_of_match", [])
    )

    return {
        "match_id": to_string(match_id),

        "date": to_string(date_value),

        "season": to_string(season),

        "gender": to_string(
            info.get("gender")
        ),

        "match_type": to_string(
            info.get("match_type")
        ),

        "city": to_string(
            info.get("city")
        ),

        "venue": to_string(
            info.get("venue")
        ),

        "team_1": to_string(team_1),

        "team_2": to_string(team_2),

        "toss_winner": to_string(
            toss.get("winner")
        ),

        "toss_decision": to_string(
            toss.get("decision")
        ),

        "winner": to_string(
            outcome.get("winner")
        ),

        "result": to_string(
            outcome.get("result")
        ),

        "method": to_string(
            outcome.get("method")
        ),

        "win_by_runs": to_int(
            outcome_by.get("runs")
        ),

        "win_by_wickets": to_int(
            outcome_by.get("wickets")
        ),

        "win_by_innings": to_int(
            outcome_by.get("innings")
        ),

        "player_of_match": to_string(
            player_of_match
        ),
    }


# ============================================================
# DELIVERY TRANSFORMATION
# ============================================================

def transform_deliveries(match_id, data):
    rows = []

    innings_list = data.get("innings", [])

    for innings_index, innings in enumerate(
        innings_list
    ):

        # IMPORTANT:
        # Cricsheet innings are naturally ordered.
        # We make the analytical key 1-based.
        innings_number = innings_index + 1

        batting_team = innings.get("team")

        overs = innings.get("overs", [])

        for over_index, over in enumerate(overs):

            over_number = to_int(
                over.get("over")
            )

            deliveries = over.get(
                "deliveries",
                []
            )

            for delivery_index, delivery in enumerate(
                deliveries
            ):

                runs = delivery.get(
                    "runs",
                    {}
                )

                extras = delivery.get(
                    "extras",
                    {}
                )

                if not isinstance(
                    runs,
                    dict
                ):
                    runs = {}

                if not isinstance(
                    extras,
                    dict
                ):
                    extras = {}

                batter_runs = to_int(
                    runs.get("batter")
                )

                extra_runs = to_int(
                    runs.get("extras")
                )

                total_runs = to_int(
                    runs.get("total")
                )

                wickets = delivery.get(
                    "wickets",
                    []
                )

                if not isinstance(
                    wickets,
                    list
                ):
                    wickets = []

                # ------------------------------------------------
                # Wicket handling
                # ------------------------------------------------
                #
                # A delivery can contain more than one wicket.
                #
                # The agreed Silver grain is one delivery/event.
                # Therefore we preserve the first wicket in the
                # delivery-level columns.
                #
                # event_id still identifies the delivery itself.
                # ------------------------------------------------

                wicket_kind = None
                player_out = None

                if wickets:
                    first_wicket = wickets[0]

                    if isinstance(
                        first_wicket,
                        dict
                    ):
                        wicket_kind = first_wicket.get(
                            "kind"
                        )

                        player_out = first_wicket.get(
                            "player_out"
                        )

                delivery_number = delivery.get(
                    "actual_delivery"
                )

                delivery_number = to_string(
                    delivery_number
                )

                # ------------------------------------------------
                # CRITICAL EVENT ID
                # ------------------------------------------------
                #
                # delivery_number such as 0.3 is NOT a reliable
                # unique key.
                #
                # We use the actual array positions.
                #
                event_id = (
                    f"{match_id}_"
                    f"{innings_number}_"
                    f"{over_index}_"
                    f"{delivery_index}"
                )

                rows.append({
                    "match_id": to_string(
                        match_id
                    ),

                    "innings_number":
                        innings_number,

                    "batting_team":
                        to_string(
                            batting_team
                        ),

                    "over_index":
                        over_index,

                    "delivery_index":
                        delivery_index,

                    "over_number":
                        over_number,

                    "delivery_number":
                        delivery_number,

                    "batter":
                        to_string(
                            delivery.get(
                                "batter"
                            )
                        ),

                    "bowler":
                        to_string(
                            delivery.get(
                                "bowler"
                            )
                        ),

                    "non_striker":
                        to_string(
                            delivery.get(
                                "non_striker"
                            )
                        ),

                    "batter_runs":
                        batter_runs,

                    "extra_runs":
                        extra_runs,

                    "total_runs":
                        total_runs,

                    "extra_byes":
                        to_float(
                            extras.get(
                                "byes"
                            )
                        ),

                    "extra_legbyes":
                        to_float(
                            extras.get(
                                "legbyes"
                            )
                        ),

                    "extra_noballs":
                        to_float(
                            extras.get(
                                "noballs"
                            )
                        ),

                    "extra_wides":
                        to_float(
                            extras.get(
                                "wides"
                            )
                        ),

                    "extra_penalty":
                        to_float(
                            extras.get(
                                "penalty"
                            )
                        ),

                    "wicket_kind":
                        to_string(
                            wicket_kind
                        ),

                    "player_out":
                        to_string(
                            player_out
                        ),

                    "event_id":
                        event_id,
                })

    return rows


# ============================================================
# VALIDATION BEFORE WRITING
# ============================================================

def validate_match_rows(rows):
    if not rows:
        raise ValueError(
            "No match rows generated."
        )

    match_ids = [
        row["match_id"]
        for row in rows
    ]

    if any(
        value is None
        for value in match_ids
    ):
        raise ValueError(
            "match_id contains null values."
        )

    if len(match_ids) != len(
        set(match_ids)
    ):
        raise ValueError(
            "Duplicate match_id detected."
        )


def validate_delivery_rows(rows):
    if not rows:
        raise ValueError(
            "No delivery rows generated."
        )

    event_ids = [
        row["event_id"]
        for row in rows
    ]

    if any(
        value is None
        for value in event_ids
    ):
        raise ValueError(
            "event_id contains null values."
        )

    if len(event_ids) != len(
        set(event_ids)
    ):
        raise ValueError(
            "Duplicate event_id detected."
        )

    # Validate the run relationship.
    for row in rows:
        batter_runs = row["batter_runs"]
        extra_runs = row["extra_runs"]
        total_runs = row["total_runs"]

        if (
            batter_runs is not None
            and extra_runs is not None
            and total_runs is not None
        ):
            if (
                batter_runs + extra_runs
                != total_runs
            ):
                raise ValueError(
                    "Run mismatch detected "
                    f"for event_id "
                    f"{row['event_id']}: "
                    f"{batter_runs} + "
                    f"{extra_runs} != "
                    f"{total_runs}"
                )


# ============================================================
# PARQUET WRITING
# ============================================================

def write_matches_parquet(rows, path):
    table = pa.Table.from_pylist(
        rows,
        schema=MATCHES_SCHEMA,
    )

    pq.write_table(
        table,
        path,
    )


def write_deliveries_parquet(rows, path):
    table = pa.Table.from_pylist(
        rows,
        schema=DELIVERIES_SCHEMA,
    )

    pq.write_table(
        table,
        path,
    )


# ============================================================
# S3 UPLOAD
# ============================================================

def upload_file(local_path, s3_key):
    print(
        f"Uploading: "
        f"s3://{BUCKET}/{s3_key}"
    )

    s3.upload_file(
        str(local_path),
        BUCKET,
        s3_key,
    )

    # Verify that S3 can see the object.
    s3.head_object(
        Bucket=BUCKET,
        Key=s3_key,
    )

    print("Upload verified.")


# ============================================================
# PROCESS ONE BATCH
# ============================================================

def process_batch(
    match_type,
    batch_number,
    s3_keys,
):
    print()
    print("=" * 80)
    print(
        f"{match_type.upper()} BATCH "
        f"{batch_number}"
    )
    print("=" * 80)

    start_index = (
        (batch_number - 1)
        * BATCH_SIZE
    )

    end_index = min(
        start_index + BATCH_SIZE,
        len(s3_keys),
    )

    batch_keys = s3_keys[
        start_index:end_index
    ]

    print(
        f"Matches in batch: "
        f"{len(batch_keys)}"
    )

    batch_root = (
        WORK_ROOT
        / match_type
        / f"batch_{batch_number:04d}"
    )

    json_dir = batch_root / "json"
    output_dir = batch_root / "output"

    # Clean stale work from a previously failed run.
    if batch_root.exists():
        shutil.rmtree(batch_root)

    json_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    match_rows = []
    delivery_rows = []

    # --------------------------------------------------------
    # Download + transform
    # --------------------------------------------------------

    for index, s3_key in enumerate(
        batch_keys,
        start=1,
    ):

        filename = Path(s3_key).name

        local_json = (
            json_dir / filename
        )

        download_json(
            s3_key,
            local_json,
        )

        with open(
            local_json,
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        match_id = Path(
            filename
        ).stem

        match_rows.append(
            transform_match(
                match_id,
                data,
            )
        )

        delivery_rows.extend(
            transform_deliveries(
                match_id,
                data,
            )
        )

        if (
            index % 50 == 0
            or index == len(batch_keys)
        ):
            print(
                f"Processed "
                f"{index}/{len(batch_keys)}"
            )

    print()
    print(
        f"Match rows: "
        f"{len(match_rows)}"
    )

    print(
        f"Delivery rows: "
        f"{len(delivery_rows)}"
    )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    print()
    print("Validating transformed data...")

    validate_match_rows(
        match_rows
    )

    validate_delivery_rows(
        delivery_rows
    )

    print("Validation passed.")

    # --------------------------------------------------------
    # Local Parquet
    # --------------------------------------------------------

    matches_path = (
        output_dir
        / f"matches_batch_"
        f"{batch_number:04d}.parquet"
    )

    deliveries_path = (
        output_dir
        / f"deliveries_batch_"
        f"{batch_number:04d}.parquet"
    )

    write_matches_parquet(
        match_rows,
        matches_path,
    )

    write_deliveries_parquet(
        delivery_rows,
        deliveries_path,
    )

    print()
    print(
        "Local Parquet files created."
    )

    # --------------------------------------------------------
    # S3 destinations
    # --------------------------------------------------------

    matches_s3_key = (
        SILVER_PREFIXES[match_type][
            "matches"
        ]
        + f"batch_{batch_number:04d}.parquet"
    )

    deliveries_s3_key = (
        SILVER_PREFIXES[match_type][
            "deliveries"
        ]
        + f"batch_{batch_number:04d}.parquet"
    )

    # Upload both.
    upload_file(
        matches_path,
        matches_s3_key,
    )

    upload_file(
        deliveries_path,
        deliveries_s3_key,
    )

    # --------------------------------------------------------
    # Cleanup only after successful uploads
    # --------------------------------------------------------

    shutil.rmtree(
        batch_root
    )

    print()
    print(
        f"{match_type.upper()} batch "
        f"{batch_number} "
        f"completed successfully."
    )


# ============================================================
# PROCESS DATASET
# ============================================================

def process_dataset(match_type):
    print()
    print("#" * 80)
    print(
        f"DISCOVERING "
        f"{match_type.upper()} BRONZE DATA"
    )
    print("#" * 80)

    prefix = BRONZE_PREFIXES[
        match_type
    ]

    s3_keys = list_s3_keys(
        prefix
    )

    print(
        f"{match_type.upper()} Bronze files: "
        f"{len(s3_keys)}"
    )

    if not s3_keys:
        print(
            "No Bronze files found."
        )
        return

    total_batches = (
        len(s3_keys)
        + BATCH_SIZE
        - 1
    ) // BATCH_SIZE

    print(
        f"Total batches: "
        f"{total_batches}"
    )

    manifest = load_manifest(
        match_type
    )

    end_batch = (
        total_batches
        if END_BATCH is None
        else min(
            END_BATCH,
            total_batches,
        )
    )

    print(
        f"Target batches: "
        f"{START_BATCH}-{end_batch}"
    )

    processed = 0
    skipped = 0

    for batch_number in range(
        START_BATCH,
        end_batch + 1,
    ):

        batch_key = str(
            batch_number
        )

        # ----------------------------------------------------
        # Checkpoint
        # ----------------------------------------------------

        if (
            manifest.get(batch_key)
            == "complete"
        ):
            print()
            print(
                f"Skipping completed "
                f"{match_type} batch "
                f"{batch_number}"
            )

            skipped += 1
            continue

        try:
            process_batch(
                match_type,
                batch_number,
                s3_keys,
            )

            # IMPORTANT:
            # Only mark complete AFTER BOTH
            # Parquet files have uploaded successfully.
            manifest[
                batch_key
            ] = "complete"

            save_manifest(
                match_type,
                manifest,
            )

            processed += 1

        except Exception as exc:
            print()
            print("=" * 80)
            print(
                f"FAILED: "
                f"{match_type} batch "
                f"{batch_number}"
            )
            print("=" * 80)

            print(
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print()
            print(
                "Manifest was NOT marked "
                "complete."
            )

            print(
                "The batch can be "
                "retried safely."
            )

            raise

    completed_total = sum(
        1
        for value in manifest.values()
        if value == "complete"
    )

    print()
    print("=" * 80)
    print(
        f"{match_type.upper()} DATASET COMPLETE"
    )
    print("=" * 80)

    print(
        f"Processed this run: "
        f"{processed}"
    )

    print(
        f"Skipped this run: "
        f"{skipped}"
    )

    print(
        f"Total completed batches: "
        f"{completed_total}/{total_batches}"
    )


# ============================================================
# MAIN
# ============================================================

def main():
    start_time = time.time()

    WORK_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    STATE_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 80)
    print(
        "CRICKET ODI + TEST SILVER "
        "TRANSFORMATION"
    )
    print("=" * 80)

    print()
    print(
        "Architecture:"
    )

    print(
        "S3 Bronze -> Python -> "
        "PyArrow -> S3 Silver"
    )

    print()
    print(
        "Spark is intentionally NOT used "
        "in this Bronze -> Silver stage."
    )

    # --------------------------------------------------------
    # ODI
    # --------------------------------------------------------

    process_dataset("odi")

    # --------------------------------------------------------
    # Test
    # --------------------------------------------------------

    process_dataset("test")

    elapsed = (
        time.time() - start_time
    )

    print()
    print("=" * 80)
    print(
        "PIPELINE COMPLETED SUCCESSFULLY"
    )
    print("=" * 80)

    print(
        f"Elapsed time: "
        f"{elapsed / 60:.2f} minutes"
    )


if __name__ == "__main__":
    main()