import os
import sys
import json
import re
import time
import shutil
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# WINDOWS / PYSPARK CONFIGURATION
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from pyspark.sql import SparkSession

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    ArrayType,
)

from pyspark.sql.functions import (
    col,
    posexplode,
    input_file_name,
    regexp_extract,
    concat_ws,
)


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET = "sports-intel-platform-data"

BRONZE_PREFIX = "bronze/cricket/cricsheet/t20is/"

SILVER_MATCHES_PREFIX = "silver/cricket/matches/"
SILVER_DELIVERIES_PREFIX = "silver/cricket/deliveries/"

BATCH_SIZE = 250

# We are correcting only these batches.
START_BATCH = 13
END_BATCH = 23

REPROCESS_BATCHES = set(range(START_BATCH, END_BATCH + 1))


# ============================================================
# LOCAL DIRECTORIES
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

WORK_DIR = (
    BASE_DIR
    / "transformation"
    / "cricket"
    / "work"
    / "t20i"
)

STAGING_DIR = WORK_DIR / "staging"
OUTPUT_DIR = WORK_DIR / "output"

STATE_DIR = (
    BASE_DIR
    / "transformation"
    / "cricket"
    / "state"
    / "t20i"
)

MANIFEST_PATH = STATE_DIR / "manifest.json"

STAGING_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# AWS
# ============================================================

s3 = boto3.client("s3")


# ============================================================
# EXPLICIT CRICSHEET SCHEMA
# ============================================================

delivery_wicket_schema = StructType([
    StructField("kind", StringType(), True),
    StructField("player_out", StringType(), True),
])


delivery_schema = StructType([
    StructField("actual_delivery", DoubleType(), True),

    StructField("batter", StringType(), True),
    StructField("bowler", StringType(), True),
    StructField("non_striker", StringType(), True),

    StructField(
        "runs",
        StructType([
            StructField("batter", LongType(), True),
            StructField("extras", LongType(), True),
            StructField("total", LongType(), True),
        ]),
        True,
    ),

    StructField(
        "extras",
        StructType([
            StructField("byes", LongType(), True),
            StructField("legbyes", LongType(), True),
            StructField("noballs", LongType(), True),
            StructField("wides", LongType(), True),
        ]),
        True,
    ),

    StructField(
        "wickets",
        ArrayType(delivery_wicket_schema),
        True,
    ),
])


over_schema = StructType([
    StructField("over", IntegerType(), True),

    StructField(
        "deliveries",
        ArrayType(delivery_schema),
        True,
    ),
])


innings_schema = StructType([
    StructField("team", StringType(), True),

    StructField(
        "overs",
        ArrayType(over_schema),
        True,
    ),
])


outcome_schema = StructType([
    StructField("winner", StringType(), True),

    StructField(
        "by",
        StructType([
            StructField("runs", LongType(), True),
            StructField("wickets", LongType(), True),
        ]),
        True,
    ),
])


toss_schema = StructType([
    StructField("winner", StringType(), True),
    StructField("decision", StringType(), True),
])


info_schema = StructType([
    StructField(
        "dates",
        ArrayType(StringType()),
        True,
    ),

    StructField("season", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("match_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("venue", StringType(), True),

    StructField(
        "teams",
        ArrayType(StringType()),
        True,
    ),

    StructField("toss", toss_schema, True),

    StructField("outcome", outcome_schema, True),

    StructField(
        "player_of_match",
        ArrayType(StringType()),
        True,
    ),
])


CRICSHEET_SCHEMA = StructType([
    StructField("info", info_schema, True),

    StructField(
        "innings",
        ArrayType(innings_schema),
        True,
    ),
])


# ============================================================
# MANIFEST
# ============================================================

def load_manifest():

    if not MANIFEST_PATH.exists():
        return {
            "completed_batches": [],
            "failed_batches": [],
        }

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(manifest):

    temp_path = MANIFEST_PATH.with_suffix(".tmp")

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(
            manifest,
            f,
            indent=2,
        )

    os.replace(temp_path, MANIFEST_PATH)


# ============================================================
# BRONZE DISCOVERY
# ============================================================

def discover_bronze_files():

    files = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=BRONZE_PREFIX,
    ):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if key.endswith(".json"):
                files.append(key)

    files.sort()

    return files


# ============================================================
# DOWNLOAD BATCH
# ============================================================

def download_batch(batch_files, batch_number):

    batch_dir = STAGING_DIR / f"batch_{batch_number:04d}"

    if batch_dir.exists():
        shutil.rmtree(batch_dir)

    batch_dir.mkdir(parents=True)

    local_files = []

    for key in batch_files:

        filename = Path(key).name

        local_path = batch_dir / filename

        print(f"Downloading: {key}")

        s3.download_file(
            BUCKET,
            key,
            str(local_path),
        )

        local_files.append(str(local_path))

    return local_files, batch_dir


# ============================================================
# SPARK TRANSFORMATION
# ============================================================

def transform_batch(
    spark,
    local_files,
    batch_number,
):

    print()
    print("=" * 70)
    print(f"SPARK TRANSFORMATION - BATCH {batch_number:04d}")
    print("=" * 70)

    print(f"Input JSON files: {len(local_files)}")

    # --------------------------------------------------------
    # Read JSON with explicit schema
    # --------------------------------------------------------

    df = (
        spark.read
        .schema(CRICSHEET_SCHEMA)
        .option("multiLine", "true")
        .json(local_files)
    )

    # --------------------------------------------------------
    # Extract match ID from source filename
    # --------------------------------------------------------

    df = df.withColumn(
        "match_id",
        regexp_extract(
            input_file_name(),
            r"(\d+)\.json$",
            1,
        ),
    )

    # ========================================================
    # MATCHES
    # ========================================================

    matches_df = df.select(

        col("match_id"),

        col("info.dates")
        .getItem(0)
        .alias("date"),

        col("info.season")
        .alias("season"),

        col("info.gender")
        .alias("gender"),

        col("info.match_type")
        .alias("match_type"),

        col("info.city")
        .alias("city"),

        col("info.venue")
        .alias("venue"),

        col("info.teams")
        .getItem(0)
        .alias("team_1"),

        col("info.teams")
        .getItem(1)
        .alias("team_2"),

        col("info.toss.winner")
        .alias("toss_winner"),

        col("info.toss.decision")
        .alias("toss_decision"),

        col("info.outcome.winner")
        .alias("winner"),

        col("info.outcome.by.runs")
        .alias("win_by_runs"),

        concat_ws(
            ", ",
            col("info.player_of_match"),
        ).alias("player_of_match"),
    )

    # ========================================================
    # INNINGS
    # ========================================================

    innings_df = (
        df
        .select(
            "match_id",
            posexplode(
                col("innings")
            ).alias(
                "innings_index",
                "innings",
            ),
        )
        .withColumn(
            "innings_number",
            col("innings_index") + 1,
        )
    )

    # ========================================================
    # OVERS
    # ========================================================

    overs_df = (
        innings_df
        .select(
            "match_id",
            "innings_number",

            col("innings.team")
            .alias("batting_team"),

            posexplode(
                col("innings.overs")
            ).alias(
                "over_index",
                "over",
            ),
        )
    )

    # ========================================================
    # DELIVERIES
    # ========================================================

    deliveries_df = (
        overs_df

        .select(
            "match_id",
            "innings_number",
            "batting_team",
            "over_index",

            posexplode(
                col("over.deliveries")
            ).alias(
                "delivery_index",
                "delivery",
            ),

            col("over.over")
            .cast("int")
            .alias("over_number"),
        )

        .select(
            "match_id",
            "innings_number",
            "batting_team",
            "over_index",
            "delivery_index",
            "over_number",

            # IMPORTANT:
            # Always store delivery identifier as STRING.
            col("delivery.actual_delivery")
            .cast("string")
            .alias("delivery_number"),

            col("delivery.batter")
            .alias("batter"),

            col("delivery.bowler")
            .alias("bowler"),

            col("delivery.non_striker")
            .alias("non_striker"),

            col("delivery.runs.batter")
            .alias("batter_runs"),

            col("delivery.runs.extras")
            .alias("extra_runs"),

            col("delivery.runs.total")
            .alias("total_runs"),

            col("delivery.extras.byes")
            .alias("extra_byes"),

            col("delivery.extras.legbyes")
            .alias("extra_legbyes"),

            col("delivery.extras.noballs")
            .alias("extra_noballs"),

            col("delivery.extras.wides")
            .alias("extra_wides"),

            col("delivery.wickets")
            .getItem(0)
            .getField("kind")
            .alias("wicket_kind"),

            col("delivery.wickets")
            .getItem(0)
            .getField("player_out")
            .alias("player_out"),
        )

        .withColumn(
            "event_id",
            concat_ws(
                "_",
                col("match_id"),
                col("innings_number"),
                col("over_index"),
                col("delivery_index"),
            ),
        )
    )

    # ========================================================
    # VALIDATION
    # ========================================================

    print()
    print("VALIDATING BATCH")

    match_count = matches_df.count()

    delivery_count = deliveries_df.count()

    distinct_event_count = (
        deliveries_df
        .select("event_id")
        .distinct()
        .count()
    )

    print(f"Matches: {match_count}")
    print(f"Deliveries: {delivery_count}")
    print(f"Distinct event IDs: {distinct_event_count}")

    if delivery_count != distinct_event_count:

        raise RuntimeError(
            "Event ID uniqueness validation failed."
        )

    # ========================================================
    # LOCAL OUTPUT PATHS
    # ========================================================

    matches_output = (
        OUTPUT_DIR
        / f"batch_{batch_number:04d}_matches.parquet"
    )

    deliveries_output = (
        OUTPUT_DIR
        / f"batch_{batch_number:04d}_deliveries.parquet"
    )

    matches_local_path = str(matches_output)
    deliveries_local_path = str(deliveries_output)

    # Remove previous local output if present

    if matches_output.exists():
        matches_output.unlink()

    if deliveries_output.exists():
        deliveries_output.unlink()

    # ========================================================
    # WRITE LOCAL PARQUET
    # ========================================================

    matches_pd = matches_df.toPandas()

    deliveries_pd = deliveries_df.toPandas()

    # --------------------------------------------------------
    # EXPLICIT MATCHES PARQUET SCHEMA
    # --------------------------------------------------------

    matches_schema = pa.schema([
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
        pa.field("win_by_runs", pa.int64()),
        pa.field("player_of_match", pa.string()),
    ])

    # --------------------------------------------------------
    # EXPLICIT DELIVERY PARQUET SCHEMA
    # --------------------------------------------------------

    deliveries_schema = pa.schema([
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
        pa.field("wicket_kind", pa.string()),
        pa.field("player_out", pa.string()),
        pa.field("event_id", pa.string()),
    ])

    # ========================================================
    # WRITE MATCHES
    # ========================================================

    pq.write_table(
        pa.Table.from_pandas(
            matches_pd,
            schema=matches_schema,
            preserve_index=False,
            safe=False,
        ),
        matches_local_path,
    )

    # ========================================================
    # WRITE DELIVERIES
    # ========================================================

    pq.write_table(
        pa.Table.from_pandas(
            deliveries_pd,
            schema=deliveries_schema,
            preserve_index=False,
            safe=False,
        ),
        deliveries_local_path,
    )

    print()
    print("Local Parquet created:")
    print(matches_output)
    print(deliveries_output)

    return matches_output, deliveries_output


# ============================================================
# S3 UPLOAD - FORCE OVERWRITE
# ============================================================

def upload_file(local_path, s3_key):

    print(f"Uploading / overwriting: {s3_key}")

    s3.upload_file(
        str(local_path),
        BUCKET,
        s3_key,
        ExtraArgs={
            "ServerSideEncryption": "AES256",
        },
    )

    # --------------------------------------------------------
    # Verify upload
    # --------------------------------------------------------

    response = s3.head_object(
        Bucket=BUCKET,
        Key=s3_key,
    )

    if response.get("ContentLength", 0) <= 0:

        raise RuntimeError(
            f"S3 upload verification failed: {s3_key}"
        )

    print(
        f"Verified S3 object: "
        f"{s3_key} "
        f"({response['ContentLength']} bytes)"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 70)
    print("CRICKET T20I SILVER CORRECTION")
    print("=" * 70)

    print()
    print(
        f"Regenerating batches "
        f"{START_BATCH} through {END_BATCH}"
    )

    # ========================================================
    # DISCOVER BRONZE
    # ========================================================

    bronze_files = discover_bronze_files()

    total_files = len(bronze_files)

    total_batches = (
        total_files + BATCH_SIZE - 1
    ) // BATCH_SIZE

    print()
    print(f"Bronze files: {total_files}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Total batches: {total_batches}")

    # ========================================================
    # LOAD MANIFEST
    # ========================================================

    manifest = load_manifest()

    completed_batches = set(
        manifest.get(
            "completed_batches",
            [],
        )
    )

    failed_batches = set(
        manifest.get(
            "failed_batches",
            [],
        )
    )

    # ========================================================
    # SPARK
    # ========================================================

    spark = None

    processed_this_run = 0
    skipped_this_run = 0

    try:

        spark = (
            SparkSession.builder
            .appName("CricketT20ITransformation")
            .master("local[*]")
            .config(
                "spark.sql.caseSensitive",
                "false",
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # ====================================================
        # PROCESS ONLY 13-23
        # ====================================================

        for batch_number in range(
            START_BATCH,
            END_BATCH + 1,
        ):

            batch_id = (
                f"batch_{batch_number:04d}"
            )

            # ------------------------------------------------
            # Correct 1-based batch calculation
            #
            # batch 1 = files 0-249
            # batch 2 = files 250-499
            # ...
            # batch 23 = final files
            # ------------------------------------------------

            start = (
                (batch_number - 1)
                * BATCH_SIZE
            )

            end = min(
                start + BATCH_SIZE,
                total_files,
            )

            batch_files = bronze_files[
                start:end
            ]

            if not batch_files:

                print()
                print(
                    f"WARNING: {batch_id} "
                    f"has no files. Skipping."
                )

                continue

            print()
            print("#" * 70)
            print(
                f"PROCESSING {batch_id}"
            )
            print("#" * 70)

            print(
                f"Files {start + 1} "
                f"to {end} "
                f"of {total_files}"
            )

            batch_dir = None

            try:

                # ============================================
                # DOWNLOAD
                # ============================================

                local_files, batch_dir = (
                    download_batch(
                        batch_files,
                        batch_number,
                    )
                )

                # ============================================
                # TRANSFORM
                # ============================================

                (
                    matches_output,
                    deliveries_output,
                ) = transform_batch(
                    spark,
                    local_files,
                    batch_number,
                )

                # ============================================
                # S3 KEYS
                # ============================================

                matches_key = (
                    f"{SILVER_MATCHES_PREFIX}"
                    f"batch_{batch_number:04d}.parquet"
                )

                deliveries_key = (
                    f"{SILVER_DELIVERIES_PREFIX}"
                    f"batch_{batch_number:04d}.parquet"
                )

                # ============================================
                # FORCE OVERWRITE
                # ============================================

                upload_file(
                    matches_output,
                    matches_key,
                )

                upload_file(
                    deliveries_output,
                    deliveries_key,
                )

                # ============================================
                # MARK COMPLETE
                # ============================================

                completed_batches.add(
                    batch_id
                )

                failed_batches.discard(
                    batch_id
                )

                manifest["completed_batches"] = sorted(
                    completed_batches
                )

                manifest["failed_batches"] = sorted(
                    failed_batches
                )

                save_manifest(manifest)

                processed_this_run += 1

                print()
                print(
                    f"{batch_id} COMPLETED"
                )

                # ============================================
                # CLEAN STAGING
                # ============================================

                if (
                    batch_dir
                    and batch_dir.exists()
                ):
                    shutil.rmtree(
                        batch_dir
                    )

                if matches_output.exists():
                    matches_output.unlink()

                if deliveries_output.exists():
                    deliveries_output.unlink()

            except Exception as e:

                print()
                print(
                    f"ERROR in {batch_id}: {e}"
                )

                failed_batches.add(
                    batch_id
                )

                manifest["failed_batches"] = sorted(
                    failed_batches
                )

                manifest["completed_batches"] = sorted(
                    completed_batches
                )

                save_manifest(manifest)

                print(
                    f"{batch_id} marked FAILED."
                )

                print(
                    "Pipeline stopped."
                )

                print(
                    "Run the script again to retry."
                )

                break

    finally:

        if spark is not None:
            spark.stop()

    # ========================================================
    # SUMMARY
    # ========================================================

    elapsed = (
        time.time() - start_time
    ) / 60

    print()
    print("=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)

    print(
        f"Bronze files: {total_files}"
    )

    print(
        f"Total batches: {total_batches}"
    )

    print(
        f"Batches targeted: "
        f"{START_BATCH}-{END_BATCH}"
    )

    print(
        f"Batches processed this run: "
        f"{processed_this_run}"
    )

    print(
        f"Batches skipped: "
        f"{skipped_this_run}"
    )

    print(
        f"Elapsed time: "
        f"{elapsed:.2f} minutes"
    )

    print(
        f"Total completed batches in manifest: "
        f"{len(completed_batches)}"
    )

    if failed_batches:

        print()
        print(
            "FAILED BATCHES:"
        )

        for batch in sorted(
            failed_batches
        ):
            print(
                f"  {batch}"
            )

    else:

        print()
        print(
            "BATCHES 13-23 "
            "REGENERATED SUCCESSFULLY."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()