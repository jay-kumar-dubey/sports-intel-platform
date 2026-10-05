import os
import sys
import json
import shutil
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIG
# ============================================================

BUCKET = "sports-intel-platform-data"

# Prototype: process only batch 1 first
START_BATCH = 1
END_BATCH = 1

FORMATS = {
    "T20I": {
        "matches_prefix": "silver/cricket/matches/",
        "deliveries_prefix": "silver/cricket/deliveries/",
    },
    "ODI": {
        "matches_prefix": "silver/cricket/odi/matches/",
        "deliveries_prefix": "silver/cricket/odi/deliveries/",
    },
    "Test": {
        "matches_prefix": "silver/cricket/test/matches/",
        "deliveries_prefix": "silver/cricket/test/deliveries/",
    },
}

STATE_DIR = "transformation/cricket/state_gold_player_match"
WORK_DIR = "transformation/cricket/work_gold_player_match"

S3 = boto3.client("s3")


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("SportsIntel-Gold-PlayerMatch")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# HELPERS
# ============================================================

def list_s3_parquets(prefix):
    """
    Return all parquet files under an S3 prefix.
    """

    paginator = S3.get_paginator("list_objects_v2")

    files = []

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".parquet"):
                files.append(key)

    return sorted(files)


def download_s3_file(s3_key, local_path):
    """
    Download one S3 file locally.
    """

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    S3.download_file(
        BUCKET,
        s3_key,
        local_path
    )


def download_batch(s3_keys, local_dir):
    """
    Download a list of S3 files.
    """

    os.makedirs(local_dir, exist_ok=True)

    local_files = []

    for key in s3_keys:

        filename = os.path.basename(key)

        local_path = os.path.join(
            local_dir,
            filename
        )

        download_s3_file(
            key,
            local_path
        )

        local_files.append(local_path)

    return local_files


def upload_file(local_path, s3_key):
    """
    Upload local file to S3.
    """

    S3.upload_file(
        local_path,
        BUCKET,
        s3_key
    )


def clean_directory(path):
    """
    Delete and recreate a directory.
    """

    if os.path.exists(path):
        shutil.rmtree(path)

    os.makedirs(path, exist_ok=True)


# ============================================================
# PLAYER MATCH STATS
# ============================================================

def transform_player_match_stats(
    matches_df,
    deliveries_df,
    cricket_format
):
    """
    Create one row per:

        format + match_id + player

    containing batting and bowling statistics.
    """

    # --------------------------------------------------------
    # MATCH TEAM INFORMATION
    # --------------------------------------------------------

    match_info = matches_df.select(
        "match_id",
        "date",
        "season",
        "gender",
        "match_type",
        "team_1",
        "team_2",
        "winner"
    )

    # --------------------------------------------------------
    # BATTERS
    # --------------------------------------------------------

    batting = (
        deliveries_df
        .groupBy(
            "match_id",
            "batting_team",
            "batter"
        )
        .agg(
            F.sum("batter_runs").alias("runs"),

            F.sum(
                F.when(
                    ~F.col("extra_wides").isNotNull(),
                    F.lit(0)
                ).otherwise(0)
            ).alias("_dummy")
        )
    )

    # --------------------------------------------------------
    # BATTER BALLS FACED
    #
    # A batter does NOT face:
    # - wides
    # - no-balls
    # --------------------------------------------------------

    batting_balls = (
        deliveries_df
        .filter(
            (F.coalesce(F.col("extra_wides"), F.lit(0)) == 0)
            &
            (F.coalesce(F.col("extra_noballs"), F.lit(0)) == 0)
        )
        .groupBy(
            "match_id",
            "batting_team",
            "batter"
        )
        .agg(
            F.count("*").alias("balls_faced"),

            F.sum(
                F.when(
                    F.col("batter_runs") == 4,
                    1
                ).otherwise(0)
            ).alias("fours"),

            F.sum(
                F.when(
                    F.col("batter_runs") == 6,
                    1
                ).otherwise(0)
            ).alias("sixes")
        )
    )

    batting = (
        batting
        .drop("_dummy")
        .join(
            batting_balls,
            on=[
                "match_id",
                "batting_team",
                "batter"
            ],
            how="left"
        )
        .withColumnRenamed(
            "batter",
            "player"
        )
        .withColumnRenamed(
            "batting_team",
            "team"
        )
        .withColumn(
            "balls_faced",
            F.coalesce(F.col("balls_faced"), F.lit(0))
        )
        .withColumn(
            "fours",
            F.coalesce(F.col("fours"), F.lit(0))
        )
        .withColumn(
            "sixes",
            F.coalesce(F.col("sixes"), F.lit(0))
        )
    )

    # --------------------------------------------------------
    # BOWLING
    # --------------------------------------------------------

    bowling_base = deliveries_df.withColumn(
        "legal_delivery",
        F.when(
            (F.coalesce(F.col("extra_wides"), F.lit(0)) == 0)
            &
            (F.coalesce(F.col("extra_noballs"), F.lit(0)) == 0),
            1
        ).otherwise(0)
    )

    bowling = (
    bowling_base
    .groupBy(
        "match_id",
        "batting_team",
        "bowler"
    )
    .agg(

        # Legal balls bowled
        F.sum("legal_delivery").alias(
            "balls_bowled"
        ),

        # Bowler-conceded runs
        F.sum(
            F.col("total_runs")
            - F.coalesce(F.col("extra_byes"), F.lit(0))
            - F.coalesce(F.col("extra_legbyes"), F.lit(0))
        ).alias(
            "runs_conceded"
        ),

        # Bowler-credit wickets
        F.sum(
            F.when(
                F.col("wicket_kind").isin(
                    "bowled",
                    "caught",
                    "caught and bowled",
                    "lbw",
                    "stumped",
                    "hit wicket"
                ),
                1
            ).otherwise(0)
        ).alias(
            "wickets"
        )
    )
    .withColumnRenamed(
        "bowler",
        "player"
    )
)
    # --------------------------------------------------------
    # DETERMINE BOWLER'S TEAM
    #
    # The bowler's team is the team that is NOT batting.
    # --------------------------------------------------------

    bowling = (
        bowling
        .join(
            match_info.select(
                "match_id",
                "team_1",
                "team_2"
            ),
            on="match_id",
            how="left"
        )
        .withColumn(
            "team",
            F.when(
                F.col("team_1") == F.col("batting_team"),
                F.col("team_2")
            ).otherwise(
                F.col("team_1")
            )
        )
        .drop(
            "team_1",
            "team_2"
        )
        .withColumnRenamed(
            "batting_team",
            "opponent"
        )
    )

    # --------------------------------------------------------
    # BATTER OPPONENT
    # --------------------------------------------------------

    batting = (
        batting
        .join(
            match_info.select(
                "match_id",
                "team_1",
                "team_2"
            ),
            on="match_id",
            how="left"
        )
        .withColumn(
            "opponent",
            F.when(
                F.col("team") == F.col("team_1"),
                F.col("team_2")
            ).otherwise(
                F.col("team_1")
            )
        )
        .drop(
            "team_1",
            "team_2"
        )
    )

    # --------------------------------------------------------
    # NORMALISE BATTING
    # --------------------------------------------------------

    batting = batting.select(
        "match_id",
        "player",
        "team",
        "opponent",
        "runs",
        "balls_faced",
        "fours",
        "sixes",
        F.lit(0).alias("balls_bowled"),
        F.lit(0).alias("runs_conceded"),
        F.lit(0).alias("wickets")
    )

    # --------------------------------------------------------
    # NORMALISE BOWLING
    # --------------------------------------------------------

    bowling = bowling.select(
        "match_id",
        "player",
        "team",
        "opponent",
        F.lit(0).alias("runs"),
        F.lit(0).alias("balls_faced"),
        F.lit(0).alias("fours"),
        F.lit(0).alias("sixes"),
        "balls_bowled",
        "runs_conceded",
        "wickets"
    )

    # --------------------------------------------------------
    # COMBINE BATTING + BOWLING
    # --------------------------------------------------------

    player_stats = (
        batting
        .unionByName(bowling)
        .groupBy(
            "match_id",
            "player",
            "team",
            "opponent"
        )
        .agg(
            F.sum("runs").alias("runs"),
            F.sum("balls_faced").alias("balls_faced"),
            F.sum("fours").alias("fours"),
            F.sum("sixes").alias("sixes"),
            F.sum("balls_bowled").alias("balls_bowled"),
            F.sum("runs_conceded").alias("runs_conceded"),
            F.sum("wickets").alias("wickets")
        )
    )

    # --------------------------------------------------------
    # ADD MATCH METADATA
    # --------------------------------------------------------

    player_stats = (
        player_stats
        .join(
            match_info,
            on="match_id",
            how="left"
        )
        .withColumn(
            "batting_strike_rate",
            F.when(
                F.col("balls_faced") > 0,
                F.round(
                    F.col("runs") * 100 /
                    F.col("balls_faced"),
                    2
                )
            )
        )
        .withColumn(
            "bowling_economy",
            F.when(
                F.col("balls_bowled") > 0,
                F.round(
                    F.col("runs_conceded") * 6 /
                    F.col("balls_bowled"),
                    2
                )
            )
        )
        .withColumn(
            "format",
            F.lit(cricket_format)
        )
    )

    # --------------------------------------------------------
    # FINAL COLUMN ORDER
    # --------------------------------------------------------

    return player_stats.select(
        "format",
        "match_id",
        "date",
        "season",
        "gender",
        "match_type",
        "player",
        "team",
        "opponent",
        "winner",

        "runs",
        "balls_faced",
        "fours",
        "sixes",
        "batting_strike_rate",

        "balls_bowled",
        "runs_conceded",
        "wickets",
        "bowling_economy"
    )


# ============================================================
# PROCESS ONE FORMAT
# ============================================================

def process_format(
    cricket_format,
    config,
    batch_number
):

    print()
    print("=" * 70)
    print(f"PROCESSING {cricket_format} - BATCH {batch_number}")
    print("=" * 70)

    matches_prefix = config["matches_prefix"]
    deliveries_prefix = config["deliveries_prefix"]

    # --------------------------------------------------------
    # DISCOVER SILVER FILES
    # --------------------------------------------------------

    match_keys = list_s3_parquets(
        matches_prefix
    )

    delivery_keys = list_s3_parquets(
        deliveries_prefix
    )

    print(
        f"Silver match files discovered: "
        f"{len(match_keys)}"
    )

    print(
        f"Silver delivery files discovered: "
        f"{len(delivery_keys)}"
    )

    if not match_keys:
        raise RuntimeError(
            f"No match Parquet files found for {cricket_format}"
        )

    if not delivery_keys:
        raise RuntimeError(
            f"No delivery Parquet files found for {cricket_format}"
        )

    # --------------------------------------------------------
    # SELECT BATCH
    # --------------------------------------------------------

    start = batch_number - 1

    match_batch = match_keys[
        start:start + 1
    ]

    delivery_batch = delivery_keys[
        start:start + 1
    ]

    if not match_batch:
        raise RuntimeError(
            f"No match batch {batch_number} "
            f"for {cricket_format}"
        )

    if not delivery_batch:
        raise RuntimeError(
            f"No delivery batch {batch_number} "
            f"for {cricket_format}"
        )

    print()
    print("Match file:")
    print(match_batch[0])

    print()
    print("Delivery file:")
    print(delivery_batch[0])

    # --------------------------------------------------------
    # LOCAL DIRECTORIES
    # --------------------------------------------------------

    format_dir = os.path.join(
        WORK_DIR,
        cricket_format.lower(),
        f"batch_{batch_number:04d}"
    )

    clean_directory(format_dir)

    # --------------------------------------------------------
    # DOWNLOAD
    # --------------------------------------------------------

    print()
    print("Downloading Silver files...")

    local_match_files = download_batch(
        match_batch,
        os.path.join(
            format_dir,
            "matches"
        )
    )

    local_delivery_files = download_batch(
        delivery_batch,
        os.path.join(
            format_dir,
            "deliveries"
        )
    )

    print("Download complete.")

    # --------------------------------------------------------
    # READ WITH SPARK
    # --------------------------------------------------------

    print()
    print("Reading Silver with Spark...")

    matches_df = spark.read.parquet(
        *local_match_files
    )

    deliveries_df = spark.read.parquet(
        *local_delivery_files
    )

    print(
        f"Matches: {matches_df.count()}"
    )

    print(
        f"Deliveries: {deliveries_df.count()}"
    )

    # --------------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------------

    print()
    print("Building player match statistics...")

    gold_df = transform_player_match_stats(
        matches_df,
        deliveries_df,
        cricket_format
    )

    print(
        f"Player match rows: "
        f"{gold_df.count()}"
    )

    # --------------------------------------------------------
    # SHOW SAMPLE
    # --------------------------------------------------------

    print()
    print("Sample Gold rows:")

    gold_df.orderBy(
        "match_id",
        "player"
    ).show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # WRITE LOCAL PARQUET
    # --------------------------------------------------------
    # --------------------------------------------------------
    # WRITE LOCAL PARQUET (PyArrow, avoids Windows Hadoop writer)
    # --------------------------------------------------------

    output_dir = os.path.join(
        format_dir,
        "output"
    )

    clean_directory(
        output_dir
    )

    print()
    print("Writing local Gold Parquet...")

    part_file = os.path.join(
        output_dir,
        f"batch_{batch_number:04d}.parquet"
    )

    gold_pdf = gold_df.toPandas()

    pq.write_table(
        pa.Table.from_pandas(
            gold_pdf,
            preserve_index=False
        ),
        part_file,
        compression="snappy"
    )

    print(f"Written: {part_file}")
    print(f"Rows: {len(gold_pdf)}")
    # --------------------------------------------------------
    # GOLD S3 PATH
    # --------------------------------------------------------

    output_key = (
        f"gold/cricket/player_match_stats/"
        f"format={cricket_format}/"
        f"batch_{batch_number:04d}.parquet"
    )

    print()
    print(
        f"Uploading Gold to: s3://{BUCKET}/{output_key}"
    )

    upload_file(
        part_file,
        output_key
    )

    print()
    print("UPLOAD SUCCESS")
    print(
        f"s3://{BUCKET}/{output_key}"
    )

    # --------------------------------------------------------
    # CLEANUP
    # --------------------------------------------------------

    shutil.rmtree(
        format_dir,
        ignore_errors=True
    )

    print()
    print(
        f"{cricket_format} BATCH "
        f"{batch_number} COMPLETE"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print("CRICKET GOLD - PLAYER MATCH STATS")
    print("=" * 70)

    print()
    print(
        f"Batch range: "
        f"{START_BATCH} -> {END_BATCH}"
    )

    for cricket_format, config in FORMATS.items():

        for batch_number in range(
            START_BATCH,
            END_BATCH + 1
        ):

            process_format(
                cricket_format,
                config,
                batch_number
            )

    print()
    print("=" * 70)
    print("GOLD PLAYER MATCH TRANSFORMATION COMPLETE")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()