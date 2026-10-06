"""
Football Silver: TheStatsAPI World Cup 2026 matches
    -> silver/football/matches/season=2026   (unified matches schema)

The Bronze matches layout was not inspected yet, so this job adapts to what
is actually in the files: it unwraps {"data": [...]} / {"data": {...}} / bare
objects, and reads every field through get(), which returns NULL (never a fake
value) when the source does not contain that field.

Run:
    python scripts\\football\\transform_2026_matches.py
"""

from common import banner, get_spark, list_files, write_parquet
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE = "the-stats-api"
SEASON = 2026

INPUT_DIR = "data/bronze/football/the-stats-api/world-cup-2026/matches"
OUTPUT_DIR = "data/silver/football/matches"


def field_type(schema, dotted):
    t = schema
    for part in dotted.split("."):
        if not isinstance(t, T.StructType) or part not in t.names:
            return None
        t = t[part].dataType
    return t


def get(df, dotted, cast=None):
    """Column for a (nested) leaf field, or a typed NULL if the source lacks it."""

    t = field_type(df.schema, dotted)

    if t is None or isinstance(t, (T.StructType, T.ArrayType)):
        return F.lit(None).cast(cast or "string")

    col = F.col(dotted)

    return col.cast(cast) if cast else col


def unwrap(df):
    if "data" in df.columns:
        dt = df.schema["data"].dataType

        if isinstance(dt, T.ArrayType):
            return df.select(F.explode("data").alias("m")).select("m.*")

        if isinstance(dt, T.StructType):
            return df.select("data.*")

    return df


def main():
    banner("FOOTBALL MATCHES - BRONZE -> SILVER (2026)")

    spark = get_spark("FootballSilverMatches2026")

    files = list_files(INPUT_DIR, "*.json")

    if not files:
        raise FileNotFoundError(f"No JSON files found in {INPUT_DIR}")

    print(f"Input files: {len(files)}")

    raw = spark.read.option("multiLine", True).json(files)

    df = unwrap(raw)

    if "id" not in df.columns:
        raise RuntimeError(f"No 'id' column after unwrapping. Columns: {df.columns}")

    print("\nBronze match structure detected:")
    df.printSchema()

    ts = get(df, "utc_date").cast("timestamp")

    home = get(df, "score.home", "int")
    away = get(df, "score.away", "int")
    pens = F.coalesce(get(df, "score.went_to_penalties", "boolean"), F.lit(False))
    status = get(df, "status")
    winner_src = F.lower(get(df, "score.winner"))

    winner = F.when(winner_src.isin("home", "away", "draw"), winner_src).when(
        (status == "finished") & home.isNotNull() & (home == away) & (~pens),
        F.lit("draw"),
    )

    silver = (
        df.select(
            F.lit(SOURCE).alias("source"),
            F.lit(SEASON).alias("season"),
            get(df, "id").alias("match_id"),
            get(df, "competition_id").alias("competition_id"),
            get(df, "season_id").alias("season_id"),
            F.to_date(ts).alias("match_date"),
            F.date_format(ts, "HH:mm:ss").alias("kick_off"),
            get(df, "matchday", "int").alias("matchday"),
            get(df, "stage_name").alias("stage_name"),
            get(df, "group_label").alias("group_label"),
            get(df, "home_team.id").alias("home_team_id"),
            get(df, "home_team.name").alias("home_team_name"),
            get(df, "away_team.id").alias("away_team_id"),
            get(df, "away_team.name").alias("away_team_name"),
            home.alias("home_score"),
            away.alias("away_score"),
            get(df, "score.regulation.home", "int").alias("home_regulation_score"),
            get(df, "score.regulation.away", "int").alias("away_regulation_score"),
            get(df, "score.after_extra_time.home", "int").alias(
                "home_extra_time_score"
            ),
            get(df, "score.after_extra_time.away", "int").alias(
                "away_extra_time_score"
            ),
            get(df, "score.penalty_shootout.home", "int").alias("home_penalty_score"),
            get(df, "score.penalty_shootout.away", "int").alias("away_penalty_score"),
            get(df, "score.went_to_extra_time", "boolean").alias("went_to_extra_time"),
            get(df, "score.went_to_penalties", "boolean").alias("went_to_penalties"),
            winner.alias("winner"),
            get(df, "is_neutral", "boolean").alias("is_neutral"),
            status.alias("status"),
        )
        .withColumn(
            "winner_team_id",
            F.when(F.col("winner") == "home", F.col("home_team_id")).when(
                F.col("winner") == "away", F.col("away_team_id")
            ),
        )
        .withColumn(
            "goal_difference",
            F.when(
                F.col("home_score").isNotNull() & F.col("away_score").isNotNull(),
                F.col("home_score") - F.col("away_score"),
            ),
        )
        .withColumn(
            "is_draw",
            F.when(F.col("winner").isNotNull(), F.col("winner") == "draw"),
        )
        .cache()
    )

    banner("VALIDATION", "-")

    rows = silver.count()
    distinct = silver.select("match_id").distinct().count()

    print(f"Rows:               {rows}")
    print(f"Distinct match_ids: {distinct}")

    null_ids = silver.filter(F.col("match_id").isNull()).count()
    if null_ids:
        raise RuntimeError(f"{null_ids} matches have no id")

    if rows != distinct:
        print(
            f"WARNING: {rows - distinct} duplicate match_ids - keeping one row per match_id"
        )
        silver = silver.dropDuplicates(["match_id"])

    print("\nStatus:")
    silver.groupBy("status").count().show(truncate=False)

    print("Stages:")
    silver.groupBy("stage_name").count().orderBy(F.desc("count")).show(truncate=False)

    print("Winner values:")
    silver.groupBy("winner").count().show(truncate=False)

    print("Null counts (key fields):")
    keys = [
        "home_team_id",
        "away_team_id",
        "home_score",
        "away_score",
        "stage_name",
        "match_date",
    ]
    silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in keys]).show()

    banner("WRITING SILVER PARQUET", "-")
    write_parquet(silver, OUTPUT_DIR, partition_col="season")

    banner("TRANSFORMATION COMPLETE")
    print("Upload with:")
    print(
        f"  aws s3 sync data\\silver\\football\\matches\\season={SEASON} "
        f"s3://sports-intel-platform-data/silver/football/matches/season={SEASON}/"
    )

    spark.stop()


if __name__ == "__main__":
    main()
