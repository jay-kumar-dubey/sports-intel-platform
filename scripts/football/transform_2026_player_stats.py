"""
Football Silver: TheStatsAPI World Cup 2026 player statistics
    -> silver/football/player_match_stats/season=2026   (one row per player per match)

Run from anywhere:
    python scripts\\football\\transform_2026_player_stats.py
"""

from common import banner, get_spark, list_files, write_parquet
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE = "the-stats-api"
SEASON = 2026

INPUT_DIR = "data/bronze/football/the-stats-api/world-cup-2026/player-statistics"
OUTPUT_DIR = "data/silver/football/player_match_stats"

STR, INT, DBL, BOOL = T.StringType(), T.IntegerType(), T.DoubleType(), T.BooleanType()


def struct(*fields):
    return T.StructType([T.StructField(name, dtype, True) for name, dtype in fields])


# Explicit schema built from the real Bronze files (inspect_player_stats.py).
PLAYER = struct(
    ("player_id", STR),
    ("player_name", STR),
    ("team_id", STR),
    ("club_team_id", STR),
    ("position", STR),
    ("rating", DBL),
    ("started", BOOL),
    ("played", BOOL),
    ("minutes_played", INT),
    ("passing", struct(
        ("total_passes", INT), ("accurate_passes", INT), ("key_passes", INT),
        ("assists", INT), ("total_crosses", INT), ("accurate_crosses", INT),
        ("total_long_balls", INT), ("accurate_long_balls", INT),
    )),
    ("shooting", struct(
        ("goals", INT), ("total_shots", INT), ("shots_on_target", INT),
        ("shots_off_target", INT), ("blocked_shots", INT),
        ("big_chances_created", INT), ("expected_goals", DBL),
        ("expected_assists", DBL), ("np_expected_goals", DBL),
    )),
    ("duels", struct(
        ("duel_won", INT), ("duel_lost", INT), ("aerial_won", INT),
        ("challenge_lost", INT), ("won_contest", INT), ("dispossessed", INT),
    )),
    ("defending", struct(
        ("tackles", INT), ("interceptions", INT), ("clearances", INT),
        ("ball_recoveries", INT),
    )),
    ("goalkeeping", struct(("saves", INT))),
    ("general", struct(
        ("touches", INT), ("fouls", INT), ("was_fouled", INT), ("offsides", INT),
        ("yellow_cards", INT), ("red_cards", INT), ("possession_lost", INT),
        ("player_subbed_on", STR), ("player_subbed_off", STR),
    )),
)

SCHEMA = struct(("data", T.ArrayType(PLAYER)))

# target column -> path inside the exploded player struct `p`
COLUMNS = [
    ("player_id", "player_id"), ("player_name", "player_name"),
    ("team_id", "team_id"), ("club_team_id", "club_team_id"),
    ("position", "position"), ("rating", "rating"), ("started", "started"),
    ("played", "played"), ("minutes_played", "minutes_played"),
    ("total_passes", "passing.total_passes"), ("accurate_passes", "passing.accurate_passes"),
    ("key_passes", "passing.key_passes"), ("assists", "passing.assists"),
    ("total_crosses", "passing.total_crosses"), ("accurate_crosses", "passing.accurate_crosses"),
    ("total_long_balls", "passing.total_long_balls"),
    ("accurate_long_balls", "passing.accurate_long_balls"),
    ("goals", "shooting.goals"), ("total_shots", "shooting.total_shots"),
    ("shots_on_target", "shooting.shots_on_target"),
    ("shots_off_target", "shooting.shots_off_target"),
    ("blocked_shots", "shooting.blocked_shots"),
    ("big_chances_created", "shooting.big_chances_created"),
    ("expected_goals", "shooting.expected_goals"),
    ("expected_assists", "shooting.expected_assists"),
    ("np_expected_goals", "shooting.np_expected_goals"),
    ("duel_won", "duels.duel_won"), ("duel_lost", "duels.duel_lost"),
    ("aerial_won", "duels.aerial_won"), ("challenge_lost", "duels.challenge_lost"),
    ("won_contest", "duels.won_contest"), ("dispossessed", "duels.dispossessed"),
    ("tackles", "defending.tackles"), ("interceptions", "defending.interceptions"),
    ("clearances", "defending.clearances"), ("ball_recoveries", "defending.ball_recoveries"),
    ("saves", "goalkeeping.saves"), ("touches", "general.touches"),
    ("fouls", "general.fouls"), ("was_fouled", "general.was_fouled"),
    ("offsides", "general.offsides"), ("yellow_cards", "general.yellow_cards"),
    ("red_cards", "general.red_cards"), ("possession_lost", "general.possession_lost"),
    ("player_subbed_on", "general.player_subbed_on"),
    ("player_subbed_off", "general.player_subbed_off"),
]

ID_COLUMNS = ["source", "season", "match_id", "player_id", "player_name", "team_id"]
HARD_REQUIRED = ["source", "season", "match_id", "player_id", "team_id"]


def main():
    banner("FOOTBALL PLAYER MATCH STATS - BRONZE -> SILVER (2026)")

    files = list_files(INPUT_DIR, "*.json")

    if not files:
        raise FileNotFoundError(f"No JSON files found in {INPUT_DIR}")

    print(f"\nReading Bronze data...\nInput files: {len(files)}")

    spark = get_spark("FootballSilverPlayerMatchStats2026")

    # FAILFAST: a malformed value raises instead of silently becoming NULL.
    raw = (
        spark.read
        .schema(SCHEMA)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(files)
        .withColumn("_file", F.input_file_name())
    )

    print("\nTransforming...")

    df = (
        raw
        .withColumn("match_id", F.regexp_extract("_file", r"(mt_[A-Za-z0-9]+)\.json$", 1))
        .select("match_id", F.explode("data").alias("p"))
        .select(
            F.lit(SOURCE).alias("source"),
            F.lit(SEASON).alias("season"),
            "match_id",
            *[
                (F.upper(F.trim(F.col(f"p.{path}"))) if name == "position"
                 else F.col(f"p.{path}")).alias(name)
                for name, path in COLUMNS
            ],
        )
        .cache()
    )

    banner("VALIDATION", "-")

    rows = df.count()
    matches = df.select("match_id").distinct().count()
    players = df.select("player_id").distinct().count()

    print(f"Rows:             {rows:,}")
    print(f"Distinct matches: {matches}")
    print(f"Distinct players: {players:,}")

    if matches != len(files):
        raise RuntimeError(f"{len(files)} files but {matches} distinct match_ids - check filenames")

    bad_id = df.filter(~F.col("match_id").rlike(r"^mt_")).count()
    if bad_id:
        raise RuntimeError(f"{bad_id} rows have a match_id that was not extracted from the filename")

    nulls = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ID_COLUMNS]
    ).first().asDict()

    print("\nNull counts (identifiers):")
    for c in ID_COLUMNS:
        print(f"  {c:12s} {nulls[c]}")

    missing_required = {c: nulls[c] for c in HARD_REQUIRED if nulls[c]}
    if missing_required:
        raise RuntimeError(f"Required identifiers contain NULLs: {missing_required}")

    dupes = df.groupBy("match_id", "player_id").count().filter("count > 1").count()
    print(f"\nDuplicate (match_id, player_id) records: {dupes}")
    if dupes:
        df.groupBy("match_id", "player_id").count().filter("count > 1").show(20, False)
        raise RuntimeError("Grain violated: expected one row per player per match")

    odd = df.filter((F.col("minutes_played") > 0) & (F.col("played") == False)).count()  # noqa: E712
    print(f"Rows with minutes_played > 0 but played = false: {odd}")

    print("\nPosition values:")
    df.groupBy("position").count().orderBy(F.desc("count")).show(truncate=False)

    print("Rows per match (min / max):")
    df.groupBy("match_id").count().agg(F.min("count"), F.max("count")).show()

    print("Silver schema:")
    df.printSchema()

    banner("WRITING SILVER PARQUET", "-")
    write_parquet(df, OUTPUT_DIR, partition_col="season")

    banner("TRANSFORMATION COMPLETE")
    print("Upload with:")
    print(
        f"  aws s3 sync {OUTPUT_DIR.replace('/', chr(92))}\\season={SEASON} "
        f"s3://sports-intel-platform-data/silver/football/player_match_stats/season={SEASON}/"
    )

    spark.stop()


if __name__ == "__main__":
    main()