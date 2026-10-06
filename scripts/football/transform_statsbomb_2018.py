"""
Football Silver: StatsBomb World Cup 2018
Bronze -> Silver

Reads the StatsBomb top-level JSON arrays with Spark's inferred schema.
Every nested field is resolved through pick(), which tries several candidate
paths and falls back to NULL, so a different StatsBomb field layout can no
longer crash the job with FIELD_NOT_FOUND.

Run:
    python scripts\\football\\transform_statsbomb_2018.py
"""

from common import banner, get_spark, list_files, write_parquet
from pyspark.sql import functions as F
from pyspark.sql import types as T


SOURCE = "statsbomb"
SEASON = 2018

BASE_DIR = "data/bronze/football/statsbomb/world-cup/2018"
EVENTS_DIR = f"{BASE_DIR}/events"
LINEUPS_DIR = f"{BASE_DIR}/lineups"
MATCHES_FILE = f"{BASE_DIR}/matches/matches.json"

MATCHES_OUT = "data/silver/football/matches"
TEAMS_OUT = "data/silver/football/teams"
PLAYERS_OUT = "data/silver/football/players"
EVENTS_OUT = "data/silver/football/events"
LINEUPS_OUT = "data/silver/football/lineups"
PLAYER_STATS_OUT = "data/silver/football/player_match_stats"

INT = T.IntegerType()
DBL = T.DoubleType()
BOOL = T.BooleanType()
STR = T.StringType()

# Match JSON in this dataset uses prefixed names, e.g. home_team.home_team_id.
# Event data: period 5 is the penalty shootout.
SHOOTOUT_PERIOD = 5


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def null_int():
    return F.lit(None).cast(INT)


def null_double():
    return F.lit(None).cast(DBL)


def has(df, path):
    """True if a (possibly nested) column path resolves in df."""
    try:
        df.select(path)
        return True
    except Exception:
        return False


def pick(df, paths, dtype=None):
    """First resolvable path as a Column (optionally cast), else typed NULL."""
    if isinstance(paths, str):
        paths = [paths]
    for p in paths:
        if has(df, p):
            c = F.col(p)
            return c.cast(dtype) if dtype is not None else c
    return F.lit(None).cast(dtype if dtype is not None else STR)


def cnt(condition):
    """Count rows matching a condition as an INT."""
    return F.sum(F.when(condition, 1).otherwise(0)).cast(INT)


def match_id_from_file(col_name="_file"):
    return F.regexp_extract(col_name, r"([0-9]+)\.json$", 1).cast(INT)


def main():
    banner("FOOTBALL STATSBOMB 2018 - BRONZE -> SILVER")

    event_files = list_files(EVENTS_DIR, "*.json")
    lineup_files = list_files(LINEUPS_DIR, "*.json")

    if not event_files:
        raise FileNotFoundError(f"No event JSON files found in {EVENTS_DIR}")
    if not lineup_files:
        raise FileNotFoundError(f"No lineup JSON files found in {LINEUPS_DIR}")

    print(f"\nEvent files:  {len(event_files)}")
    print(f"Lineup files: {len(lineup_files)}")
    print(f"Matches file: {MATCHES_FILE}")

    spark = get_spark("FootballSilverStatsBomb2018")

    # ------------------------------------------------------------------
    # EVENTS
    # ------------------------------------------------------------------
    banner("READING EVENTS", "-")

    raw_events = (
        spark.read
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(event_files)
        .withColumn("_file", F.input_file_name())
        .withColumn("match_id", match_id_from_file())
    )
    r = raw_events

    events = (
        r.select(
            F.lit(SOURCE).alias("source"),
            F.lit(SEASON).alias("season"),
            "match_id",
            pick(r, "id").alias("event_id"),
            pick(r, "index", INT).alias("event_index"),
            pick(r, "period", INT).alias("period"),
            pick(r, "timestamp").alias("timestamp"),
            pick(r, "minute", INT).alias("minute"),
            pick(r, "second", INT).alias("second"),
            pick(r, "type.id", INT).alias("event_type_id"),
            pick(r, "type.name").alias("event_type"),
            pick(r, "team.id", INT).alias("team_id"),
            pick(r, "team.name").alias("team_name"),
            pick(r, "player.id", INT).alias("player_id"),
            pick(r, "player.name").alias("player_name"),
            pick(r, "position.id", INT).alias("position_id"),
            pick(r, "position.name").alias("position"),
            pick(r, "possession", INT).alias("possession"),
            pick(r, "possession_team.id", INT).alias("possession_team_id"),
            pick(r, "possession_team.name").alias("possession_team_name"),
            pick(r, "play_pattern.id", INT).alias("play_pattern_id"),
            pick(r, "play_pattern.name").alias("play_pattern"),
            pick(r, "pass.recipient.id", INT).alias("pass_recipient_id"),
            pick(r, "pass.recipient.name").alias("pass_recipient_name"),
            pick(r, "pass.outcome.id", INT).alias("pass_outcome_id"),
            pick(r, "pass.outcome.name").alias("pass_outcome"),
            pick(r, "pass.goal_assist", BOOL).alias("goal_assist"),
            pick(r, "pass.assisted_shot_id").alias("assisted_shot_id"),
            pick(r, "pass.through_ball", BOOL).alias("through_ball"),
            pick(r, "pass.cross", BOOL).alias("cross"),
            pick(r, "pass.shot_assist", BOOL).alias("shot_assist"),
            pick(r, "pass.type.name").alias("pass_type"),
            pick(r, "pass.length", DBL).alias("pass_length"),
            pick(r, "shot.statsbomb_xg", DBL).alias("xg"),
            pick(r, "shot.outcome.id", INT).alias("shot_outcome_id"),
            pick(r, "shot.outcome.name").alias("shot_outcome"),
            pick(r, "shot.key_pass_id").alias("key_pass_id"),
            pick(r, "duel.type.id", INT).alias("duel_type_id"),
            pick(r, "duel.type.name").alias("duel_type"),
            pick(r, "duel.outcome.id", INT).alias("duel_outcome_id"),
            pick(r, "duel.outcome.name").alias("duel_outcome"),
            pick(r, "interception.outcome.id", INT).alias("interception_outcome_id"),
            pick(r, "interception.outcome.name").alias("interception_outcome"),
            pick(r, "goalkeeper.type.id", INT).alias("goalkeeper_type_id"),
            pick(r, "goalkeeper.type.name").alias("goalkeeper_type"),
            pick(r, "goalkeeper.outcome.id", INT).alias("goalkeeper_outcome_id"),
            pick(r, "goalkeeper.outcome.name").alias("goalkeeper_outcome"),
            pick(r, "substitution.replacement.id", INT).alias("replacement_player_id"),
            pick(r, "substitution.replacement.name").alias("replacement_player_name"),
            pick(r, "substitution.outcome.id", INT).alias("substitution_outcome_id"),
            pick(r, "substitution.outcome.name").alias("substitution_outcome"),
        )
        .cache()
    )

    # Match-level info from event periods:
    # 1/2 = regulation, 3/4 = extra time, 5 = penalty shootout.
    match_info = (
        events
        .groupBy("match_id")
        .agg(F.max("period").alias("max_period"))
        .withColumn(
            "match_duration",
            F.when(F.col("max_period") >= 3, F.lit(120.0)).otherwise(F.lit(90.0)),
        )
        .withColumn("went_to_extra_time", F.col("max_period") >= 3)
        .withColumn("went_to_penalties", F.col("max_period") >= SHOOTOUT_PERIOD)
    )

    # ------------------------------------------------------------------
    # MATCHES
    # ------------------------------------------------------------------
    banner("READING MATCHES", "-")

    m = (
        spark.read
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(MATCHES_FILE)
    )

    matches = (
        m.select(
            F.lit(SOURCE).alias("source"),
            F.lit(SEASON).alias("season"),
            pick(m, "match_id", INT).alias("match_id"),
            F.to_date(pick(m, "match_date")).alias("match_date"),
            pick(m, "kick_off").alias("kick_off"),
            pick(m, "match_week", INT).alias("match_week"),
            F.lit(None).cast(STR).alias("matchday"),
            pick(m, "competition.competition_id", INT).alias("competition_id"),
            pick(m, "competition.competition_name").alias("competition_name"),
            pick(m, "season.season_id", INT).alias("season_id"),
            pick(m, "season.season_name").alias("season_name"),
            pick(m, ["competition_stage.id"], INT).alias("stage_id"),
            pick(m, ["competition_stage.name"]).alias("stage_name"),
            F.lit(None).cast(STR).alias("group_label"),
            pick(m, ["home_team.home_team_id", "home_team.id"], INT).alias("home_team_id"),
            pick(m, ["home_team.home_team_name", "home_team.name"]).alias("home_team_name"),
            pick(m, ["away_team.away_team_id", "away_team.id"], INT).alias("away_team_id"),
            pick(m, ["away_team.away_team_name", "away_team.name"]).alias("away_team_name"),
            pick(m, "home_score", INT).alias("home_score"),
            pick(m, "away_score", INT).alias("away_score"),
            # StatsBomb match JSON only has the final score; regulation / ET /
            # penalty splits are not provided, so they stay NULL.
            null_int().alias("home_score_regulation"),
            null_int().alias("away_score_regulation"),
            null_int().alias("home_score_extra_time"),
            null_int().alias("away_score_extra_time"),
            null_int().alias("home_score_penalties"),
            null_int().alias("away_score_penalties"),
            F.lit(True).alias("is_neutral"),
            pick(m, "match_status").alias("status"),
        )
        .dropDuplicates(["match_id"])
        .withColumn(
            "winner",
            F.when(F.col("home_score") > F.col("away_score"), F.lit("HOME"))
             .when(F.col("away_score") > F.col("home_score"), F.lit("AWAY"))
             .otherwise(F.lit("DRAW")),
        )
        .join(
            match_info.select("match_id", "went_to_extra_time", "went_to_penalties"),
            "match_id",
            "left",
        )
        .cache()
    )

    # ------------------------------------------------------------------
    # LINEUPS
    # ------------------------------------------------------------------
    banner("READING LINEUPS", "-")

    raw_lineups = (
        spark.read
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(lineup_files)
        .withColumn("_file", F.input_file_name())
        .withColumn("match_id", match_id_from_file())
        .withColumn("lp", F.explode("lineup"))
    )
    L = raw_lineups

    # StatsBomb lineups are flat (lp.player_id, lp.player_name, ...);
    # the nested variant (lp.player.id) is also accepted.
    lineups = (
        L.select(
            F.lit(SOURCE).alias("source"),
            F.lit(SEASON).alias("season"),
            "match_id",
            pick(L, "team_id", INT).alias("team_id"),
            pick(L, "team_name").alias("team_name"),
            pick(L, ["lp.player_id", "lp.player.id"], INT).alias("player_id"),
            pick(L, ["lp.player_name", "lp.player.name"]).alias("player_name"),
            pick(L, ["lp.player_nickname", "lp.player.nickname"]).alias("player_nickname"),
            pick(L, "lp.jersey_number", INT).alias("jersey_number"),
            pick(L, ["lp.positions.position", "lp.positions.position.name"]).alias("positions_raw"),
        )
        .withColumn("position", F.element_at(F.col("positions_raw"), 1))
        .drop("positions_raw")
        .filter(F.col("player_id").isNotNull())
        .dropDuplicates(["match_id", "player_id"])
        .cache()
    )

    # ------------------------------------------------------------------
    # STARTERS / SUBSTITUTIONS
    # ------------------------------------------------------------------
    banner("BUILDING PLAYER MATCH STATS", "-")

    starting_players = (
        raw_events
        .filter(F.col("type.name") == "Starting XI")
        .withColumn("starter", F.explode("tactics.lineup"))
        .select(
            "match_id",
            F.col("starter.player.id").cast(INT).alias("player_id"),
            F.col("starter.position.name").alias("starting_position"),
        )
        .dropDuplicates(["match_id", "player_id"])
    )

    subs = (
        events
        .filter(F.col("event_type") == "Substitution")
        .withColumn(
            "sub_time",
            F.col("minute").cast(DBL) + F.col("second").cast(DBL) / F.lit(60.0),
        )
    )

    substitutions_out = (
        subs.filter(F.col("player_id").isNotNull())
        .groupBy("match_id", "player_id")
        .agg(F.min("sub_time").alias("sub_off_time"))
    )

    substitutions_in = (
        subs.filter(F.col("replacement_player_id").isNotNull())
        .groupBy("match_id", "replacement_player_id")
        .agg(F.min("sub_time").alias("sub_on_time"))
        .withColumnRenamed("replacement_player_id", "player_id")
    )

    # ------------------------------------------------------------------
    # EVENT-LEVEL AGGREGATIONS (penalty shootout excluded)
    # ------------------------------------------------------------------
    play_events = events.filter(
        F.col("player_id").isNotNull() & (F.col("period") < SHOOTOUT_PERIOD)
    )

    is_pass = F.col("event_type") == "Pass"
    is_shot = F.col("event_type") == "Shot"
    is_duel = F.col("event_type") == "Duel"
    complete = F.col("pass_outcome").isNull()
    is_cross = F.coalesce(F.col("cross"), F.lit(False))
    is_long = F.col("pass_type") == "Long Ball"

    stats = (
        play_events
        .groupBy("match_id", "player_id")
        .agg(
            F.first("player_name", ignorenulls=True).alias("event_player_name"),
            F.first("position", ignorenulls=True).alias("event_position"),

            cnt(is_pass).alias("total_passes"),
            cnt(is_pass & complete).alias("accurate_passes"),
            cnt(is_pass & (F.coalesce(F.col("shot_assist"), F.lit(False))
                           | F.coalesce(F.col("goal_assist"), F.lit(False)))).alias("key_passes"),
            cnt(F.coalesce(F.col("goal_assist"), F.lit(False))).alias("assists"),
            cnt(is_pass & is_cross).alias("total_crosses"),
            cnt(is_pass & is_cross & complete).alias("accurate_crosses"),
            cnt(is_pass & is_long).alias("total_long_balls"),
            cnt(is_pass & is_long & complete).alias("accurate_long_balls"),

            cnt(is_shot).alias("total_shots"),
            cnt(is_shot & F.col("shot_outcome").isin("Goal", "Saved", "Saved to Post")).alias("shots_on_target"),
            cnt(is_shot & F.col("shot_outcome").isin("Off T", "Wayward", "Post")).alias("shots_off_target"),
            cnt(is_shot & (F.col("shot_outcome") == "Blocked")).alias("blocked_shots"),
            cnt(is_shot & (F.col("shot_outcome") == "Goal")).alias("goals"),
            F.sum(F.when(is_shot, F.col("xg")).otherwise(F.lit(0.0))).alias("expected_goals"),

            cnt(is_duel & F.col("duel_outcome").isin("Won", "Success In Play", "Success Out")).alias("duel_won"),
            cnt(is_duel & F.col("duel_outcome").isin("Lost Out", "Lost In Play")).alias("duel_lost"),
            cnt(is_duel & (F.col("duel_type") == "Tackle")).alias("tackles"),
            cnt(F.col("event_type") == "Interception").alias("interceptions"),
            cnt(F.col("event_type") == "Clearance").alias("clearances"),
            cnt(F.col("event_type") == "Ball Recovery").alias("ball_recoveries"),
            cnt(F.col("event_type") == "Dispossessed").alias("dispossessed"),
            cnt((F.col("event_type") == "Goal Keeper")
                & F.col("goalkeeper_type").startswith("Shot Saved")).alias("saves"),
            cnt(F.col("event_type") == "Foul Committed").alias("fouls"),
            cnt(F.col("event_type") == "Foul Won").alias("was_fouled"),
            cnt(F.col("event_type") == "Offside").alias("offsides"),
        )
    )

    # Player universe = lineup squad + any event-only player.
    event_only_players = (
        play_events
        .groupBy("match_id", "player_id")
        .agg(
            F.first("team_id", ignorenulls=True).alias("team_id"),
            F.first("team_name", ignorenulls=True).alias("team_name"),
            F.first("player_name", ignorenulls=True).alias("player_name"),
            F.first("position", ignorenulls=True).alias("position"),
        )
        .join(lineups.select("match_id", "player_id"), ["match_id", "player_id"], "left_anti")
        .withColumn("player_nickname", F.lit(None).cast(STR))
        .withColumn("jersey_number", F.lit(None).cast(INT))
    )

    cols = ["match_id", "team_id", "team_name", "player_id", "player_name",
            "player_nickname", "jersey_number", "position"]
    player_base = lineups.select(*cols).unionByName(event_only_players.select(*cols))

    player_minutes = (
        player_base
        .join(
            starting_players.select(
                "match_id", "player_id",
                F.lit(True).alias("started"), "starting_position",
            ),
            ["match_id", "player_id"], "left",
        )
        .join(substitutions_out, ["match_id", "player_id"], "left")
        .join(substitutions_in, ["match_id", "player_id"], "left")
        .join(match_info.select("match_id", "match_duration"), "match_id", "left")
        .withColumn("started", F.coalesce("started", F.lit(False)))
        .withColumn(
            "minutes_played",
            F.when(F.col("started") & F.col("sub_off_time").isNotNull(),
                   F.greatest(F.lit(0.0), F.col("sub_off_time")))
             .when(F.col("started"), F.col("match_duration"))
             .when(F.col("sub_on_time").isNotNull(),
                   F.greatest(F.lit(0.0), F.col("match_duration") - F.col("sub_on_time")))
             .otherwise(F.lit(0.0)),
        )
        .withColumn("player_subbed_on", F.col("sub_on_time").isNotNull())
        .withColumn("player_subbed_off", F.col("sub_off_time").isNotNull())
    )

    # ------------------------------------------------------------------
    # FINAL PLAYER-MATCH SILVER
    # ------------------------------------------------------------------
    count_cols = [
        "total_passes", "accurate_passes", "key_passes", "assists",
        "total_crosses", "accurate_crosses", "total_long_balls", "accurate_long_balls",
        "goals", "total_shots", "shots_on_target", "shots_off_target", "blocked_shots",
        "duel_won", "duel_lost", "tackles", "interceptions", "clearances",
        "ball_recoveries", "dispossessed", "saves", "fouls", "was_fouled", "offsides",
    ]

    joined = (
        player_minutes
        .join(stats, ["match_id", "player_id"], "left")
        .fillna(0, subset=count_cols)
        .fillna(0.0, subset=["expected_goals"])
        .withColumn(
            "position_final",
            F.coalesce(F.col("starting_position"), F.col("position"), F.col("event_position")),
        )
    )

    player_stats = (
        joined
        .select(
            F.lit(SOURCE).alias("source"),
            F.lit(SEASON).alias("season"),
            "match_id",
            "player_id",
            F.coalesce("player_name", "event_player_name").alias("player_name"),
            "team_id",
            F.col("team_id").alias("club_team_id"),
            F.upper(F.trim("position_final")).alias("position"),
            null_double().alias("rating"),
            "started",
            (F.col("minutes_played") > 0).alias("played"),
            F.round("minutes_played", 2).alias("minutes_played"),

            F.col("total_passes").cast(INT).alias("total_passes"),
            F.col("accurate_passes").cast(INT).alias("accurate_passes"),
            F.col("key_passes").cast(INT).alias("key_passes"),
            F.col("assists").cast(INT).alias("assists"),
            F.col("total_crosses").cast(INT).alias("total_crosses"),
            F.col("accurate_crosses").cast(INT).alias("accurate_crosses"),
            F.col("total_long_balls").cast(INT).alias("total_long_balls"),
            F.col("accurate_long_balls").cast(INT).alias("accurate_long_balls"),

            F.col("goals").cast(INT).alias("goals"),
            F.col("total_shots").cast(INT).alias("total_shots"),
            F.col("shots_on_target").cast(INT).alias("shots_on_target"),
            F.col("shots_off_target").cast(INT).alias("shots_off_target"),
            F.col("blocked_shots").cast(INT).alias("blocked_shots"),
            null_int().alias("big_chances_created"),
            F.col("expected_goals").cast(DBL).alias("expected_goals"),
            null_double().alias("expected_assists"),
            null_double().alias("np_expected_goals"),

            F.col("duel_won").cast(INT).alias("duel_won"),
            F.col("duel_lost").cast(INT).alias("duel_lost"),
            null_int().alias("aerial_won"),
            null_int().alias("challenge_lost"),
            null_int().alias("won_contest"),
            F.col("dispossessed").cast(INT).alias("dispossessed"),
            F.col("tackles").cast(INT).alias("tackles"),
            F.col("interceptions").cast(INT).alias("interceptions"),
            F.col("clearances").cast(INT).alias("clearances"),
            F.col("ball_recoveries").cast(INT).alias("ball_recoveries"),
            F.col("saves").cast(INT).alias("saves"),
            null_int().alias("touches"),
            F.col("fouls").cast(INT).alias("fouls"),
            F.col("was_fouled").cast(INT).alias("was_fouled"),
            F.col("offsides").cast(INT).alias("offsides"),
            null_int().alias("yellow_cards"),
            null_int().alias("red_cards"),
            null_int().alias("possession_lost"),
            "player_subbed_on",
            "player_subbed_off",
        )
        .dropDuplicates(["match_id", "player_id"])
        .cache()
    )

    # ------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------
    banner("VALIDATION", "-")

    event_count = events.count()
    match_count = matches.count()
    lineup_count = lineups.count()
    stat_rows = player_stats.count()

    print(f"Events:                  {event_count:,}")
    print(f"Matches:                 {match_count}")
    print(f"Lineup rows:             {lineup_count:,}")
    print(f"Player-match rows:       {stat_rows:,}")
    print(f"Distinct event matches:  {events.select('match_id').distinct().count()}")
    print(f"Distinct stat matches:   {player_stats.select('match_id').distinct().count()}")
    print(f"Distinct players:        {player_stats.select('player_id').distinct().count():,}")

    bad_ids = player_stats.filter(
        F.col("match_id").isNull() | F.col("player_id").isNull()
    ).count()
    print(f"Rows with NULL IDs:      {bad_ids}")
    if bad_ids:
        raise RuntimeError("Required player-match identifiers contain NULLs")

    dupes = (
        player_stats.groupBy("match_id", "player_id").count()
        .filter(F.col("count") > 1).count()
    )
    print(f"Duplicate player-match:  {dupes}")
    if dupes:
        raise RuntimeError("Player-match grain violated")

    print(f"Rows with minutes > 0:   {player_stats.filter(F.col('minutes_played') > 0).count()}")
    print(f"Matches w/ extra time:   {matches.filter('went_to_extra_time').count()}")
    print(f"Matches w/ penalties:    {matches.filter('went_to_penalties').count()}")

    print("\nPosition distribution:")
    player_stats.groupBy("position").count().orderBy(F.desc("count")).show(truncate=False)

    print("\nPlayer-match Silver schema:")
    player_stats.printSchema()

    # ------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------
    banner("WRITING SILVER PARQUET", "-")

    write_parquet(matches, MATCHES_OUT, partition_col="season")
    write_parquet(lineups, LINEUPS_OUT, partition_col="season")
    write_parquet(events, EVENTS_OUT, partition_col="season")
    write_parquet(player_stats, PLAYER_STATS_OUT, partition_col="season")

    teams = (
        matches.select(
            "source", "season",
            F.col("home_team_id").alias("team_id"),
            F.col("home_team_name").alias("team_name"),
        )
        .unionByName(
            matches.select(
                "source", "season",
                F.col("away_team_id").alias("team_id"),
                F.col("away_team_name").alias("team_name"),
            )
        )
        .dropDuplicates(["team_id"])
    )
    write_parquet(teams, TEAMS_OUT)

    players = (
        lineups.select("source", "season", "player_id", "player_name", "player_nickname")
        .dropDuplicates(["player_id"])
    )
    write_parquet(players, PLAYERS_OUT)

    banner("TRANSFORMATION COMPLETE")

    print("\nUpload commands:")
    for name, partitioned in [
        ("matches", True), ("teams", False), ("players", False),
        ("events", True), ("lineups", True), ("player_match_stats", True),
    ]:
        suffix = "\\season=2018" if partitioned else ""
        s3_suffix = "/season=2018" if partitioned else ""
        print(
            f"aws s3 sync data\\silver\\football\\{name}{suffix} "
            f"s3://sports-intel-platform-data/silver/football/{name}{s3_suffix}/"
        )

    spark.stop()


if __name__ == "__main__":
    main()