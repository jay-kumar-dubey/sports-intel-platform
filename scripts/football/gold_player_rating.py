"""
Football Gold: Context-Aware Player Performance Rating Engine (PySpark)

Reads:
    silver/football/player_match_stats
    silver/football/matches
    silver/football/elo

Writes:
    gold/football/player_match_rating
    gold/football/player_tournament_rating
    gold/football/player_rankings

Pipeline:
    per-90 + rate metrics
        -> percentile vs (source, season, position group)
        -> Attack / Playmaking / Defense / Efficiency (or GK model)
        -> role-weighted base score
        -> context adjustment
        -> 0-100 match score
        -> minutes-weighted tournament rating
        -> rankings

Important:
    Silver datasets contain multiple seasons and sources.
    The physical Parquet schemas are not always identical between seasons.

    This script therefore:
      1. Reads each season independently.
      2. Applies a canonical schema.
      3. Casts identifiers to STRING.
      4. Casts numeric metrics to DOUBLE.
      5. Casts boolean fields safely.
      6. Only then performs unionByName().

Run:
    python scripts\football\gold_player_rating.py
"""

import os
from functools import reduce
from itertools import chain
from pathlib import Path

from common import banner, get_spark, read_parquet_dataset, write_parquet

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)


# ============================================================
# CONFIG
# ============================================================

SILVER_STATS = "data/silver/football/player_match_stats"
SILVER_MATCHES = "data/silver/football/matches"
SILVER_ELO = "data/silver/football/elo"

GOLD_BASE = "data/gold/football"

MIN_MINUTES_REFERENCE = 15
MIN_POPULATION = 5
MIN_MINUTES_FOR_RANKING = 180

CONSISTENCY_STD_CAP = 25.0

OPPONENT_ELO_SCALE = 400.0
OPPONENT_MAX_ADJ = 0.04

MIN_ATTEMPTS = {
    "passes": 10,
    "shots": 1,
    "duels": 3,
    "crosses": 2,
    "long_balls": 3,
}


ROLE_WEIGHTS = {
    "FORWARD": {
        "attack": 0.50,
        "playmaking": 0.20,
        "defense": 0.10,
        "efficiency": 0.20,
    },
    "MIDFIELDER": {
        "attack": 0.25,
        "playmaking": 0.35,
        "defense": 0.20,
        "efficiency": 0.20,
    },
    "DEFENDER": {
        "attack": 0.10,
        "playmaking": 0.20,
        "defense": 0.50,
        "efficiency": 0.20,
    },
}


ATTACK_W = [
    ("goals_per90", 0.30),
    ("xg_per90", 0.20),
    ("assists_per90", 0.15),
    ("xa_per90", 0.15),
    ("xg_per_shot", 0.10),
    ("chance_creation_per90", 0.10),
]


PLAYMAKING_W = [
    ("key_passes_per90", 0.25),
    ("assists_per90", 0.20),
    ("xa_per90", 0.15),
    ("pass_accuracy", 0.15),
    ("accurate_long_balls_per90", 0.10),
    ("ball_involvement", 0.15),
]


DEFENSE_W = [
    ("tackles_per90", 0.20),
    ("interceptions_per90", 0.20),
    ("clearances_per90", 0.15),
    ("recoveries_per90", 0.15),
    ("duel_success", 0.10),
    ("aerial_won_per90", 0.10),
    ("dispossession_prevention", 0.10),
]


EFFICIENCY_W = [
    ("pass_accuracy", 0.30),
    ("duel_success", 0.25),
    ("shot_accuracy", 0.15),
    ("cross_accuracy", 0.10),
    ("long_ball_accuracy", 0.10),
    ("finishing_overperformance_per90", 0.10),
]


# Goalkeeper model.
# goals_prevented is unavailable because the source does not provide
# shots-on-target-faced / xGOT.
GK_W = [
    ("saves_per90", 0.35),
    ("distribution", 0.15),
    ("def_actions_per90", 0.15),
    ("clean_sheet", 0.15),
]

GK_UNAVAILABLE = ["goals_prevented"]


STAGE_FACTORS = {
    "group_stage": 1.00,
    "group": 1.00,
    "round_of_32": 1.01,
    "round_of_16": 1.03,
    "quarter_final": 1.06,
    "quarter_finals": 1.06,
    "semi_final": 1.08,
    "semi_finals": 1.08,
    "third_place": 1.00,
    "3rd_place_final": 1.00,
    "final": 1.10,
}


RESULT_ADJUSTMENT = {
    "win": 0.03,
    "draw": 0.0,
    "loss": -0.03,
}

CONTEXT_MIN = 0.85
CONTEXT_MAX = 1.15


TOURNAMENT_W = {
    "minutes_weighted": 0.70,
    "consistency": 0.15,
    "peak": 0.10,
    "availability": 0.05,
}


# ============================================================
# ELO CODE -> TEAM NAME
# ============================================================

ELO_CODE_TO_NAME = {
    "AR": "Argentina",
    "AU": "Australia",
    "BE": "Belgium",
    "BR": "Brazil",
    "CA": "Canada",
    "CH": "Switzerland",
    "CM": "Cameroon",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "DE": "Germany",
    "DK": "Denmark",
    "EC": "Ecuador",
    "EG": "Egypt",
    "EN": "England",
    "ES": "Spain",
    "FR": "France",
    "GH": "Ghana",
    "HR": "Croatia",
    "IR": "Iran",
    "IS": "Iceland",
    "JP": "Japan",
    "KR": "South Korea",
    "MA": "Morocco",
    "MX": "Mexico",
    "NG": "Nigeria",
    "NL": "Netherlands",
    "PA": "Panama",
    "PE": "Peru",
    "PL": "Poland",
    "PT": "Portugal",
    "QA": "Qatar",
    "RS": "Serbia",
    "RU": "Russia",
    "SA": "Saudi Arabia",
    "SE": "Sweden",
    "SN": "Senegal",
    "TN": "Tunisia",
    "US": "United States",
    "UY": "Uruguay",
    "WA": "Wales",
    "AT": "Austria",
    "CZ": "Czech Republic",
    "UA": "Ukraine",
    "HU": "Hungary",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "TR": "Turkey",
    "SQ": "Scotland",
    "AL": "Albania",
    "GE": "Georgia",
    "RO": "Romania",
    "CL": "Chile",
    "PY": "Paraguay",
    "VE": "Venezuela",
    "BO": "Bolivia",
    "DZ": "Algeria",
    "CI": "Ivory Coast",
    "ML": "Mali",
    "ZA": "South Africa",
    "NZ": "New Zealand",
    "NO": "Norway",
    "FI": "Finland",
    "GR": "Greece",
    "IE": "Ireland",
}


NAME_ALIASES = {
    "korearepublic": "southkorea",
    "republicofkorea": "southkorea",
    "iriran": "iran",
    "unitedstatesofamerica": "unitedstates",
    "usa": "unitedstates",
    "czechia": "czechrepublic",
    "cotedivoire": "ivorycoast",
    "turkiye": "turkey",
    "holland": "netherlands",
}


_ACCENTS_FROM = "áàâäãåéèêëíìîïóòôöõúùûüçñšžćčđ"
_ACCENTS_TO = "aaaaaa" "eeee" "iiii" "ooooo" "uuuu" "cn" "szccd"


PCT_METRICS = [
    "goals_per90",
    "xg_per90",
    "assists_per90",
    "xa_per90",
    "xg_per_shot",
    "chance_creation_per90",
    "key_passes_per90",
    "accurate_long_balls_per90",
    "touches_per90",
    "passes_per90",
    "pass_accuracy",
    "tackles_per90",
    "interceptions_per90",
    "clearances_per90",
    "recoveries_per90",
    "duel_success",
    "aerial_won_per90",
    "dispossessed_per90",
    "shot_accuracy",
    "cross_accuracy",
    "long_ball_accuracy",
    "finishing_overperformance_per90",
    "saves_per90",
    "def_actions_per90",
]


# ============================================================
# CANONICAL SILVER SCHEMAS
# ============================================================

# Every player-stat field used by the Gold engine is assigned one
# canonical type. This prevents BOOLEAN/STRING/INT/DOUBLE conflicts
# when multiple seasons are unioned.

PLAYER_STRING_COLUMNS = {
    "source",
    "match_id",
    "player_id",
    "player_name",
    "team_id",
    "club_team_id",
    "position",
}

PLAYER_BOOLEAN_COLUMNS = {
    "started",
    "played",
    "player_subbed_on",
    "player_subbed_off",
}


PLAYER_NUMERIC_COLUMNS = {
    "rating",
    "minutes_played",
    "total_passes",
    "accurate_passes",
    "key_passes",
    "assists",
    "total_crosses",
    "accurate_crosses",
    "total_long_balls",
    "accurate_long_balls",
    "goals",
    "total_shots",
    "shots_on_target",
    "shots_off_target",
    "blocked_shots",
    "big_chances_created",
    "expected_goals",
    "expected_assists",
    "np_expected_goals",
    "duel_won",
    "duel_lost",
    "aerial_won",
    "challenge_lost",
    "won_contest",
    "dispossessed",
    "tackles",
    "interceptions",
    "clearances",
    "ball_recoveries",
    "saves",
    "touches",
    "fouls",
    "was_fouled",
    "offsides",
    "yellow_cards",
    "red_cards",
    "possession_lost",
}


MATCH_STRING_COLUMNS = {
    "source",
    "match_id",
    "stage_name",
    "status",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "winner",
}


MATCH_NUMERIC_COLUMNS = {
    "match_date",
    "home_score",
    "away_score",
}


# ============================================================
# HELPERS
# ============================================================

def add(df, cols):
    return df.select(
        "*",
        *[c.alias(n) for n, c in cols.items()],
    )


def per90(col):
    return F.when(
        F.col("minutes_played") > 0,
        col / F.col("minutes_played") * 90.0,
    )


def rate(num, den, min_den):
    return F.when(
        F.col(den) >= min_den,
        F.col(num) / F.col(den),
    )


def P(name):
    return F.col(f"pct_{name}")


def wmean(pairs):
    """
    Weighted mean over non-NULL components only.
    Weights are automatically re-normalised.
    """

    num = sum(
        F.coalesce(c, F.lit(0.0)) * w
        for c, w in pairs
    )

    den = sum(
        F.when(
            c.isNotNull(),
            F.lit(float(w)),
        ).otherwise(
            F.lit(0.0)
        )
        for c, w in pairs
    )

    return F.when(
        den > 0,
        num / den,
    )


def pct_pairs(weights):
    return [(P(n), w) for n, w in weights]


def clamp(col, lo, hi):
    return F.least(
        F.greatest(
            col,
            F.lit(float(lo)),
        ),
        F.lit(float(hi)),
    )


def canon(col):
    """
    Normalise a team name so StatsBomb / ELO / other sources compare equal.
    """

    norm = F.regexp_replace(
        F.lower(
            F.translate(
                col,
                _ACCENTS_FROM,
                _ACCENTS_TO,
            )
        ),
        r"[^a-z]",
        "",
    )

    alias_map = F.create_map(
        *[
            F.lit(x)
            for x in chain.from_iterable(NAME_ALIASES.items())
        ]
    )

    return F.coalesce(
        alias_map[norm],
        norm,
    )


# ============================================================
# SAFE BOOLEAN CAST
# ============================================================

def safe_boolean(col):
    """
    Convert BOOLEAN / STRING / numeric representations into BOOLEAN.

    Handles:
        true / false
        1 / 0
        yes / no
        y / n
        t / f

    Unknown values become NULL.
    """

    text = F.lower(F.trim(col.cast("string")))

    return (
        F.when(text.isin("true", "1", "yes", "y", "t"), F.lit(True))
        .when(text.isin("false", "0", "no", "n", "f"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


# ============================================================
# CANONICALISE PLAYER-MATCH-STATS
# ============================================================

def canonicalize_player_stats(df, season):
    """
    Force one stable schema for player_match_stats.

    This is the key fix for the current error:
        BOOLEAN is not compatible with STRING

    All seasons are converted to the same physical Spark types BEFORE
    unionByName().
    """

    # Add missing columns first.
    for c in PLAYER_STRING_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(
                c,
                F.lit(None).cast("string"),
            )

    for c in PLAYER_BOOLEAN_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(
                c,
                F.lit(None).cast("boolean"),
            )

    for c in PLAYER_NUMERIC_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(
                c,
                F.lit(None).cast("double"),
            )

    # IDs / descriptive fields.
    for c in PLAYER_STRING_COLUMNS:
        df = df.withColumn(
            c,
            F.col(c).cast("string"),
        )

    # Boolean fields.
    for c in PLAYER_BOOLEAN_COLUMNS:
        df = df.withColumn(
            c,
            safe_boolean(F.col(c)),
        )

    # Numeric fields.
    for c in PLAYER_NUMERIC_COLUMNS:
        df = df.withColumn(
            c,
            F.col(c).cast("double"),
        )

    # Season is controlled by the directory name.
    df = df.withColumn(
        "season",
        F.lit(int(season)).cast("int"),
    )

    ordered = [
        "source",
        "season",
        "match_id",
        "player_id",
        "player_name",
        "team_id",
        "club_team_id",
        "position",
        "rating",
        "started",
        "played",
        "minutes_played",
        "total_passes",
        "accurate_passes",
        "key_passes",
        "assists",
        "total_crosses",
        "accurate_crosses",
        "total_long_balls",
        "accurate_long_balls",
        "goals",
        "total_shots",
        "shots_on_target",
        "shots_off_target",
        "blocked_shots",
        "big_chances_created",
        "expected_goals",
        "expected_assists",
        "np_expected_goals",
        "duel_won",
        "duel_lost",
        "aerial_won",
        "challenge_lost",
        "won_contest",
        "dispossessed",
        "tackles",
        "interceptions",
        "clearances",
        "ball_recoveries",
        "saves",
        "touches",
        "fouls",
        "was_fouled",
        "offsides",
        "yellow_cards",
        "red_cards",
        "possession_lost",
        "player_subbed_on",
        "player_subbed_off",
    ]

    return df.select(*ordered)


# ============================================================
# CANONICALISE MATCHES
# ============================================================

def canonicalize_matches(df, season):
    """
    Force one stable schema for football matches.

    Most importantly:
        match_id is ALWAYS STRING.

    This allows:
        StatsBomb numeric-looking IDs
        TheStatsAPI IDs such as mt_894046969
    to coexist safely.
    """

    for c in MATCH_STRING_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(
                c,
                F.lit(None).cast("string"),
            )

    for c in MATCH_NUMERIC_COLUMNS:
        if c not in df.columns:
            df = df.withColumn(
                c,
                F.lit(None).cast("double"),
            )

    for c in MATCH_STRING_COLUMNS:
        df = df.withColumn(
            c,
            F.col(c).cast("string"),
        )

    for c in MATCH_NUMERIC_COLUMNS:
        df = df.withColumn(
            c,
            F.col(c).cast("double"),
        )

    df = df.withColumn(
        "season",
        F.lit(int(season)).cast("int"),
    )

    ordered = [
        "source",
        "season",
        "match_id",
        "match_date",
        "stage_name",
        "status",
        "home_team_id",
        "home_team_name",
        "away_team_id",
        "away_team_name",
        "home_score",
        "away_score",
        "winner",
    ]

    return df.select(*ordered)


# ============================================================
# CANONICALISE ELO
# ============================================================

def canonicalize_elo(df, season):
    """
    Force ELO to one stable schema.
    """

    required = {
        "team_code",
        "team_name",
        "elo_rating",
        "elo_rank",
        "snapshot_year",
    }

    for c in required:
        if c not in df.columns:
            if c in {"elo_rating"}:
                dtype = "double"
            elif c in {"elo_rank", "snapshot_year"}:
                dtype = "int"
            else:
                dtype = "string"

            df = df.withColumn(
                c,
                F.lit(None).cast(dtype),
            )

    df = (
        df.withColumn("team_code", F.col("team_code").cast("string"))
        .withColumn("team_name", F.col("team_name").cast("string"))
        .withColumn("elo_rating", F.col("elo_rating").cast("double"))
        .withColumn("elo_rank", F.col("elo_rank").cast("int"))
        .withColumn("snapshot_year", F.col("snapshot_year").cast("int"))
        .withColumn("season", F.lit(int(season)).cast("int"))
    )

    return df.select(
        "season",
        "team_code",
        "team_name",
        "elo_rating",
        "elo_rank",
        "snapshot_year",
    )


# ============================================================
# ROBUST SILVER READER
# ============================================================

def read_silver(spark, path):
    """
    Read a Silver dataset season by season.

    WHY THIS EXISTS:
    ----------------
    The project contains multiple football sources and seasons.

    Example:
        StatsBomb 2018/2022 may store match_id as INT.
        TheStatsAPI 2026 stores match_id as STRING.

    Spark cannot safely read a mixed physical Parquet column as one schema.

    Therefore:

        season 2018 -> read -> canonicalise
        season 2022 -> read -> canonicalise
        season 2026 -> read -> canonicalise
                         |
                         v
                    unionByName

    The same approach also handles BOOLEAN/STRING differences in
    player_match_stats.
    """

    root = Path(__file__).resolve().parents[2] / path

    if not root.exists():
        raise FileNotFoundError(
            f"Silver dataset does not exist: {root}"
        )

    parts = sorted(
        d
        for d in os.listdir(root)
        if d.startswith("season=")
        and (root / d).is_dir()
    )

    # Non-partitioned dataset.
    if not parts:
        return read_parquet_dataset(
            spark,
            path,
        )

    frames = []

    for d in parts:
        season = d.split("=", 1)[1]

        files = sorted(
            str(p)
            for p in (root / d).rglob("*.parquet")
        )

        if not files:
            continue

        print(
            f"Reading Silver partition: {path} / season={season} "
            f"({len(files)} parquet file(s))"
        )

        # Important:
        # Explicitly hand Spark the files instead of the directory.
        # This avoids Windows Hadoop directory-listing problems.
        raw = spark.read.parquet(*files)

        if path == SILVER_STATS:
            df = canonicalize_player_stats(
                raw,
                season,
            )

        elif path == SILVER_MATCHES:
            df = canonicalize_matches(
                raw,
                season,
            )

        elif path == SILVER_ELO:
            df = canonicalize_elo(
                raw,
                season,
            )

        else:
            df = raw.withColumn(
                "season",
                F.lit(int(season)).cast("int"),
            )

        frames.append(df)

    if not frames:
        raise RuntimeError(
            f"No Parquet files found under {root}"
        )

    result = reduce(
        lambda a, b: a.unionByName(
            b,
            allowMissingColumns=True,
        ),
        frames,
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    banner(
        "FOOTBALL GOLD - PLAYER PERFORMANCE RATING ENGINE"
    )

    spark = get_spark(
        "FootballGoldPlayerRating"
    )

    # ========================================================
    # READ SILVER
    # ========================================================

    stats = read_silver(
        spark,
        SILVER_STATS,
    )

    matches = (
        read_silver(
            spark,
            SILVER_MATCHES,
        )
        .select(
            "source",
            "season",
            "match_id",
            "match_date",
            "stage_name",
            "status",
            "home_team_id",
            "home_team_name",
            "away_team_id",
            "away_team_name",
            "home_score",
            "away_score",
            "winner",
        )
    )

    print(
        f"\nSilver player rows: {stats.count():,}"
    )

    print(
        f"Silver matches:     {matches.count():,}"
    )

    # ========================================================
    # ELO
    # ========================================================

    code_map = F.create_map(
        *[
            F.lit(x)
            for x in chain.from_iterable(
                ELO_CODE_TO_NAME.items()
            )
        ]
    )

    elo_raw = read_silver(
        spark,
        SILVER_ELO,
    )

    elo_named = (
        elo_raw
        .withColumn(
            "team_name",
            F.coalesce(
                F.col("team_name"),
                code_map[F.col("team_code")],
            ),
        )
        .filter(
            F.col("team_name").isNotNull()
        )
        .select(
            F.col("season")
            .cast("int")
            .alias("season"),

            canon(
                F.col("team_name")
            ).alias("opp_key"),

            F.col("elo_rating")
            .cast("double")
            .alias("opponent_strength"),
        )
        .dropDuplicates(
            ["season", "opp_key"]
        )
    )

    print(
        f"Silver ELO teams:   "
        f"{elo_named.count():,} "
        f"(mapped from {elo_raw.count():,})"
    )

    # ========================================================
    # 1. JOIN MATCH CONTEXT
    # ========================================================

    df = stats.join(
        F.broadcast(matches),
        ["source", "season", "match_id"],
        "left",
    )

    df = df.filter(
        F.col("minutes_played") > 0
    )

    pos = F.upper(
        F.trim(
            F.col("position")
        )
    )

    df = add(
        df,
        {
            "position_group": (
                F.when(
                    pos.isin("G", "GK")
                    | pos.contains("GOALKEEPER"),
                    "GK",
                )
                .when(
                    (pos == "D")
                    | pos.contains("BACK"),
                    "DEFENDER",
                )
                .when(
                    (pos == "M")
                    | pos.contains("MIDFIELD"),
                    "MIDFIELDER",
                )
                .when(
                    pos.isin("F", "A", "FW")
                    | pos.contains("WING")
                    | pos.contains("FORWARD")
                    | pos.contains("STRIKER"),
                    "FORWARD",
                )
            ),

            "team_side": (
                F.when(
                    F.col("team_id")
                    == F.col("home_team_id"),
                    "home",
                )
                .when(
                    F.col("team_id")
                    == F.col("away_team_id"),
                    "away",
                )
            ),
        },
    )

    winner_l = F.lower(
        F.col("winner")
    )

    df = add(
        df,
        {
            "opponent_team_id": (
                F.when(
                    F.col("team_side") == "home",
                    F.col("away_team_id"),
                )
                .when(
                    F.col("team_side") == "away",
                    F.col("home_team_id"),
                )
            ),

            "opponent_name": (
                F.when(
                    F.col("team_side") == "home",
                    F.col("away_team_name"),
                )
                .when(
                    F.col("team_side") == "away",
                    F.col("home_team_name"),
                )
            ),

            "goals_conceded": (
                F.when(
                    F.col("team_side") == "home",
                    F.col("away_score"),
                )
                .when(
                    F.col("team_side") == "away",
                    F.col("home_score"),
                )
            ),

            "result": (
                F.when(
                    winner_l.isin("home", "away"),
                    F.when(
                        winner_l
                        == F.col("team_side"),
                        "win",
                    ).otherwise("loss"),
                )
                .when(
                    winner_l == "draw",
                    "draw",
                )
            ),
        },
    )

    # ========================================================
    # 2. OPPONENT ELO
    # ========================================================

    df = add(
        df,
        {
            "opp_key": canon(
                F.col("opponent_name")
            ),
        },
    )

    elo_mean = (
        df.select(
            "season",
            "opp_key",
        )
        .distinct()
        .join(
            elo_named,
            ["season", "opp_key"],
        )
        .groupBy("season")
        .agg(
            F.avg(
                "opponent_strength"
            ).alias("elo_mean")
        )
    )

    df = (
        df.join(
            F.broadcast(elo_named),
            ["season", "opp_key"],
            "left",
        )
        .join(
            F.broadcast(elo_mean),
            "season",
            "left",
        )
    )

    # ========================================================
    # 3. PER-90 AND EFFICIENCY METRICS
    # ========================================================

    df = add(
        df,
        {
            "duel_total":
                F.coalesce(
                    F.col("duel_won"),
                    F.lit(0.0),
                )
                + F.coalesce(
                    F.col("duel_lost"),
                    F.lit(0.0),
                ),

            "goals_per90":
                per90(F.col("goals")),

            "assists_per90":
                per90(F.col("assists")),

            "xg_per90":
                per90(F.col("expected_goals")),

            "xa_per90":
                per90(F.col("expected_assists")),

            "shots_per90":
                per90(F.col("total_shots")),

            "key_passes_per90":
                per90(F.col("key_passes")),

            "chance_creation_per90":
                per90(
                    F.coalesce(
                        F.col("big_chances_created"),
                        F.lit(0.0),
                    )
                    + F.coalesce(
                        F.col("key_passes"),
                        F.lit(0.0),
                    )
                ),

            "accurate_long_balls_per90":
                per90(
                    F.col("accurate_long_balls")
                ),

            "touches_per90":
                per90(F.col("touches")),

            "passes_per90":
                per90(F.col("total_passes")),

            "tackles_per90":
                per90(F.col("tackles")),

            "interceptions_per90":
                per90(F.col("interceptions")),

            "clearances_per90":
                per90(F.col("clearances")),

            "recoveries_per90":
                per90(F.col("ball_recoveries")),

            "aerial_won_per90":
                per90(F.col("aerial_won")),

            "dispossessed_per90":
                per90(F.col("dispossessed")),

            "saves_per90":
                per90(F.col("saves")),

            "def_actions_per90":
                per90(
                    F.coalesce(
                        F.col("tackles"),
                        F.lit(0.0),
                    )
                    + F.coalesce(
                        F.col("interceptions"),
                        F.lit(0.0),
                    )
                    + F.coalesce(
                        F.col("clearances"),
                        F.lit(0.0),
                    )
                ),

            "finishing_overperformance":
                F.col("goals")
                - F.col("expected_goals"),

            "assist_overperformance":
                F.col("assists")
                - F.col("expected_assists"),

            "finishing_overperformance_per90":
                per90(
                    F.col("goals")
                    - F.col("expected_goals")
                ),

            "assist_overperformance_per90":
                per90(
                    F.col("assists")
                    - F.col("expected_assists")
                ),

            "xg_per_shot":
                F.when(
                    F.col("total_shots")
                    >= MIN_ATTEMPTS["shots"],
                    F.col("expected_goals")
                    / F.col("total_shots"),
                ),

            "pass_accuracy":
                rate(
                    "accurate_passes",
                    "total_passes",
                    MIN_ATTEMPTS["passes"],
                ),

            "shot_accuracy":
                rate(
                    "shots_on_target",
                    "total_shots",
                    MIN_ATTEMPTS["shots"],
                ),

            "duel_success":
                F.when(
                    F.col("duel_total")
                    >= MIN_ATTEMPTS["duels"],
                    F.col("duel_won")
                    / F.col("duel_total"),
                ),

            "cross_accuracy":
                rate(
                    "accurate_crosses",
                    "total_crosses",
                    MIN_ATTEMPTS["crosses"],
                ),

            "long_ball_accuracy":
                rate(
                    "accurate_long_balls",
                    "total_long_balls",
                    MIN_ATTEMPTS["long_balls"],
                ),
        },
    )

    # ========================================================
    # 4. PERCENTILES
    # ========================================================

    ref = (
        F.col("minutes_played")
        >= MIN_MINUTES_REFERENCE
    ) & F.col(
        "position_group"
    ).isNotNull()

    keys = [
        "source",
        "season",
        "position_group",
    ]

    pop = df.groupBy(
        *keys
    ).agg(
        *[
            F.collect_list(
                F.when(
                    ref
                    & F.col(m).isNotNull(),
                    F.col(m),
                )
            ).alias(
                f"_pop_{m}"
            )
            for m in PCT_METRICS
        ]
    )

    df = df.join(
        F.broadcast(pop),
        keys,
        "left",
    )

    df = add(
        df,
        {
            f"pct_{m}": F.expr(
                f"""
                CASE
                    WHEN {m} IS NOT NULL
                    AND size(_pop_{m}) >= {MIN_POPULATION}
                    THEN
                        (
                            size(
                                filter(
                                    _pop_{m},
                                    v -> v < {m}
                                )
                            )
                            +
                            0.5 * size(
                                filter(
                                    _pop_{m},
                                    v -> v = {m}
                                )
                            )
                        )
                        / size(_pop_{m})
                        * 100.0
                END
                """
            )
            for m in PCT_METRICS
        },
    ).drop(
        *[
            f"_pop_{m}"
            for m in PCT_METRICS
        ]
    )

    # ========================================================
    # 5. DERIVED PERCENTILES
    # ========================================================

    df = add(
        df,
        {
            "pct_dispossession_prevention":
                F.when(
                    P(
                        "dispossessed_per90"
                    ).isNotNull(),
                    100.0
                    - P(
                        "dispossessed_per90"
                    ),
                ),

            "pct_ball_involvement":
                wmean(
                    [
                        (
                            P("touches_per90"),
                            1.0,
                        ),
                        (
                            P("passes_per90"),
                            1.0,
                        ),
                    ]
                ),

            "pct_distribution":
                wmean(
                    [
                        (
                            P("pass_accuracy"),
                            1.0,
                        ),
                        (
                            P(
                                "long_ball_accuracy"
                            ),
                            1.0,
                        ),
                    ]
                ),

            "pct_clean_sheet":
                F.when(
                    (
                        F.col("minutes_played")
                        >= 60
                    )
                    & F.col(
                        "goals_conceded"
                    ).isNotNull(),
                    F.when(
                        F.col(
                            "goals_conceded"
                        )
                        == 0,
                        100.0,
                    ).otherwise(0.0),
                ),
        },
    )

    # ========================================================
    # 6. COMPONENT SCORES
    # ========================================================

    G = F.col(
        "position_group"
    )

    df = add(
        df,
        {
            "attack_score":
                F.when(
                    G != "GK",
                    wmean(
                        pct_pairs(
                            ATTACK_W
                        )
                    ),
                ),

            "playmaking_score":
                F.when(
                    G != "GK",
                    wmean(
                        pct_pairs(
                            PLAYMAKING_W
                        )
                    ),
                ),

            "defense_score":
                F.when(
                    G != "GK",
                    wmean(
                        pct_pairs(
                            DEFENSE_W
                        )
                    ),
                ),

            "efficiency_score":
                F.when(
                    G != "GK",
                    wmean(
                        pct_pairs(
                            EFFICIENCY_W
                        )
                    ),
                ),

            "gk_score":
                F.when(
                    G == "GK",
                    wmean(
                        pct_pairs(
                            GK_W
                        )
                    ),
                ),
        },
    )

    base = F.when(
        G == "GK",
        F.col("gk_score"),
    )

    for role, w in ROLE_WEIGHTS.items():

        base = base.when(
            G == role,
            wmean(
                [
                    (
                        F.col(
                            "attack_score"
                        ),
                        w["attack"],
                    ),
                    (
                        F.col(
                            "playmaking_score"
                        ),
                        w["playmaking"],
                    ),
                    (
                        F.col(
                            "defense_score"
                        ),
                        w["defense"],
                    ),
                    (
                        F.col(
                            "efficiency_score"
                        ),
                        w["efficiency"],
                    ),
                ]
            ),
        )

    df = add(
        df,
        {
            "base_score": base,
        },
    )

    # ========================================================
    # 7. DATA COVERAGE
    # ========================================================

    outfield_inputs = sorted(
        {
            n
            for W in (
                ATTACK_W,
                PLAYMAKING_W,
                DEFENSE_W,
                EFFICIENCY_W,
            )
            for n, _ in W
        }
    )

    gk_inputs = [
        n
        for n, _ in GK_W
    ]

    def missing(names):
        return F.concat_ws(
            ",",
            *[
                F.when(
                    P(n).isNull(),
                    F.lit(n),
                )
                for n in names
            ],
        )

    def coverage(names):
        return sum(
            F.when(
                P(n).isNotNull(),
                1.0,
            ).otherwise(0.0)
            for n in names
        ) / len(names)

    df = add(
        df,
        {
            "missing_components":
                F.when(
                    G == "GK",
                    F.concat_ws(
                        ",",
                        missing(
                            gk_inputs
                        ),
                        F.lit(
                            ",".join(
                                GK_UNAVAILABLE
                            )
                        ),
                    ),
                ).otherwise(
                    missing(
                        outfield_inputs
                    )
                ),

            "data_coverage":
                F.when(
                    G == "GK",
                    coverage(
                        gk_inputs
                    ),
                ).otherwise(
                    coverage(
                        outfield_inputs
                    )
                ),
        },
    )

    # ========================================================
    # 8. CONTEXT ADJUSTMENT
    # ========================================================

    stage_norm = F.regexp_replace(
        F.lower(
            F.trim(
                F.col("stage_name")
            )
        ),
        r"[\s\-]+",
        "_",
    )

    stage_factor = None

    for stage, factor in STAGE_FACTORS.items():

        if stage_factor is None:
            stage_factor = F.when(
                stage_norm == stage,
                F.lit(factor),
            )
        else:
            stage_factor = stage_factor.when(
                stage_norm == stage,
                F.lit(factor),
            )

    stage_factor = stage_factor.otherwise(
        F.lit(1.0)
    )

    result_adj = (
        F.when(
            F.col("result") == "win",
            RESULT_ADJUSTMENT["win"],
        )
        .when(
            F.col("result") == "loss",
            RESULT_ADJUSTMENT["loss"],
        )
        .otherwise(
            RESULT_ADJUSTMENT["draw"]
        )
    )

    df = add(
        df,
        {
            "opponent_factor":
                F.when(
                    F.col(
                        "opponent_strength"
                    ).isNotNull()
                    & F.col(
                        "elo_mean"
                    ).isNotNull(),
                    clamp(
                        (
                            F.col(
                                "opponent_strength"
                            )
                            - F.col(
                                "elo_mean"
                            )
                        )
                        / OPPONENT_ELO_SCALE,
                        -1.0,
                        1.0,
                    ),
                ),

            "stage_factor":
                stage_factor,

            "result_factor":
                1.0 + result_adj,
        },
    )

    df = add(
        df,
        {
            "opponent_adjustment":
                F.coalesce(
                    F.col(
                        "opponent_factor"
                    )
                    * OPPONENT_MAX_ADJ,
                    F.lit(0.0),
                ),
        },
    )

    df = add(
        df,
        {
            "context_adjustment":
                clamp(
                    1.0
                    + F.col(
                        "opponent_adjustment"
                    )
                    + (
                        F.col(
                            "stage_factor"
                        )
                        - 1.0
                    )
                    + (
                        F.col(
                            "result_factor"
                        )
                        - 1.0
                    ),
                    CONTEXT_MIN,
                    CONTEXT_MAX,
                ),
        },
    )

    df = add(
        df,
        {
            "final_performance_score":
                F.when(
                    F.col(
                        "base_score"
                    ).isNotNull(),
                    F.round(
                        clamp(
                            F.col(
                                "base_score"
                            )
                            * F.col(
                                "context_adjustment"
                            ),
                            0,
                            100,
                        ),
                        2,
                    ),
                ),

            "performance_confidence":
                (
                    F.when(
                        F.col(
                            "minutes_played"
                        ) < 15,
                        "low",
                    )
                    .when(
                        F.col(
                            "minutes_played"
                        ) < 30,
                        "moderate",
                    )
                    .when(
                        F.col(
                            "minutes_played"
                        ) < 60,
                        "good",
                    )
                    .otherwise("high")
                ),
        },
    )

    # ========================================================
    # 9. MATCH RATING OUTPUT
    # ========================================================

    rating = df.select(
        "source",
        "season",
        "match_id",
        "match_date",
        "stage_name",
        "player_id",
        "player_name",
        "team_id",
        "opponent_team_id",
        "opponent_name",
        "position",
        "position_group",
        "started",
        "minutes_played",

        F.round(
            "attack_score",
            2,
        ).alias(
            "attack_score"
        ),

        F.round(
            "playmaking_score",
            2,
        ).alias(
            "playmaking_score"
        ),

        F.round(
            "defense_score",
            2,
        ).alias(
            "defense_score"
        ),

        F.round(
            "efficiency_score",
            2,
        ).alias(
            "efficiency_score"
        ),

        F.round(
            "gk_score",
            2,
        ).alias(
            "gk_score"
        ),

        F.round(
            "base_score",
            2,
        ).alias(
            "base_score"
        ),

        "opponent_strength",

        F.round(
            "opponent_factor",
            4,
        ).alias(
            "opponent_factor"
        ),

        F.round(
            "opponent_adjustment",
            4,
        ).alias(
            "opponent_adjustment"
        ),

        "stage_factor",
        "result_factor",
        "result",

        F.round(
            "context_adjustment",
            4,
        ).alias(
            "context_adjustment"
        ),

        "final_performance_score",
        "performance_confidence",

        F.round(
            "data_coverage",
            3,
        ).alias(
            "data_coverage"
        ),

        "missing_components",

        # Explanatory metrics.
        "goals",
        "assists",
        "expected_goals",
        "expected_assists",

        F.round(
            "goals_per90",
            3,
        ).alias(
            "goals_per90"
        ),

        F.round(
            "assists_per90",
            3,
        ).alias(
            "assists_per90"
        ),

        F.round(
            "xg_per90",
            3,
        ).alias(
            "xg_per90"
        ),

        F.round(
            "xa_per90",
            3,
        ).alias(
            "xa_per90"
        ),

        F.round(
            "key_passes_per90",
            3,
        ).alias(
            "key_passes_per90"
        ),

        F.round(
            "shots_per90",
            3,
        ).alias(
            "shots_per90"
        ),

        F.round(
            "tackles_per90",
            3,
        ).alias(
            "tackles_per90"
        ),

        F.round(
            "interceptions_per90",
            3,
        ).alias(
            "interceptions_per90"
        ),

        F.round(
            "clearances_per90",
            3,
        ).alias(
            "clearances_per90"
        ),

        F.round(
            "recoveries_per90",
            3,
        ).alias(
            "recoveries_per90"
        ),

        F.round(
            "duel_success",
            3,
        ).alias(
            "duel_success"
        ),

        F.round(
            "pass_accuracy",
            3,
        ).alias(
            "pass_accuracy"
        ),

        F.round(
            "finishing_overperformance",
            3,
        ).alias(
            "finishing_overperformance"
        ),

        F.round(
            "assist_overperformance",
            3,
        ).alias(
            "assist_overperformance"
        ),

        F.col(
            "rating"
        ).alias(
            "source_rating"
        ),
    ).cache()

    # ========================================================
    # 10. MATCH VALIDATION
    # ========================================================

    banner(
        "MATCH RATING VALIDATION",
        "-",
    )

    total = rating.count()

    scored = (
        rating
        .filter(
            F.col(
                "final_performance_score"
            ).isNotNull()
        )
        .count()
    )

    print(
        f"Player-match rows (minutes > 0): {total:,}"
    )

    print(
        f"Scored rows:                     {scored:,}"
    )

    dupes = (
        rating
        .groupBy(
            "source",
            "season",
            "match_id",
            "player_id",
        )
        .count()
        .filter(
            "count > 1"
        )
        .count()
    )

    print(
        f"Duplicate (match_id, player_id): {dupes}"
    )

    if dupes:
        raise RuntimeError(
            "Gold grain violated: duplicate player-match rows"
        )

    no_match = (
        rating
        .filter(
            F.col(
                "opponent_team_id"
            ).isNull()
        )
        .count()
    )

    print(
        f"Rows without a matched opponent: {no_match}"
    )

    no_elo = (
        rating
        .filter(
            F.col(
                "opponent_strength"
            ).isNull()
        )
        .count()
    )

    print(
        f"Rows without opponent ELO:       {no_elo}"
    )

    print(
        "\nELO coverage by source / season:"
    )

    (
        rating
        .groupBy(
            "source",
            "season",
        )
        .agg(
            F.count("*").alias(
                "rows"
            ),
            F.count(
                "opponent_strength"
            ).alias(
                "rows_with_elo"
            ),
        )
        .orderBy(
            "source",
            "season",
        )
        .show(
            truncate=False
        )
    )

    print(
        "Opponents without ELO "
        "(add to ELO_CODE_TO_NAME / NAME_ALIASES if needed):"
    )

    (
        rating
        .filter(
            F.col(
                "opponent_strength"
            ).isNull()
        )
        .select(
            "source",
            "season",
            "opponent_name",
        )
        .distinct()
        .orderBy(
            "season",
            "opponent_name",
        )
        .show(
            60,
            truncate=False,
        )
    )

    print(
        "\nUnmapped position codes "
        "(not scored):"
    )

    (
        rating
        .filter(
            F.col(
                "position_group"
            ).isNull()
        )
        .groupBy(
            "position"
        )
        .count()
        .show(
            truncate=False
        )
    )

    print(
        "Results found "
        "(should be win / draw / loss):"
    )

    rating.groupBy(
        "result"
    ).count().show()

    print(
        "Stage names not in STAGE_FACTORS "
        "(treated as 1.00):"
    )

    stage_check = F.regexp_replace(
        F.lower(
            F.trim(
                F.col("stage_name")
            )
        ),
        r"[\s\-]+",
        "_",
    )

    (
        rating
        .filter(
            ~stage_check.isin(
                *STAGE_FACTORS.keys()
            )
        )
        .groupBy(
            "stage_name"
        )
        .count()
        .show(
            truncate=False
        )
    )

    print(
        "Score distribution by position group:"
    )

    (
        rating
        .groupBy(
            "position_group"
        )
        .agg(
            F.count(
                "final_performance_score"
            ).alias("n"),

            F.round(
                F.min(
                    "final_performance_score"
                ),
                1,
            ).alias("min"),

            F.round(
                F.avg(
                    "final_performance_score"
                ),
                1,
            ).alias("avg"),

            F.round(
                F.max(
                    "final_performance_score"
                ),
                1,
            ).alias("max"),

            F.round(
                F.corr(
                    "final_performance_score",
                    "source_rating",
                ),
                3,
            ).alias(
                "corr_vs_source_rating"
            ),
        )
        .orderBy(
            "position_group"
        )
        .show()
    )

    print("Confidence:")

    (
        rating
        .groupBy(
            "performance_confidence"
        )
        .count()
        .show()
    )

    print(
        "Opponent adjustment range (ELO):"
    )

    rating.agg(
        F.round(
            F.min(
                "opponent_adjustment"
            ),
            4,
        ).alias("min"),

        F.round(
            F.avg(
                "opponent_adjustment"
            ),
            4,
        ).alias("avg"),

        F.round(
            F.max(
                "opponent_adjustment"
            ),
            4,
        ).alias("max"),
    ).show()

    # ========================================================
    # 11. TOURNAMENT RATING
    # ========================================================

    team_matches = (
        stats
        .groupBy(
            "source",
            "season",
            "team_id",
        )
        .agg(
            F.countDistinct(
                "match_id"
            ).alias(
                "team_matches"
            )
        )
    )

    score = F.col(
        "final_performance_score"
    )

    agg = (
        rating
        .groupBy(
            "source",
            "season",
            "player_id",
        )
        .agg(
            F.max(
                F.struct(
                    "minutes_played",
                    "position_group",
                    "position",
                    "player_name",
                    "team_id",
                )
            ).alias("_top"),

            F.countDistinct(
                "match_id"
            ).alias(
                "matches_played"
            ),

            F.sum(
                F.when(
                    F.col("started"),
                    1,
                ).otherwise(0)
            ).alias(
                "starts"
            ),

            F.sum(
                "minutes_played"
            ).alias(
                "total_minutes"
            ),

            F.avg(
                score
            ).alias(
                "average_match_score"
            ),

            F.sum(
                F.when(
                    score.isNotNull(),
                    score
                    * F.col(
                        "minutes_played"
                    ),
                )
            ).alias(
                "_ws"
            ),

            F.sum(
                F.when(
                    score.isNotNull(),
                    F.col(
                        "minutes_played"
                    ),
                )
            ).alias(
                "_wm"
            ),

            F.max(
                score
            ).alias(
                "best_match_score"
            ),

            F.min(
                score
            ).alias(
                "worst_match_score"
            ),

            F.stddev_samp(
                score
            ).alias(
                "score_std"
            ),
        )
        .select(
            "source",
            "season",
            "player_id",

            F.col(
                "_top.player_name"
            ).alias(
                "player_name"
            ),

            F.col(
                "_top.team_id"
            ).alias(
                "team_id"
            ),

            F.col(
                "_top.position"
            ).alias(
                "position"
            ),

            F.col(
                "_top.position_group"
            ).alias(
                "position_group"
            ),

            "matches_played",
            "starts",
            "total_minutes",
            "average_match_score",

            (
                F.col("_ws")
                / F.col("_wm")
            ).alias(
                "minutes_weighted_score"
            ),

            "best_match_score",
            "worst_match_score",
            "score_std",
        )
        .join(
            team_matches,
            [
                "source",
                "season",
                "team_id",
            ],
            "left",
        )
    )

    agg = add(
        agg,
        {
            "consistency_score":
                F.when(
                    F.col(
                        "score_std"
                    ).isNotNull(),

                    100.0
                    - F.least(
                        F.col(
                            "score_std"
                        )
                        / CONSISTENCY_STD_CAP,
                        F.lit(1.0),
                    )
                    * 100.0,
                ),

            "availability":
                F.when(
                    F.col(
                        "team_matches"
                    ) > 0,

                    F.least(
                        F.col(
                            "total_minutes"
                        )
                        / (
                            F.col(
                                "team_matches"
                            )
                            * 90.0
                        )
                        * 100.0,

                        F.lit(100.0),
                    ),
                ),
        },
    )

    tournament = (
        add(
            agg,
            {
                "tournament_rating":
                    clamp(
                        wmean(
                            [
                                (
                                    F.col(
                                        "minutes_weighted_score"
                                    ),
                                    TOURNAMENT_W[
                                        "minutes_weighted"
                                    ],
                                ),

                                (
                                    F.col(
                                        "consistency_score"
                                    ),
                                    TOURNAMENT_W[
                                        "consistency"
                                    ],
                                ),

                                (
                                    F.col(
                                        "best_match_score"
                                    ),
                                    TOURNAMENT_W[
                                        "peak"
                                    ],
                                ),

                                (
                                    F.col(
                                        "availability"
                                    ),
                                    TOURNAMENT_W[
                                        "availability"
                                    ],
                                ),
                            ]
                        ),
                        0,
                        100,
                    ),

                "eligible_for_ranking":
                    F.col(
                        "total_minutes"
                    )
                    >= MIN_MINUTES_FOR_RANKING,
            },
        )
        .select(
            "source",
            "season",
            "player_id",
            "player_name",
            "team_id",
            "position",
            "position_group",
            "matches_played",
            "starts",
            "total_minutes",

            F.round(
                "average_match_score",
                2,
            ).alias(
                "average_match_score"
            ),

            F.round(
                "minutes_weighted_score",
                2,
            ).alias(
                "minutes_weighted_score"
            ),

            "best_match_score",
            "worst_match_score",

            F.round(
                "consistency_score",
                2,
            ).alias(
                "consistency_score"
            ),

            F.round(
                "availability",
                2,
            ).alias(
                "availability"
            ),

            F.round(
                "tournament_rating",
                2,
            ).alias(
                "tournament_rating"
            ),

            "eligible_for_ranking",
        )
        .cache()
    )

    # ========================================================
    # 12. RANKINGS
    # ========================================================

    elig = tournament.filter(
        F.col(
            "eligible_for_ranking"
        )
        & F.col(
            "tournament_rating"
        ).isNotNull()
    )

    def rank_over(*parts):
        return F.rank().over(
            Window.partitionBy(
                "source",
                "season",
                *parts,
            ).orderBy(
                F.desc(
                    "tournament_rating"
                )
            )
        )

    rankings = (
        elig
        .select(
            "source",
            "season",
            "player_id",
            "player_name",
            "team_id",
            "position",
            "position_group",
            "matches_played",
            "total_minutes",
            "tournament_rating",

            rank_over().alias(
                "overall_rank"
            ),

            rank_over(
                "position_group"
            ).alias(
                "position_group_rank"
            ),

            rank_over(
                "position"
            ).alias(
                "position_rank"
            ),
        )
        .orderBy(
            "overall_rank"
        )
    )

    banner(
        "TOURNAMENT RATING / RANKINGS",
        "-",
    )

    print(
        f"Players rated: "
        f"{tournament.count():,}   "
        f"ranked (>= {MIN_MINUTES_FOR_RANKING} min): "
        f"{elig.count():,}"
    )

    print(
        "\nTop 15 overall:"
    )

    (
        rankings
        .select(
            "overall_rank",
            "player_name",
            "team_id",
            "position_group",
            "total_minutes",
            "tournament_rating",
            "position_group_rank",
        )
        .show(
            15,
            False,
        )
    )

    # ========================================================
    # 13. WRITE GOLD
    # ========================================================

    banner(
        "WRITING GOLD PARQUET",
        "-",
    )

    write_parquet(
        rating,
        f"{GOLD_BASE}/player_match_rating",
        "season",
    )

    write_parquet(
        tournament,
        f"{GOLD_BASE}/player_tournament_rating",
        "season",
    )

    write_parquet(
        rankings,
        f"{GOLD_BASE}/player_rankings",
        "season",
    )

    banner(
        "GOLD COMPLETE"
    )

    print(
        "Upload with:"
    )

    print(
        "aws s3 sync "
        "data\\gold\\football "
        "s3://sports-intel-platform-data/gold/football/ "
        "--delete"
    )

    spark.stop()


if __name__ == "__main__":
    main()