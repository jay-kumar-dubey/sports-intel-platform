import io

import boto3
import pyarrow.parquet as pq


BUCKET = "sports-intel-platform-data"

FORMATS = {
    "T20I": "gold/cricket/player_match_stats/format=T20I/batch_0001.parquet",
    "ODI": "gold/cricket/player_match_stats/format=ODI/batch_0001.parquet",
    "Test": "gold/cricket/player_match_stats/format=Test/batch_0001.parquet",
}

# Max legal balls one bowler can normally bowl in a match innings-set.
# Used only for WARNINGS (reduced/odd matches can legitimately differ).
MAX_BALLS_BOWLED = {
    "T20I": 24,   # 4 overs
    "ODI": 60,    # 10 overs
}

# Stored values are rounded to 2 decimals, so allow half a hundredth
# (plus a tiny epsilon for floating point error).
ROUNDING_TOLERANCE = 0.005 + 1e-9

S3 = boto3.client("s3")


def load_parquet(s3_key):

    response = S3.get_object(
        Bucket=BUCKET,
        Key=s3_key
    )

    data = response["Body"].read()

    table = pq.read_table(
        io.BytesIO(data)
    )

    return table.to_pandas()


def validate_format(cricket_format, s3_key):

    print()
    print("=" * 70)
    print(f"VALIDATING {cricket_format}")
    print("=" * 70)

    df = load_parquet(s3_key)

    print()
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")

    # --------------------------------------------------------
    # EXPECTED COLUMNS
    # --------------------------------------------------------

    expected_columns = [
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
        "bowling_economy",
    ]

    missing = [
        col
        for col in expected_columns
        if col not in df.columns
    ]

    if missing:
        print(f"[FAIL] Missing columns: {missing}")
        return False

    print("[PASS] Expected columns exist")

    # --------------------------------------------------------
    # FORMAT
    # --------------------------------------------------------

    formats = df["format"].dropna().unique()

    if len(formats) != 1 or formats[0] != cricket_format:
        print(f"[FAIL] Unexpected format values: {formats}")
        return False

    print("[PASS] Format value correct")

    # --------------------------------------------------------
    # PRIMARY KEY
    # --------------------------------------------------------

    key_columns = [
        "match_id",
        "player"
    ]

    duplicate_count = df.duplicated(subset=key_columns).sum()

    if duplicate_count != 0:
        print(f"[FAIL] Duplicate player-match rows: {duplicate_count}")
        return False

    print("[PASS] match_id + player is unique")

    # --------------------------------------------------------
    # REQUIRED FIELDS
    # --------------------------------------------------------

    required_columns = [
        "match_id",
        "player",
        "team",
        "opponent",
    ]

    for column in required_columns:

        nulls = df[column].isna().sum()

        if nulls != 0:
            print(f"[FAIL] {column} contains {nulls} nulls")
            return False

        print(f"[PASS] {column} has no nulls")

    # --------------------------------------------------------
    # TEAM != OPPONENT
    # --------------------------------------------------------

    invalid_team_rows = (df["team"] == df["opponent"]).sum()

    if invalid_team_rows != 0:
        print(f"[FAIL] team == opponent in {invalid_team_rows} rows")
        return False

    print("[PASS] team != opponent")

    # --------------------------------------------------------
    # NON-NEGATIVE METRICS
    # --------------------------------------------------------

    numeric_columns = [
        "runs",
        "balls_faced",
        "fours",
        "sixes",
        "balls_bowled",
        "runs_conceded",
        "wickets",
    ]

    for column in numeric_columns:

        invalid = (df[column].fillna(0) < 0).sum()

        if invalid != 0:
            print(f"[FAIL] Negative values in {column}: {invalid}")
            return False

        print(f"[PASS] {column} has no negative values")

    # --------------------------------------------------------
    # STRIKE RATE CHECK
    # --------------------------------------------------------

    sr_rows = df[df["balls_faced"] > 0].copy()

    sr_rows["sr_diff"] = (
        sr_rows["batting_strike_rate"]
        - sr_rows["runs"] / sr_rows["balls_faced"] * 100
    ).abs()

    sr_mismatch = sr_rows[sr_rows["sr_diff"] > ROUNDING_TOLERANCE]

    # Strike rate must be null when no balls were faced
    sr_should_be_null = df[
        (df["balls_faced"] == 0)
        & df["batting_strike_rate"].notna()
    ]

    if len(sr_mismatch) != 0 or len(sr_should_be_null) != 0:
        print(
            f"[FAIL] Strike-rate mismatches: {len(sr_mismatch)}, "
            f"non-null with no balls faced: {len(sr_should_be_null)}"
        )

        if len(sr_mismatch) > 0:
            print(
                sr_mismatch[
                    [
                        "match_id",
                        "player",
                        "runs",
                        "balls_faced",
                        "batting_strike_rate",
                        "sr_diff",
                    ]
                ].head(20).to_string(index=False)
            )

        return False

    print("[PASS] batting_strike_rate correct")

    # --------------------------------------------------------
    # ECONOMY CHECK
    # --------------------------------------------------------

    economy_rows = df[df["balls_bowled"] > 0].copy()

    exact_economy = (
        economy_rows["runs_conceded"] * 6
        / economy_rows["balls_bowled"]
    )

    economy_rows["economy_diff"] = (
        economy_rows["bowling_economy"] - exact_economy
    ).abs()

    economy_mismatch = economy_rows[
        economy_rows["economy_diff"] > ROUNDING_TOLERANCE
    ]

    # Economy must be null when no balls were bowled
    economy_should_be_null = df[
        (df["balls_bowled"] == 0)
        & df["bowling_economy"].notna()
    ]

    if len(economy_mismatch) != 0 or len(economy_should_be_null) != 0:
        print(
            f"[FAIL] Bowling-economy mismatches: {len(economy_mismatch)}, "
            f"non-null with no balls bowled: {len(economy_should_be_null)}"
        )

        if len(economy_mismatch) > 0:
            print(
                economy_mismatch[
                    [
                        "match_id",
                        "player",
                        "runs_conceded",
                        "balls_bowled",
                        "bowling_economy",
                        "economy_diff",
                    ]
                ].head(20).to_string(index=False)
            )

        return False

    print("[PASS] bowling_economy correct")

    # --------------------------------------------------------
    # WICKETS SANITY (a bowler cannot take more than 10 in an innings;
    # in Tests across two innings the limit is 20)
    # --------------------------------------------------------

    max_wickets = 20 if cricket_format == "Test" else 10

    too_many_wickets = df[df["wickets"] > max_wickets]

    if len(too_many_wickets) != 0:
        print(
            f"[FAIL] wickets > {max_wickets} in "
            f"{len(too_many_wickets)} rows"
        )
        print(
            too_many_wickets[
                ["match_id", "player", "wickets"]
            ].head(20).to_string(index=False)
        )
        return False

    print(f"[PASS] wickets <= {max_wickets} per player-match")

    # --------------------------------------------------------
    # BALLS BOWLED SANITY (warning only)
    # --------------------------------------------------------

    max_balls = MAX_BALLS_BOWLED.get(cricket_format)

    if max_balls is not None:

        over_limit = df[df["balls_bowled"] > max_balls]

        if len(over_limit) != 0:
            print(
                f"[WARN] balls_bowled > {max_balls} in "
                f"{len(over_limit)} rows (check for extra-over / "
                f"super-over matches)"
            )
            print(
                over_limit[
                    ["match_id", "player", "balls_bowled"]
                ].head(10).to_string(index=False)
            )
        else:
            print(f"[PASS] balls_bowled <= {max_balls}")

    # --------------------------------------------------------
    # PLAYER + MATCH COUNT
    # --------------------------------------------------------

    match_count = df["match_id"].nunique()
    player_count = df["player"].nunique()

    print()
    print(f"Unique matches: {match_count}")
    print(f"Unique players: {player_count}")

    print()
    print(f"{cricket_format} GOLD VALIDATION: ALL CHECKS PASSED")

    return True


def main():

    print()
    print("=" * 70)
    print("CRICKET GOLD - PLAYER MATCH VALIDATION")
    print("=" * 70)

    all_passed = True

    for cricket_format, s3_key in FORMATS.items():

        passed = validate_format(
            cricket_format,
            s3_key
        )

        if not passed:
            all_passed = False

    print()
    print("=" * 70)

    if all_passed:
        print("ALL GOLD VALIDATIONS PASSED")
    else:
        print("GOLD VALIDATION FAILED")

    print("=" * 70)


if __name__ == "__main__":
    main()