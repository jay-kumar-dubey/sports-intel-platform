import tempfile
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = "sports-intel-platform-data"
TOTAL_BATCHES = 23

MATCHES_PREFIX = "silver/cricket/matches/"
DELIVERIES_PREFIX = "silver/cricket/deliveries/"

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

s3 = boto3.client("s3")


def normalize(prefix, target_schema, batch_number, tmp):
    key = f"{prefix}batch_{batch_number:04d}.parquet"
    local = tmp / Path(key).name

    s3.download_file(BUCKET, key, str(local))

    table = pq.read_table(local)

    if table.schema.equals(target_schema):
        print(f"OK      {key}")
        return

    missing = set(target_schema.names) - set(table.schema.names)
    if missing:
        raise RuntimeError(f"{key} is missing columns: {missing}")

    fixed = table.select(target_schema.names).cast(target_schema, safe=False)

    out = tmp / f"fixed_{Path(key).name}"
    pq.write_table(fixed, out)

    s3.upload_file(
        str(out), BUCKET, key,
        ExtraArgs={"ServerSideEncryption": "AES256"},
    )
    print(f"FIXED   {key}")


def main():
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        for n in range(1, TOTAL_BATCHES + 1):
            normalize(MATCHES_PREFIX, matches_schema, n, tmp)
            normalize(DELIVERIES_PREFIX, deliveries_schema, n, tmp)
    print("DONE")


if __name__ == "__main__":
    main()