from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

BASE = Path(__file__).resolve().parent


def load(folder):
    files = sorted((BASE / folder).glob("batch_*.parquet"))
    print(f"{folder}: {len(files)} files")
    return pa.concat_tables([pq.read_table(f) for f in files])


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    return ok


def main():
    print("=" * 70)
    print("T20I SILVER VALIDATION")
    print("=" * 70)

    matches = load("matches")
    deliveries = load("deliveries")

    print(f"Total matches:    {matches.num_rows}")
    print(f"Total deliveries: {deliveries.num_rows}")
    print()

    results = []

    # Uniqueness
    match_ids = matches.column("match_id")
    results.append(check(
        "match_id unique in matches",
        len(pc.unique(match_ids)) == matches.num_rows,
    ))

    event_ids = deliveries.column("event_id")
    results.append(check(
        "event_id unique in deliveries",
        len(pc.unique(event_ids)) == deliveries.num_rows,
    ))

    # Referential integrity
    delivery_match_ids = pc.unique(deliveries.column("match_id"))
    orphan = pc.invert(pc.is_in(delivery_match_ids, value_set=match_ids))
    n_orphan = pc.sum(orphan).as_py() or 0
    results.append(check(
        "every delivery's match_id exists in matches",
        n_orphan == 0,
        f"(orphans: {n_orphan})",
    ))

    no_deliveries = pc.invert(
        pc.is_in(match_ids, value_set=delivery_match_ids)
    )
    n_empty = pc.sum(no_deliveries).as_py() or 0
    results.append(check(
        "every match has deliveries",
        n_empty == 0,
        f"(matches without deliveries: {n_empty})",
    ))

    # Null checks on key columns
    for col in ["match_id", "date", "team_1", "team_2"]:
        n = matches.column(col).null_count
        results.append(check(f"matches.{col} has no nulls", n == 0, f"(nulls: {n})"))

    for col in ["match_id", "innings_number", "batter", "bowler",
                "batter_runs", "extra_runs", "total_runs", "event_id"]:
        n = deliveries.column(col).null_count
        results.append(check(f"deliveries.{col} has no nulls", n == 0, f"(nulls: {n})"))

    # Run arithmetic
    expected = pc.add(deliveries.column("batter_runs"),
                      deliveries.column("extra_runs"))
    mismatch = pc.sum(
        pc.cast(pc.not_equal(expected, deliveries.column("total_runs")), pa.int64())
    ).as_py() or 0
    results.append(check(
        "total_runs == batter_runs + extra_runs",
        mismatch == 0,
        f"(mismatches: {mismatch})",
    ))

    print()
    print("=" * 70)
    if all(results):
        print("ALL CHECKS PASSED")
    else:
        print(f"{results.count(False)} CHECK(S) FAILED")
    print("=" * 70)


if __name__ == "__main__":
    main()