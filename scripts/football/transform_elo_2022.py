from pathlib import Path
import shutil

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]

YEAR = 2021
SEASON = 2022

INPUT = REPO_ROOT / f"data/bronze/football/elo/{YEAR}.tsv"
OUTPUT = REPO_ROOT / f"data/silver/football/elo/season={SEASON}"

print(f"=== FOOTBALL ELO SILVER {SEASON} ===")
print("INPUT:", INPUT)
print("OUTPUT:", OUTPUT)

if not INPUT.exists():
    raise FileNotFoundError(INPUT)

# ------------------------------------------------------------
# READ WORLD FOOTBALL ELO TSV
# ------------------------------------------------------------
rows = []

with open(INPUT, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        parts = line.split()
        if len(parts) < 4:
            continue

        try:
            rank = int(parts[1])
            team_code = parts[2]
            elo_rating = float(parts[3])
        except (ValueError, IndexError):
            continue

        rows.append((SEASON, rank, team_code, elo_rating))

df = pd.DataFrame(rows, columns=["season", "rank", "team_code", "elo_rating"])
df = df.astype(
    {
        "season": "int32",
        "rank": "int32",
        "team_code": "string",
        "elo_rating": "float64",
    }
)
df = df.sort_values(["rank", "team_code"]).reset_index(drop=True)

# ------------------------------------------------------------
# VALIDATION
# ------------------------------------------------------------
print("\n=== VALIDATION ===")
print("ROWS:", len(df))
print("DISTINCT TEAMS:", df["team_code"].nunique())
print("NULL RANK:", int(df["rank"].isna().sum()))
print("NULL TEAM CODE:", int(df["team_code"].isna().sum()))
print("NULL ELO:", int(df["elo_rating"].isna().sum()))
print("DUPLICATES:", int(df["team_code"].duplicated().sum()))

if len(df) == 0:
    raise RuntimeError("No rows parsed from the input file")

print("\nTOP 20:")
print(df.head(20).to_string(index=False))

# ------------------------------------------------------------
# WRITE SILVER (overwrite)
# ------------------------------------------------------------
if OUTPUT.exists():
    shutil.rmtree(OUTPUT)
OUTPUT.mkdir(parents=True, exist_ok=True)

out_file = OUTPUT / "part-00000.parquet"
df.to_parquet(out_file, engine="pyarrow", index=False)

print("\nWROTE:", out_file)
print("\n=== ELO SILVER COMPLETE ===")
