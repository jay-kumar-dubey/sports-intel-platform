import csv, glob, os, re
from pyspark.sql import SparkSession

ELO_FILE = "data/bronze/football/elo/2025.tsv"  # pre-tournament snapshot for the 2026 World Cup
SEASON = 2026
SNAPSHOT_YEAR = 2025
OUT = "data/silver/football/elo"

# Silver team name -> eloratings.net code
NAME_TO_CODE = {
    "Algeria": "DZ",
    "Argentina": "AR",
    "Australia": "AU",
    "Austria": "AT",
    "Belgium": "BE",
    "Bosnia & Herzegovina": "BA",
    "Brazil": "BR",
    "Canada": "CA",
    "Cape Verde": "CV",
    "Colombia": "CO",
    "Croatia": "HR",
    "Curaçao": "CW",
    "Czechia": "CZ",
    "Côte d'Ivoire": "CI",
    "DR Congo": "CD",
    "Ecuador": "EC",
    "Egypt": "EG",
    "England": "EN",
    "France": "FR",
    "Germany": "DE",
    "Ghana": "GH",
    "Haiti": "HT",
    "Iran": "IR",
    "Iraq": "IQ",
    "Japan": "JP",
    "Jordan": "JO",
    "Mexico": "MX",
    "Morocco": "MA",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Norway": "NO",
    "Panama": "PA",
    "Paraguay": "PY",
    "Portugal": "PT",
    "Qatar": "QA",
    "Saudi Arabia": "SA",
    "Scotland": "SQ",
    "Senegal": "SN",
    "South Africa": "ZA",
    "South Korea": "KR",
    "Spain": "ES",
    "Sweden": "SE",
    "Switzerland": "CH",
    "Tunisia": "TN",
    "Türkiye": "TR",
    "USA": "US",
    "Uruguay": "UY",
    "Uzbekistan": "UZ",
}

# 1. Parse the TSV (no header; field 0 is a junk sign, 1 = rank, 2 = code, 3 = rating)
ratings = {}
with open(ELO_FILE, encoding="utf-8", newline="") as f:
    for row in csv.reader(f, delimiter="\t"):
        if len(row) < 4:
            continue
        rank, code, rating = row[1].strip(), row[2].strip(), row[3].strip()
        if rank.isdigit() and re.fullmatch(r"[A-Z]{2}", code) and rating.isdigit():
            ratings[code] = (int(rating), int(rank))
print(f"ELO rows parsed: {len(ratings)}")
assert len(ratings) > 150, "Parsing looks wrong - expected 200+ teams"

# 2. Check every mapped code exists
rows, missing = [], []
for name, code in sorted(NAME_TO_CODE.items()):
    if code in ratings:
        rating, rank = ratings[code]
        rows.append((SEASON, name, code, rating, rank, SNAPSHOT_YEAR))
    else:
        missing.append((name, code))

print(f"Mapped teams: {len(rows)} / {len(NAME_TO_CODE)}")
if missing:
    print("CODES NOT FOUND IN ELO FILE (fix these in NAME_TO_CODE):")
    for n, c in missing:
        print(f"  {n}: {c}")

# 3. Check all Silver match teams are covered
spark = SparkSession.builder.appName("silver_elo").getOrCreate()
mfiles = glob.glob(
    os.path.abspath("data/silver/football/matches") + "/**/*.parquet", recursive=True
)
m = spark.read.parquet(*mfiles)
teams = {
    r[0]
    for r in m.select("home_team_name")
    .union(m.select("away_team_name"))
    .distinct()
    .collect()
}
unmapped = sorted(teams - set(NAME_TO_CODE))
print("Silver teams with no ELO mapping:", unmapped if unmapped else "none")

# 4. Print the sanity table for eyeballing
print("\nTeam / code / rating / world rank:")
for r in sorted(rows, key=lambda x: x[4]):
    print(f"  {r[1]:<22}{r[2]}  {r[3]}  #{r[4]}")

# 5. Write Silver with pyarrow (avoids Spark's Windows commit step)
if missing or unmapped:
    print("\nNOT WRITING: fix the issues above first.")
else:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_dir = os.path.join(OUT, f"season={SEASON}")
    os.makedirs(out_dir, exist_ok=True)
    for old in glob.glob(os.path.join(out_dir, "*")):
        os.remove(old)

    table = pa.table(
        {
            "season":        pa.array([r[0] for r in rows], pa.int32()),
            "team_name":     pa.array([r[1] for r in rows], pa.string()),
            "elo_code":      pa.array([r[2] for r in rows], pa.string()),
            "elo_rating":    pa.array([r[3] for r in rows], pa.int32()),
            "elo_rank":      pa.array([r[4] for r in rows], pa.int32()),
            "snapshot_year": pa.array([r[5] for r in rows], pa.int32()),
        }
    )
    pq.write_table(table, os.path.join(out_dir, "part-00000.parquet"))
    print(f"\nWrote {table.num_rows} rows -> {out_dir}")
    print("Upload with:\n  aws s3 sync data\\silver\\football\\elo s3://sports-intel-platform-data/silver/football/elo/ --delete")