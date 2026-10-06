import pandas as pd
from pathlib import Path

INPUT = Path("data/silver/football/elo/season=2022/part-00000.parquet")
OUTPUT = INPUT

ELO_TO_TEAM_NAME = {
    "AR": "Argentina",
    "AU": "Australia",
    "BE": "Belgium",
    "BR": "Brazil",
    "CM": "Cameroon",
    "CA": "Canada",
    "CR": "Costa Rica",
    "HR": "Croatia",
    "DK": "Denmark",
    "EC": "Ecuador",
    "EN": "England",
    "FR": "France",
    "DE": "Germany",
    "GH": "Ghana",
    "IR": "Iran",
    "JP": "Japan",
    "KR": "South Korea",
    "MX": "Mexico",
    "MA": "Morocco",
    "NL": "Netherlands",
    "PL": "Poland",
    "PT": "Portugal",
    "QA": "Qatar",
    "SA": "Saudi Arabia",
    "SN": "Senegal",
    "RS": "Serbia",
    "CH": "Switzerland",
    "TN": "Tunisia",
    "US": "United States",
    "UY": "Uruguay",
    "WA": "Wales",
    "ES": "Spain",
}

df = pd.read_parquet(INPUT)

df["team_name"] = df["team_code"].map(ELO_TO_TEAM_NAME)

# Keep only the 2022 World Cup teams.
df = df[df["team_name"].notna()].copy()

print("=== FOOTBALL ELO SILVER 2022 FIX ===")
print("Rows kept:", len(df))
print("Distinct World Cup teams:", df["team_name"].nunique())

print("\nTeams:")
print(sorted(df["team_name"].tolist()))

if df["team_name"].nunique() != 32:
    raise RuntimeError(
        f"Expected 32 World Cup teams, found {df['team_name'].nunique()}"
    )

if df["team_name"].isna().any():
    raise RuntimeError("NULL team_name found.")

if df["elo_rating"].isna().any():
    raise RuntimeError("NULL ELO rating found.")

df = df[
    ["season", "rank", "team_code", "team_name", "elo_rating"]
]

df.to_parquet(OUTPUT, index=False)

print("\nWROTE:", OUTPUT)
print("Columns:", list(df.columns))
print("\n=== COMPLETE ===")
