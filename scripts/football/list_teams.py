import glob, os
from pyspark.sql import SparkSession

files = glob.glob(
    os.path.abspath("data/silver/football/matches") + "/**/*.parquet", recursive=True
)
print("parquet files found:", len(files))

spark = SparkSession.builder.appName("teams").getOrCreate()
df = spark.read.parquet(*files)
print(df.columns)

cols = [c for c in ("home_team_name", "away_team_name") if c in df.columns]
names = None
for c in cols:
    s = df.select(df[c].alias("team"))
    names = s if names is None else names.union(s)

if names is not None:
    for r in names.distinct().orderBy("team").collect():
        print(r["team"])
