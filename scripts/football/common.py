"""
Shared helpers for the football Bronze -> Silver -> Gold jobs.

All data transformation is done with Spark DataFrames. The only non-Spark
step is the very last one: writing the finished result to Parquet.

USE_PYARROW_WRITER = True   (default)
    Collect the FINISHED Spark result and write it with PyArrow. This avoids
    Spark's Hadoop file writer, which crashes on Windows without hadoop.dll /
    winutils (the NativeIO$Windows.access0 error). Fine for this data size.

USE_PYARROW_WRITER = False
    Use Spark's own df.write.parquet(). Needs hadoop.dll + winutils.exe in
    C:\\hadoop\\bin, HADOOP_HOME set and that folder on PATH.
"""

import os
import shutil
import sys
from pathlib import Path

# Always run relative to the repo root, wherever the script is launched from.
REPO_ROOT = Path(__file__).resolve().parents[2]
os.chdir(REPO_ROOT)

os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import types as T

USE_PYARROW_WRITER = True


def get_spark(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def list_files(root, pattern):
    """
    List files with plain Python. Spark's own directory listing calls a Hadoop
    native routine that crashes on this Windows setup (NativeIO$Windows.access0),
    but reading files by explicit path works fine.
    """

    root_path = Path(root)

    if not root_path.exists():
        raise FileNotFoundError(f"Directory does not exist: {root}")

    return sorted(p.as_posix() for p in root_path.rglob(pattern))


def read_parquet_dataset(spark, root):
    """Read a (possibly season=... partitioned) Parquet folder via explicit file paths."""

    files = list_files(root, "*.parquet")

    if not files:
        raise FileNotFoundError(f"No Parquet files under {root}")

    return spark.read.option("basePath", root).parquet(*files)


def banner(title, char="="):
    print()
    print(char * 70)
    print(title)
    print(char * 70)


# ------------------------------------------------------------
# Parquet writing
# ------------------------------------------------------------

_ARROW_TYPES = [
    (T.StringType, pa.string()),
    (T.IntegerType, pa.int32()),
    (T.LongType, pa.int64()),
    (T.DoubleType, pa.float64()),
    (T.FloatType, pa.float32()),
    (T.BooleanType, pa.bool_()),
    (T.DateType, pa.date32()),
]


def _arrow_type(spark_type):
    for spark_cls, arrow_type in _ARROW_TYPES:
        if isinstance(spark_type, spark_cls):
            return arrow_type
    raise TypeError(f"Unsupported Spark type for Parquet export: {spark_type}")


def _to_arrow(df):
    """Spark DataFrame -> Arrow table, keeping the exact Spark column types."""

    if hasattr(df, "toArrow"):  # PySpark 4+
        return df.toArrow()

    schema = pa.schema(
        [pa.field(f.name, _arrow_type(f.dataType)) for f in df.schema.fields]
    )

    return pa.Table.from_pandas(df.toPandas(), schema=schema, preserve_index=False)


def write_parquet(df, out_dir, partition_col=None):
    """
    Re-runnable write. With a partition column, only the partitions present
    in `df` are replaced; other seasons already on disk are left alone.
    """

    out = Path(out_dir)

    if not USE_PYARROW_WRITER:
        writer = df.write.mode("overwrite")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.parquet(str(out))
        return

    table = _to_arrow(df)

    if partition_col:
        for value in set(table.column(partition_col).to_pylist()):
            shutil.rmtree(out / f"{partition_col}={value}", ignore_errors=True)

        out.mkdir(parents=True, exist_ok=True)

        pq.write_to_dataset(table, root_path=str(out), partition_cols=[partition_col])

    else:
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True, exist_ok=True)

        pq.write_table(table, str(out / "part-0.parquet"))

    print(f"Wrote {table.num_rows:,} rows -> {out}")
