"""Shared utilities used by every pipeline stage: local Spark session
creation (for non-Databricks use only -- see get_spark below), config
loading, and path resolution.

Everything here is a pure function of its arguments (no hardcoded paths,
no dependency on a pre-existing SparkSession), so the same code runs
unchanged on a laptop, in a pytest fixture, or on Databricks -- classic
clusters and serverless compute alike -- with only BASE_PATH changing.
"""
import os

import yaml
from pyspark.sql import SparkSession

NULL_TOKENS = ["", "NULL", "null", "None", "N/A", "n/a"]
CORRUPT_COL = "_corrupt_record"


def get_spark(app_name="client-ingestion-pipeline", use_delta=True):
    """Build a local SparkSession for running outside Databricks (the CLI
    entry point in run.py, and the pytest fixture in tests/conftest.py).

    Never called on Databricks. Serverless compute doesn't support building
    or reconfiguring a session at all -- a SparkSession (with Delta already
    built in) is provided by the notebook before any of this code runs, and
    every pipeline function takes `spark` as a plain argument, so
    notebooks/00_run_pipeline.py just passes that one through instead of
    calling this function. use_delta=True here configures the Delta Lake
    extensions locally via delta-spark, since -- unlike on Databricks --
    nothing provides them by default.
    """
    builder = SparkSession.builder.appName(app_name).master(
        os.environ.get("SPARK_MASTER", "local[*]")
    )
    if use_delta:
        try:
            from delta import configure_spark_with_delta_pip

            builder = builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            pass
    return builder.getOrCreate()


def load_client_config(configs_dir, client_id):
    path = os.path.join(configs_dir, f"{client_id}.yaml")
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    if config is None:
        # yaml.safe_load returns None (not an error) for an empty file --
        # most often the result of a copy step that produced a 0-byte file
        # rather than an actual YAML syntax problem. Failing here with the
        # exact path, instead of letting `None` propagate into
        # `"customers" in cfg`-style checks downstream, turns a cryptic
        # "argument of type 'NoneType' is not iterable" several calls deep
        # into an immediate, actionable error.
        raise ValueError(f"{path} loaded as empty/None -- check that the file actually has content.")
    return config


def list_client_ids(configs_dir):
    return sorted(
        fname[:-5] for fname in os.listdir(configs_dir) if fname.endswith(".yaml")
    )


def resolve_paths(base_path, source_path=None):
    """Every path the pipeline touches. Output (raw/refined/quarantine/
    curated) always lives under BASE_PATH -- the one thing that has to be
    writable. configs/ and input_files/ are read from source_path instead
    when given, which defaults to BASE_PATH for the common case (a local
    checkout, or a workspace where copying source files onto BASE_PATH
    first, per the assignment's suggested pattern, works fine).

    Databricks notebooks pass source_path=<repo checkout path> and point
    BASE_PATH at DBFS instead, reading configs/input_files straight from
    the Repo/Git-folder checkout: serverless compute has no /dbfs FUSE
    mount, so copying those onto BASE_PATH first isn't reliable there,
    and skipping that copy also means one less moving part in general.
    """
    source_path = source_path or base_path
    return {
        "configs": f"{source_path}/configs",
        "input": f"{source_path}/input_files",
        "raw": f"{base_path}/output/raw",
        "refined": f"{base_path}/output/refined",
        "quarantine": f"{base_path}/output/refined_quarantine",
        "curated": f"{base_path}/output/curated",
    }


def write_table(df, path, output_format="delta", mode="overwrite"):
    writer = df.write.format(output_format).mode(mode)
    if output_format == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def read_table(spark, path, output_format="delta"):
    return spark.read.format(output_format).load(path)
