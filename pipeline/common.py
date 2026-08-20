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
    with open(os.path.join(configs_dir, f"{client_id}.yaml"), "r") as f:
        return yaml.safe_load(f)


def list_client_ids(configs_dir):
    return sorted(
        fname[:-5] for fname in os.listdir(configs_dir) if fname.endswith(".yaml")
    )


def resolve_paths(base_path):
    """Every path the pipeline touches, derived from a single BASE_PATH.

    Changing BASE_PATH is the only thing required to move this pipeline
    between environments (local disk, DBFS, a fresh workspace, ...).
    """
    return {
        "configs": f"{base_path}/configs",
        "input": f"{base_path}/input_files",
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
