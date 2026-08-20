"""Shared utilities used by every pipeline stage: Spark session creation,
config loading, and path resolution.

Everything here is a pure function of its arguments (no hardcoded paths),
so the same code runs unchanged on a laptop, in a pytest fixture, or inside
a fresh Databricks workspace -- only BASE_PATH changes.
"""
import os

import yaml
from pyspark.sql import SparkSession

NULL_TOKENS = ["", "NULL", "null", "None", "N/A", "n/a"]
CORRUPT_COL = "_corrupt_record"


def get_spark(app_name="client-ingestion-pipeline", use_delta=True):
    """Build (or fetch) the active SparkSession.

    use_delta=True configures the Delta Lake extensions locally via
    delta-spark. On Databricks, Delta support is built into the runtime, so
    this path is skipped there in favor of the cluster's native support.
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
