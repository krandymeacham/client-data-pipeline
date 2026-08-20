"""Shared utilities used by every pipeline stage: config loading, path
resolution, and Unity Catalog Volume setup for pipeline output.

Everything here is a pure function of its arguments -- no hardcoded paths,
no session creation of its own. `spark` is always passed in, because this
pipeline runs on Databricks (see notebooks/00_run_pipeline.py), which
already provides one with Delta Lake built in before any of this code runs.
"""
import os

import yaml

NULL_TOKENS = ["", "NULL", "null", "None", "N/A", "n/a"]
CORRUPT_COL = "_corrupt_record"


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
    when given, which defaults to BASE_PATH for the common case where
    everything lives together.

    The Databricks notebook stages configs/input_files onto the Volume
    (BASE_PATH) first and passes that staged location as source_path --
    Spark's distributed execution on serverless compute can't read
    /Workspace/... files directly, only the notebook's own Python process
    can, so they have to land somewhere Spark can actually reach.
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


def ensure_volume(spark, catalog, schema, volume):
    """Idempotently ensure a Unity Catalog schema and volume exist, and
    return the /Volumes path pipeline output should be written to. Volumes
    work the same way on serverless compute and classic clusters, so this
    is the one location every run writes output to. Every statement here
    is IF NOT EXISTS, so calling this on every run is safe and assumes no
    pre-existing state, same as the rest of the pipeline.
    """
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception:
        # Creating a catalog needs metastore-admin privilege most
        # workspace users won't have. If `catalog` already exists -- the
        # common case, e.g. the "workspace"/"main" catalog every Unity
        # Catalog workspace is provisioned with -- that's fine, and
        # schema/volume creation below still works against it.
        pass
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
    return f"/Volumes/{catalog}/{schema}/{volume}"


def write_table(df, path, mode="overwrite"):
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)


def read_table(spark, path):
    return spark.read.format("delta").load(path)
