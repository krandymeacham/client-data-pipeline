"""Shared utilities used by every pipeline stage: config loading, Unity
Catalog setup, and reading/writing Delta tables.

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


def _ensure_catalog(spark, catalog):
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    except Exception:
        # Creating a catalog needs metastore-admin privilege most
        # workspace users won't have. If `catalog` already exists -- the
        # common case, e.g. the "workspace"/"main" catalog every Unity
        # Catalog workspace is provisioned with -- that's fine, and
        # schema creation below still works against it.
        pass


def ensure_schema(spark, catalog, schema):
    """Idempotently ensure catalog.schema exists (creating the catalog too
    where privilege allows -- see _ensure_catalog) and return "catalog.schema"
    for use as a table-name prefix. Every statement is IF NOT EXISTS, so
    calling this on every run is safe and assumes no pre-existing state,
    same as the rest of the pipeline.
    """
    _ensure_catalog(spark, catalog)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    return f"{catalog}.{schema}"


def ensure_volume(spark, catalog, schema, volume):
    """Idempotently ensure a Unity Catalog schema and volume exist, and
    return the /Volumes path used to stage configs/input_files before
    ingestion. Volumes work the same way on serverless compute and classic
    clusters; see the "Spark reads on serverless compute" note in the
    README for why staging is needed at all.
    """
    ensure_schema(spark, catalog, schema)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
    return f"/Volumes/{catalog}/{schema}/{volume}"


def write_table(df, table_name, mode="overwrite"):
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table_name)


def read_table(spark, table_name):
    return spark.table(table_name)
