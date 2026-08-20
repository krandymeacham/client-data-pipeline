# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Client Ingestion Pipeline: Bronze -> Silver -> Gold
# MAGIC
# MAGIC Thin Databricks-native driver for the pipeline in `pipeline/*.py`. All the
# MAGIC actual ingestion/refine/curate logic lives in plain, testable Python
# MAGIC modules (see `tests/`).
# MAGIC
# MAGIC Output is Unity Catalog managed tables, one schema per medallion layer:
# MAGIC `{catalog}.{schema}_bronze`, `{schema}_silver`, `{schema}_gold`. Bronze and
# MAGIC silver hold one table per client per entity (`client_a_customers`,
# MAGIC `client_a_transactions`, ...) since each entity has a genuinely different
# MAGIC schema; silver also holds a `..._quarantine` table alongside each clean
# MAGIC one. Gold holds exactly two tables -- `customers` and `transactions` --
# MAGIC aggregated across every client.
# MAGIC
# MAGIC **Setup in a fresh workspace**
# MAGIC 1. Add this repo as a Git folder under your Workspace (Repos > Add Repo,
# MAGIC    or the newer "Git folder" flow -- either way it lands under
# MAGIC    `/Workspace/...`).
# MAGIC 2. Open this notebook. No cluster to attach or configure -- it runs on
# MAGIC    serverless compute, which provides `spark` (with Delta Lake already
# MAGIC    built in) before the first cell even runs. `pipeline/*.py` never
# MAGIC    builds its own SparkSession; it just takes this notebook's `spark`
# MAGIC    as a plain argument.
# MAGIC 3. Run All. The catalog/schema widgets below are the one thing you'd
# MAGIC    normally change; everything else is derived from them or auto-detected.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "client_ingestion", "Schema prefix (bronze/silver/gold appended)")
dbutils.widgets.text("volume", "source_files", "Staging volume (created if missing)")
dbutils.widgets.text("repo_root", "", "Repo root (blank = auto-detect)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
REPO_ROOT_OVERRIDE = dbutils.widgets.get("repo_root").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Locate the repo checkout

# COMMAND ----------

import os
import sys

if REPO_ROOT_OVERRIDE:
    repo_root = REPO_ROOT_OVERRIDE
else:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )
    workspace_notebook_dir = os.path.dirname(notebook_path)
    repo_root = "/Workspace" + os.path.dirname(workspace_notebook_dir)

print("repo_root:", repo_root)
assert os.path.isdir(f"{repo_root}/pipeline"), (
    f"Couldn't find pipeline/ under {repo_root} -- set the 'Repo root' widget explicitly."
)
sys.path.insert(0, repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the bronze/silver/gold schemas and a staging volume
# MAGIC
# MAGIC Everything here is `IF NOT EXISTS`, so this is safe to re-run. (Catalog
# MAGIC creation is attempted too but silently skipped on failure -- that needs
# MAGIC metastore-admin privilege most users won't have, and the default
# MAGIC `workspace` catalog already exists in any Unity Catalog workspace.)
# MAGIC
# MAGIC The volume holds only staged `configs`/`input_files` -- pipeline output
# MAGIC is tables now, not files.

# COMMAND ----------

from pipeline.common import ensure_schema, ensure_volume

BRONZE_SCHEMA = ensure_schema(spark, CATALOG, f"{SCHEMA}_bronze")
SILVER_SCHEMA = ensure_schema(spark, CATALOG, f"{SCHEMA}_silver")
GOLD_SCHEMA = ensure_schema(spark, CATALOG, f"{SCHEMA}_gold")
STAGING_PATH = ensure_volume(spark, CATALOG, SCHEMA, VOLUME)

print("bronze schema:", BRONZE_SCHEMA)
print("silver schema:", SILVER_SCHEMA)
print("gold schema:  ", GOLD_SCHEMA)
print("staging path: ", STAGING_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Stage configs + input_files onto the volume
# MAGIC
# MAGIC Serverless compute runs Spark's actual query execution on separate
# MAGIC infrastructure from the notebook's own Python process, so `spark.read`
# MAGIC can't see `/Workspace/...` files at all -- no scheme fixes that, because
# MAGIC it isn't a path-format problem. `dbutils.fs.cp`, unlike `spark.read`, is
# MAGIC a driver-side utility rather than a distributed Spark job, so it *can*
# MAGIC see Workspace files -- copying them onto the volume, which both
# MAGIC `dbutils.fs` and `spark.read` reliably reach, is what actually fixes it.
# MAGIC `recurse=True` overwrites, so this is safe to re-run.

# COMMAND ----------

dbutils.fs.cp(f"file:{repo_root}/configs", f"{STAGING_PATH}/configs", recurse=True)
dbutils.fs.cp(f"file:{repo_root}/input_files", f"{STAGING_PATH}/input_files", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity-check the configs before running anything
# MAGIC
# MAGIC Loads each client's YAML the exact same way `run.py` will, so a bad
# MAGIC path or an empty file fails here with a clear message instead of a
# MAGIC generic error a few cells later.

# COMMAND ----------

from pipeline.common import list_client_ids, load_client_config

client_ids = list_client_ids(f"{STAGING_PATH}/configs")
client_configs = {cid: load_client_config(f"{STAGING_PATH}/configs", cid) for cid in client_ids}
print("clients found:", client_ids)
for client_id, cfg in client_configs.items():
    print(f"  {client_id}: entities={list(cfg.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run bronze -> silver -> gold
# MAGIC
# MAGIC `spark` here is the session serverless compute already started for
# MAGIC this notebook -- passed straight into `main()` rather than having
# MAGIC `run.py` build (or serverless refuse to let it build) one of its own.

# COMMAND ----------

from run import main

gold_counts = main(BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, spark, source_path=STAGING_PATH)
gold_counts

# COMMAND ----------

# MAGIC %md ## 6. Curated (gold) tables

# COMMAND ----------

display(spark.table(f"{GOLD_SCHEMA}.customers"))

# COMMAND ----------

display(spark.table(f"{GOLD_SCHEMA}.transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quarantine summary
# MAGIC
# MAGIC What got set aside during refine, and why - These could be records prompted for business review. 

# COMMAND ----------

for client_id, cfg in client_configs.items():
    for entity_name in ("customers", "transactions"):
        if entity_name not in cfg:
            continue
        table = f"{SILVER_SCHEMA}.{client_id}_{entity_name}_quarantine"
        print(table)
        spark.table(table).groupBy("_quarantine_reason").count().orderBy(
            "count", ascending=False
        ).show(truncate=80)
