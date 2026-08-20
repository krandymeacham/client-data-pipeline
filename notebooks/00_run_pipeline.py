# Databricks notebook source
# MAGIC %md
# MAGIC # Client Ingestion Pipeline: Bronze -> Silver -> Gold
# MAGIC
# MAGIC Thin Databricks-native driver for the pipeline in `pipeline/*.py`. All the
# MAGIC actual ingestion/refine/curate logic lives in plain, testable Python
# MAGIC modules (see `tests/`).
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
# MAGIC 3. Run All. The catalog/schema/volume widgets below (for pipeline
# MAGIC    *output* only) are the one thing you'd normally change; everything
# MAGIC    else is derived from them or auto-detected.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "client_ingestion", "Schema (created if missing)")
dbutils.widgets.text("volume", "client_ingestion", "Volume (created if missing)")
dbutils.widgets.text("base_path", "", "BASE_PATH override (blank = auto /Volumes path)")
dbutils.widgets.text("repo_root", "", "Repo root (blank = auto-detect)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
BASE_PATH_OVERRIDE = dbutils.widgets.get("base_path").strip()
REPO_ROOT_OVERRIDE = dbutils.widgets.get("repo_root").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Locate the repo checkout
# MAGIC
# MAGIC `configs/` and `input_files/` are read directly from here -- **not**
# MAGIC copied onto BASE_PATH first. Serverless compute has no `/dbfs` FUSE
# MAGIC mount, so a `dbutils.fs.cp` from a `file:/Workspace/...` source can
# MAGIC silently land as 0-byte files there; reading Workspace files directly
# MAGIC (plain Python for the tiny YAML configs, Spark's normal file reader for
# MAGIC the CSVs) sidesteps that entirely and is one less moving part besides.

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
# MAGIC ## 2. Point pipeline output at a Unity Catalog Volume
# MAGIC
# MAGIC Newer workspaces -- serverless-only and free-tier ones especially --
# MAGIC disable the public DBFS root entirely (`[DBFS_DISABLED] Public DBFS
# MAGIC root is disabled`). Volumes are the portable replacement and work the
# MAGIC same way on serverless compute and classic clusters; creating the
# MAGIC schema/volume with `IF NOT EXISTS` keeps this safe to re-run, same as
# MAGIC every other layer. (Catalog creation is attempted too but silently
# MAGIC skipped on failure -- that needs metastore-admin privilege most users
# MAGIC won't have, and the default `workspace` catalog already exists in any
# MAGIC Unity Catalog workspace.)

# COMMAND ----------

from pipeline.common import ensure_volume

if BASE_PATH_OVERRIDE:
    BASE_PATH = BASE_PATH_OVERRIDE
else:
    BASE_PATH = ensure_volume(spark, CATALOG, SCHEMA, VOLUME)

print("BASE_PATH:", BASE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sanity-check the configs before running anything
# MAGIC
# MAGIC Loads each client's YAML the exact same way `run.py` will, so a bad
# MAGIC path or an empty file fails here with a clear message instead of a
# MAGIC generic error a few cells later.

# COMMAND ----------

from pipeline.common import list_client_ids, load_client_config

client_ids = list_client_ids(f"{repo_root}/configs")
print("clients found:", client_ids)
for client_id in client_ids:
    cfg = load_client_config(f"{repo_root}/configs", client_id)
    print(f"  {client_id}: entities={list(cfg.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run bronze -> silver -> gold
# MAGIC
# MAGIC `spark` here is the session serverless compute already started for
# MAGIC this notebook -- passed straight into `main()` rather than having
# MAGIC `run.py` build (or serverless refuse to let it build) one of its own.
# MAGIC `source_path=repo_root` is what points configs/input_files at the repo
# MAGIC checkout instead of BASE_PATH; see `pipeline/common.py:resolve_paths`.

# COMMAND ----------

from run import main

gold_counts = main(BASE_PATH, output_format="delta", spark=spark, source_path=repo_root)
gold_counts

# COMMAND ----------

# MAGIC %md ## 5. Curated (gold) tables

# COMMAND ----------

display(spark.read.format("delta").load(f"{BASE_PATH}/output/curated/customers"))

# COMMAND ----------

display(spark.read.format("delta").load(f"{BASE_PATH}/output/curated/transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quarantine summary
# MAGIC
# MAGIC What got set aside during refine, and why -- the data-quality visibility
# MAGIC the assignment asks for, rather than rows silently vanishing.

# COMMAND ----------

quarantine_root = f"{BASE_PATH}/output/refined_quarantine"
for client_dir in dbutils.fs.ls(quarantine_root):
    for entity_dir in dbutils.fs.ls(client_dir.path):
        df = spark.read.format("delta").load(entity_dir.path)
        print(entity_dir.path)
        df.groupBy("_quarantine_reason").count().orderBy("count", ascending=False).show(truncate=80)
