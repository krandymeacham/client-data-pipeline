# Databricks notebook source
# MAGIC %md
# MAGIC # Client Ingestion Pipeline: Bronze -> Silver -> Gold
# MAGIC
# MAGIC Thin Databricks-native driver for the pipeline in `pipeline/*.py`. All the
# MAGIC actual ingestion/refine/curate logic lives in plain, testable Python
# MAGIC modules (see `tests/`); this notebook just wires them to DBFS.
# MAGIC
# MAGIC **Setup in a fresh workspace**
# MAGIC 1. Repos > Add Repo > this repository's Git URL (branch `main`).
# MAGIC 2. Open this notebook. No cluster to attach or configure -- it runs on
# MAGIC    serverless compute, which provides `spark` (with Delta Lake already
# MAGIC    built in) before the first cell even runs. `pipeline/*.py` never
# MAGIC    builds its own SparkSession; it just takes this notebook's `spark`
# MAGIC    as a plain argument.
# MAGIC 3. Run All. `BASE_PATH` below is the only thing you'd change to point
# MAGIC    this at a different location; every other path is derived from it.

# COMMAND ----------

dbutils.widgets.text("base_path", "/dbfs/tmp/client_ingestion", "BASE_PATH")
dbutils.widgets.text("repo_root", "", "Repo root (blank = auto-detect)")

BASE_PATH = dbutils.widgets.get("base_path")
REPO_ROOT_OVERRIDE = dbutils.widgets.get("repo_root").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Locate the repo checkout, copy configs + input files onto BASE_PATH
# MAGIC
# MAGIC Repos are checked out under a per-user Workspace path that we can't
# MAGIC hardcode; `notebookPath()` gives us this notebook's own location, and
# MAGIC the project root is one directory up from `notebooks/`. Copying
# MAGIC `configs/` and `input_files/` onto BASE_PATH (rather than reading the
# MAGIC repo checkout directly) keeps every downstream path rooted at
# MAGIC BASE_PATH only, per the assignment's portability requirement.

# COMMAND ----------

import os

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

dbutils.fs.mkdirs(BASE_PATH)
dbutils.fs.cp(f"file:{repo_root}/configs", f"{BASE_PATH}/configs", recurse=True)
dbutils.fs.cp(f"file:{repo_root}/input_files", f"{BASE_PATH}/input_files", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run bronze -> silver -> gold
# MAGIC
# MAGIC `spark` here is the session serverless compute already started for
# MAGIC this notebook -- passed straight into `main()` rather than having
# MAGIC `run.py` build (or serverless refuse to let it build) one of its own.

# COMMAND ----------

import sys

sys.path.insert(0, repo_root)

from run import main

gold_counts = main(BASE_PATH, output_format="delta", spark=spark)
gold_counts

# COMMAND ----------

# MAGIC %md ## 3. Curated (gold) tables

# COMMAND ----------

display(spark.read.format("delta").load(f"{BASE_PATH}/output/curated/customers"))

# COMMAND ----------

display(spark.read.format("delta").load(f"{BASE_PATH}/output/curated/transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine summary
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
