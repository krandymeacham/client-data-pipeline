"""Entry point: runs the full bronze -> silver -> gold pipeline end to end.

Called from notebooks/00_run_pipeline.py with the notebook's own `spark`
(Databricks provides one, with Delta Lake built in, before the notebook's
first cell runs), the bronze/silver/gold schema names the notebook already
created via pipeline.common.ensure_schema, and source_path pointing at
configs/input_files staged onto a Volume (see pipeline/common.py and the
README for why staging is needed on serverless compute). Re-running is
safe -- every table is written with mode("overwrite").
"""
from pipeline.common import list_client_ids, load_client_config
from pipeline.curate import run_curate
from pipeline.ingest import run_ingest
from pipeline.refine import run_refine


def main(bronze_schema, silver_schema, gold_schema, spark, source_path):
    configs_path = f"{source_path}/configs"
    input_path = f"{source_path}/input_files"

    client_ids = list_client_ids(configs_path)
    client_configs = {cid: load_client_config(configs_path, cid) for cid in client_ids}
    print(f"Clients discovered from {configs_path}: {client_ids}")

    print("\n=== BRONZE ===")
    for r in run_ingest(spark, client_configs, input_path, bronze_schema):
        print(f"  {r['client_id']:10s} {r['entity']:14s} rows={r['row_count']:>6}  -> {r['table']}")

    print("\n=== SILVER ===")
    for r in run_refine(spark, client_configs, bronze_schema, silver_schema):
        print(
            f"  {r['client_id']:10s} {r['entity']:14s} "
            f"clean={r['clean_count']:>6}  quarantined={r['quarantine_count']:>4}"
        )

    print("\n=== GOLD ===")
    gold_counts = run_curate(spark, client_configs, silver_schema, gold_schema)
    for entity, count in gold_counts.items():
        print(f"  {entity:14s} rows={count}")

    return gold_counts
