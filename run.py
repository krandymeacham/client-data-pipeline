"""Entry point: runs the full bronze -> silver -> gold pipeline end to end.

Called from notebooks/00_run_pipeline.py with the notebook's own `spark`
(Databricks provides one, with Delta Lake built in, before the notebook's
first cell runs) and BASE_PATH pointed at a Unity Catalog Volume. Every
other path is derived from BASE_PATH (see pipeline/common.py:resolve_paths).
Re-running is safe -- every layer is written with mode("overwrite").

`source_path` is optional and only needed when configs/input_files should
be read from somewhere other than BASE_PATH -- the notebook reads them
straight from its Repo/Git-folder checkout while BASE_PATH is output only.
"""
from pipeline.common import list_client_ids, load_client_config, resolve_paths
from pipeline.curate import run_curate
from pipeline.ingest import run_ingest
from pipeline.refine import run_refine


def main(base_path, spark, source_path=None):
    paths = resolve_paths(base_path, source_path=source_path)

    client_ids = list_client_ids(paths["configs"])
    client_configs = {cid: load_client_config(paths["configs"], cid) for cid in client_ids}
    print(f"Clients discovered from {paths['configs']}: {client_ids}")

    print("\n=== BRONZE (raw) ===")
    for r in run_ingest(spark, client_configs, paths["input"], paths["raw"]):
        print(f"  {r['client_id']:10s} {r['entity']:14s} rows={r['row_count']:>6}  -> {r['path']}")

    print("\n=== SILVER (refined) ===")
    for r in run_refine(spark, client_configs, paths["raw"], paths["refined"], paths["quarantine"]):
        print(
            f"  {r['client_id']:10s} {r['entity']:14s} "
            f"clean={r['clean_count']:>6}  quarantined={r['quarantine_count']:>4}"
        )

    print("\n=== GOLD (curated) ===")
    gold_counts = run_curate(spark, client_configs, paths["refined"], paths["curated"])
    for entity, count in gold_counts.items():
        print(f"  {entity:14s} rows={count}")

    return gold_counts
