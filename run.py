"""Entry point: runs the full bronze -> silver -> gold pipeline end to end.

    python run.py --base-path /dbfs/tmp/client_ingestion --format delta

The only thing that changes between a laptop, pytest, and a fresh Databricks
workspace is --base-path (equivalently the BASE_PATH env var); every other
path is derived from it (see pipeline/common.py:resolve_paths). Re-running
is safe -- every layer is written with mode("overwrite").

On Databricks -- including serverless compute -- a SparkSession is already
running before this module is even imported, so main() accepts one via the
`spark` argument instead of creating its own (see notebooks/00_run_pipeline.py,
which passes the notebook's ambient `spark`). Building a session with
pipeline.common.get_spark() only happens here, and only when nothing was
passed in -- i.e. for the local CLI / pytest path below, not on Databricks.

`source_path` is optional and only needed when configs/input_files should
be read from somewhere other than BASE_PATH -- e.g. a Databricks notebook
reading them straight from its Repo/Git-folder checkout while BASE_PATH
points at DBFS for output only (see pipeline/common.py:resolve_paths).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.common import get_spark, list_client_ids, load_client_config, resolve_paths
from pipeline.curate import run_curate
from pipeline.ingest import run_ingest
from pipeline.refine import run_refine


def main(base_path, output_format="delta", spark=None, source_path=None):
    paths = resolve_paths(base_path, source_path=source_path)
    if spark is None:
        spark = get_spark(use_delta=(output_format == "delta"))

    client_ids = list_client_ids(paths["configs"])
    client_configs = {cid: load_client_config(paths["configs"], cid) for cid in client_ids}
    print(f"Clients discovered from {paths['configs']}: {client_ids}")

    print("\n=== BRONZE (raw) ===")
    for r in run_ingest(spark, client_configs, paths["input"], paths["raw"], output_format):
        print(f"  {r['client_id']:10s} {r['entity']:14s} rows={r['row_count']:>6}  -> {r['path']}")

    print("\n=== SILVER (refined) ===")
    for r in run_refine(
        spark, client_configs, paths["raw"], paths["refined"], paths["quarantine"], output_format
    ):
        print(
            f"  {r['client_id']:10s} {r['entity']:14s} "
            f"clean={r['clean_count']:>6}  quarantined={r['quarantine_count']:>4}"
        )

    print("\n=== GOLD (curated) ===")
    gold_counts = run_curate(spark, client_configs, paths["refined"], paths["curated"], output_format)
    for entity, count in gold_counts.items():
        print(f"  {entity:14s} rows={count}")

    return gold_counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the client ingestion pipeline end to end.")
    parser.add_argument(
        "--base-path",
        default=os.environ.get("BASE_PATH", os.path.dirname(os.path.abspath(__file__))),
        help="Root containing configs/ and input_files/; output/{raw,refined,curated} is written under it.",
    )
    parser.add_argument(
        "--format",
        default=os.environ.get("OUTPUT_FORMAT", "delta"),
        choices=["delta", "parquet"],
        help="Storage format for every layer (default: delta; native on Databricks).",
    )
    args = parser.parse_args()
    main(args.base_path, args.format)
