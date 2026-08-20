"""Unit tests for pipeline.common: config loading, path resolution, and
Unity Catalog Volume setup for pipeline output.
"""
from unittest.mock import MagicMock

import pytest

from pipeline.common import ensure_volume, load_client_config, resolve_paths


def test_load_client_config_empty_file_raises_clear_error(tmp_path):
    (tmp_path / "client_x.yaml").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="client_x.yaml"):
        load_client_config(str(tmp_path), "client_x")


def test_load_client_config_reads_real_yaml(tmp_path):
    (tmp_path / "client_x.yaml").write_text("client_id: client_x\ncustomers:\n  file: c.txt\n", encoding="utf-8")
    cfg = load_client_config(str(tmp_path), "client_x")
    assert cfg["client_id"] == "client_x"


def test_resolve_paths_defaults_source_to_base_path():
    paths = resolve_paths("/base")
    assert paths["configs"] == "/base/configs"
    assert paths["input"] == "/base/input_files"
    assert paths["raw"] == "/base/output/raw"


def test_resolve_paths_source_path_only_affects_configs_and_input():
    # Mirrors the Databricks notebook: BASE_PATH (the Volume) for output,
    # source_path (configs/input_files staged onto that same Volume) for
    # everything Spark needs to read.
    base_path = "/Volumes/workspace/client_ingestion/client_ingestion"
    source_path = f"{base_path}/_source"
    paths = resolve_paths(base_path, source_path=source_path)
    assert paths["configs"] == f"{source_path}/configs"
    assert paths["input"] == f"{source_path}/input_files"
    assert paths["raw"] == f"{base_path}/output/raw"
    assert paths["curated"] == f"{base_path}/output/curated"


def test_ensure_volume_creates_catalog_schema_volume_and_returns_path():
    spark = MagicMock()
    path = ensure_volume(spark, "workspace", "client_ingestion", "client_ingestion")

    assert path == "/Volumes/workspace/client_ingestion/client_ingestion"
    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert any("CREATE CATALOG IF NOT EXISTS" in s for s in statements)
    assert any("CREATE SCHEMA IF NOT EXISTS" in s and "`workspace`.`client_ingestion`" in s for s in statements)
    assert any("CREATE VOLUME IF NOT EXISTS" in s for s in statements)


def test_ensure_volume_tolerates_catalog_creation_failure():
    # Creating a catalog needs metastore-admin privilege most workspace
    # users won't have -- ensure_volume should still succeed against an
    # already-existing catalog (e.g. the default "workspace" catalog).
    spark = MagicMock()
    spark.sql.side_effect = [Exception("PERMISSION_DENIED"), None, None]

    path = ensure_volume(spark, "workspace", "s", "v")

    assert path == "/Volumes/workspace/s/v"
    assert spark.sql.call_count == 3
