"""Unit tests for pipeline.common: config loading and Unity Catalog setup
(schema/volume creation).
"""
from unittest.mock import MagicMock

import pytest

from pipeline.common import ensure_schema, ensure_volume, load_client_config


def test_load_client_config_empty_file_raises_clear_error(tmp_path):
    (tmp_path / "client_x.yaml").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="client_x.yaml"):
        load_client_config(str(tmp_path), "client_x")


def test_load_client_config_reads_real_yaml(tmp_path):
    (tmp_path / "client_x.yaml").write_text("client_id: client_x\ncustomers:\n  file: c.txt\n", encoding="utf-8")
    cfg = load_client_config(str(tmp_path), "client_x")
    assert cfg["client_id"] == "client_x"


def test_ensure_schema_creates_catalog_and_schema_and_returns_name():
    spark = MagicMock()
    result = ensure_schema(spark, "workspace", "client_ingestion_bronze")

    assert result == "workspace.client_ingestion_bronze"
    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert any("CREATE CATALOG IF NOT EXISTS" in s for s in statements)
    assert any(
        "CREATE SCHEMA IF NOT EXISTS" in s and "`workspace`.`client_ingestion_bronze`" in s
        for s in statements
    )


def test_ensure_schema_tolerates_catalog_creation_failure():
    # Creating a catalog needs metastore-admin privilege most workspace
    # users won't have -- ensure_schema should still succeed against an
    # already-existing catalog (e.g. the default "workspace" catalog).
    spark = MagicMock()
    spark.sql.side_effect = [Exception("PERMISSION_DENIED"), None]

    result = ensure_schema(spark, "workspace", "s")

    assert result == "workspace.s"
    assert spark.sql.call_count == 2


def test_ensure_volume_creates_schema_and_volume_and_returns_path():
    spark = MagicMock()
    path = ensure_volume(spark, "workspace", "client_ingestion", "source_files")

    assert path == "/Volumes/workspace/client_ingestion/source_files"
    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert any("CREATE CATALOG IF NOT EXISTS" in s for s in statements)
    assert any("CREATE SCHEMA IF NOT EXISTS" in s and "`workspace`.`client_ingestion`" in s for s in statements)
    assert any(
        "CREATE VOLUME IF NOT EXISTS" in s and "`workspace`.`client_ingestion`.`source_files`" in s
        for s in statements
    )
