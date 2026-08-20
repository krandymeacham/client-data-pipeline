"""Unit tests for pipeline.common: config loading and path resolution.

Regression tests for a real bug hit running on Databricks serverless: a
notebook step that copied configs onto DBFS produced 0-byte files there
(serverless has no /dbfs FUSE mount, which that copy path relied on),
and yaml.safe_load() on an empty file returns None rather than raising --
which surfaced several calls later as a generic
"TypeError: argument of type 'NoneType' is not iterable" instead of
pointing at the actual problem.
"""
import pytest

from pipeline.common import load_client_config, resolve_paths


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
    # Mirrors the Databricks notebook: BASE_PATH (DBFS) for output,
    # source_path (the repo checkout) for configs/input_files.
    paths = resolve_paths("dbfs:/tmp/client_ingestion", source_path="/Workspace/Users/me/repo")
    assert paths["configs"] == "/Workspace/Users/me/repo/configs"
    assert paths["input"] == "/Workspace/Users/me/repo/input_files"
    assert paths["raw"] == "dbfs:/tmp/client_ingestion/output/raw"
    assert paths["curated"] == "dbfs:/tmp/client_ingestion/output/curated"
