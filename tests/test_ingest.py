"""Unit tests for pipeline.ingest: the bronze reader's schema, delimiter/
quote handling, and malformed-row detection. These write tiny real files to
a pytest tmp_path (ingest reads from disk by design), unlike refine/curate's
pure in-memory tests.
"""
from pipeline.ingest import read_raw

CUSTOMERS_CFG = {
    "file": "customers.txt",
    "delimiter": ",",
    "header": True,
    "quote": '"',
    "columns": ["id", "name", "email"],
}


def _write(tmp_path, client_id, filename, content):
    client_dir = tmp_path / client_id
    client_dir.mkdir(parents=True, exist_ok=True)
    (client_dir / filename).write_text(content, encoding="utf-8")
    return str(tmp_path)


def test_read_raw_parses_well_formed_rows(tmp_path, spark):
    content = "id,name,email\n1,Jane Doe,jane@example.com\n2,John Smith,john@example.com\n"
    input_dir = _write(tmp_path, "client_a", "customers.txt", content)

    df = read_raw(spark, "client_a", "customers", CUSTOMERS_CFG, input_dir)

    assert set(df.columns) >= {"id", "name", "email", "_corrupt_record", "_client_id", "_source_file"}
    rows = {r.id: r for r in df.collect()}
    assert rows["1"].name == "Jane Doe"
    assert rows["1"]._corrupt_record is None
    assert rows["1"]._client_id == "client_a"


def test_read_raw_flags_ragged_row_as_corrupt(tmp_path, spark):
    # A row with the wrong number of fields for the declared schema -- the
    # same shape as the sample data's injected garbage/malformed rows.
    content = "id,name,email\n1,Jane Doe,jane@example.com\nBAD_ROW_TOO_FEW_COLS\n"
    input_dir = _write(tmp_path, "client_a", "customers.txt", content)

    df = read_raw(spark, "client_a", "customers", CUSTOMERS_CFG, input_dir)

    corrupt_rows = df.filter(df["_corrupt_record"].isNotNull()).collect()
    assert len(corrupt_rows) == 1
    assert corrupt_rows[0]._corrupt_record == "BAD_ROW_TOO_FEW_COLS"


def test_read_raw_quoted_field_with_embedded_delimiter(tmp_path, spark):
    # Mirrors client_b's full_name field, which is sometimes quoted and
    # contains a literal '|' -- the pipe delimiter must not split it.
    cfg = {**CUSTOMERS_CFG, "delimiter": "|", "columns": ["id", "name", "email"]}
    content = 'id|name|email\n1|"Raj|Johnson"|raj@example.com\n'
    input_dir = _write(tmp_path, "client_b", "customers.txt", content)

    df = read_raw(spark, "client_b", "customers", cfg, input_dir)

    row = df.collect()[0]
    assert row.name == "Raj|Johnson"
    assert row._corrupt_record is None
