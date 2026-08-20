"""Unit tests for pipeline.refine: pure DataFrame-in/DataFrame-out logic,
so these run against tiny in-memory frames with no file I/O involved.

Several of these are regression tests for real bugs found while validating
against the actual sample data: Spark's default ANSI SQL mode makes
`.cast()` and out-of-range array indexing raise on bad input instead of
returning null, which would otherwise crash the whole pipeline on exactly
the malformed rows it's supposed to quarantine gracefully.
"""
from decimal import Decimal

from pyspark.sql.types import StringType, StructField, StructType

from pipeline.refine import refine_entity

TXN_CONFIG = {
    "column_map": {
        "transaction_id": "source_transaction_id",
        "customer_id": "source_customer_id",
        "currency": "currency",
    },
    "decimal_fields": {"amount": "amount"},
    "date_fields": {
        "created_ts": {"target": "transaction_ts", "format": "yyyy-MM-dd'T'HH:mm:ss", "type": "timestamp"}
    },
    "boolean_fields": {
        "is_refund": {"target": "is_refund", "true_values": ["true", "1"], "false_values": ["false", "0"]}
    },
    "required_fields": ["source_transaction_id", "source_customer_id", "amount"],
}
TXN_COLUMNS = [
    "transaction_id", "customer_id", "amount", "currency", "created_ts", "is_refund",
    "_corrupt_record", "_client_id",
]

CUSTOMER_WITH_NAME_SPLIT_CONFIG = {
    "column_map": {"cust_id": "source_customer_id"},
    "special_transform": "split_full_name",
    "required_fields": ["source_customer_id"],
}
CUSTOMER_COLUMNS = ["cust_id", "full_name", "_corrupt_record", "_client_id"]


def _make_df(spark, columns, rows):
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    return spark.createDataFrame(rows, schema)


def _txn_row(transaction_id="T1", customer_id="C1", amount="100.00", currency="USD",
             created_ts="2024-01-01T00:00:00", is_refund="true", corrupt=None, client_id="client_a"):
    return (transaction_id, customer_id, amount, currency, created_ts, is_refund, corrupt, client_id)


def test_null_token_normalization(spark):
    df = _make_df(spark, TXN_COLUMNS, [_txn_row(currency="NULL"), _txn_row(transaction_id="T2", currency="None")])
    clean, _ = refine_entity(df, TXN_CONFIG)
    currencies = {r.currency for r in clean.collect()}
    assert currencies == {None}


def test_malformed_decimal_becomes_null_not_a_crash(spark):
    # "EXTRA" mirrors the deliberately corrupt values in the real sample
    # data; under Spark's default ANSI mode a plain .cast() raises here.
    df = _make_df(spark, TXN_COLUMNS, [_txn_row(amount="EXTRA")])
    clean, quarantine = refine_entity(df, TXN_CONFIG)
    assert clean.count() == 0
    row = quarantine.collect()[0]
    assert row.amount is None
    assert row._quarantine_reason == "missing_required_field:amount"


def test_currency_symbol_and_thousands_separator_stripped(spark):
    df = _make_df(spark, TXN_COLUMNS, [_txn_row(amount="$1,234.50")])
    clean, _ = refine_entity(df, TXN_CONFIG)
    assert clean.collect()[0].amount == Decimal("1234.50")


def test_malformed_date_becomes_null_not_required(spark):
    df = _make_df(spark, TXN_COLUMNS, [_txn_row(created_ts="NOT_A_DATE")])
    clean, quarantine = refine_entity(df, TXN_CONFIG)
    assert quarantine.count() == 0
    assert clean.collect()[0].transaction_ts is None


def test_boolean_normalization(spark):
    df = _make_df(
        spark, TXN_COLUMNS,
        [_txn_row(transaction_id="T1", is_refund="true"),
         _txn_row(transaction_id="T2", is_refund="0"),
         _txn_row(transaction_id="T3", is_refund="")],
    )
    clean, _ = refine_entity(df, TXN_CONFIG)
    by_id = {r.source_transaction_id: r.is_refund for r in clean.collect()}
    assert by_id == {"T1": True, "T2": False, "T3": None}


def test_corrupt_record_is_quarantined_regardless_of_other_fields(spark):
    df = _make_df(spark, TXN_COLUMNS, [_txn_row(corrupt="garbage,raw,line")])
    clean, quarantine = refine_entity(df, TXN_CONFIG)
    assert clean.count() == 0
    assert quarantine.collect()[0]._quarantine_reason == "unparseable_row"


def test_missing_required_field_is_quarantined_with_field_name(spark):
    df = _make_df(spark, TXN_COLUMNS, [_txn_row(customer_id=None)])
    _, quarantine = refine_entity(df, TXN_CONFIG)
    assert quarantine.collect()[0]._quarantine_reason == "missing_required_field:source_customer_id"


def test_clean_row_passes_through_with_correct_types(spark):
    df = _make_df(spark, TXN_COLUMNS, [_txn_row()])
    clean, quarantine = refine_entity(df, TXN_CONFIG)
    assert quarantine.count() == 0
    row = clean.collect()[0]
    assert row.amount == Decimal("100.00")
    assert row.is_refund is True
    assert str(row.transaction_ts) == "2024-01-01 00:00:00"


def test_split_full_name_two_words(spark):
    df = _make_df(spark, CUSTOMER_COLUMNS, [("1", "Jane Doe", None, "client_b")])
    clean, _ = refine_entity(df, CUSTOMER_WITH_NAME_SPLIT_CONFIG)
    row = clean.collect()[0]
    assert (row.first_name, row.last_name) == ("Jane", "Doe")


def test_split_full_name_single_word_does_not_crash(spark):
    # Mirrors a real row where a name arrives with no whitespace to split
    # on (e.g. one preserved with an embedded '|' inside quotes) -- an
    # out-of-range array index otherwise raises under ANSI mode.
    df = _make_df(spark, CUSTOMER_COLUMNS, [("1", "Cher", None, "client_b")])
    clean, _ = refine_entity(df, CUSTOMER_WITH_NAME_SPLIT_CONFIG)
    row = clean.collect()[0]
    assert (row.first_name, row.last_name) == ("Cher", None)


def test_split_full_name_blank(spark):
    df = _make_df(spark, CUSTOMER_COLUMNS, [("1", "", None, "client_b")])
    clean, _ = refine_entity(df, CUSTOMER_WITH_NAME_SPLIT_CONFIG)
    row = clean.collect()[0]
    assert (row.first_name, row.last_name) == (None, None)
