"""Unit tests for pipeline.curate: unioning per-client silver tables into
one canonical schema, and deduping on the surrogate key.
"""
from datetime import datetime

from pipeline.curate import build_gold_customers, build_gold_transactions


def test_gold_customers_unions_and_fills_missing_columns(spark):
    # client_a reports loyalty_points but not country; client_b reports
    # country but not loyalty_points -- both should end up as real, null-
    # filled columns in the unioned result rather than being dropped.
    client_a_df = spark.createDataFrame(
        [("client_a", "1", "Jane", "Doe", 100, datetime(2024, 1, 1))],
        ["client_id", "source_customer_id", "first_name", "last_name", "loyalty_points", "_silver_refined_ts"],
    )
    client_b_df = spark.createDataFrame(
        [("client_b", "1", "John", "Smith", "US", datetime(2024, 1, 1))],
        ["client_id", "source_customer_id", "first_name", "last_name", "country", "_silver_refined_ts"],
    )
    gold = build_gold_customers({"client_a": client_a_df, "client_b": client_b_df})

    rows = {r.client_id: r for r in gold.collect()}
    assert rows["client_a"].loyalty_points == 100
    assert rows["client_a"].country is None
    assert rows["client_b"].country == "US"
    assert rows["client_b"].loyalty_points is None
    assert rows["client_a"].customer_key == "client_a_1"


def test_gold_customers_dedupes_keeping_latest_refined_row(spark):
    df = spark.createDataFrame(
        [
            ("client_a", "1", "Old Name", datetime(2024, 1, 1)),
            ("client_a", "1", "New Name", datetime(2024, 6, 1)),
        ],
        ["client_id", "source_customer_id", "first_name", "_silver_refined_ts"],
    )
    gold = build_gold_customers({"client_a": df})
    rows = gold.collect()
    assert len(rows) == 1
    assert rows[0].first_name == "New Name"


def test_gold_transactions_customer_key_matches_customers_format(spark):
    txn_df = spark.createDataFrame(
        [("client_a", "T1", "C1", datetime(2024, 1, 1))],
        ["client_id", "source_transaction_id", "source_customer_id", "_silver_refined_ts"],
    )
    gold = build_gold_transactions({"client_a": txn_df})
    row = gold.collect()[0]
    assert row.customer_key == "client_a_C1"
    assert row.transaction_key == "client_a_T1"
