"""Gold / curated layer: one standardized table per entity, unioned across
every client into a single consistent schema.

 A field only client_b has (is_vip) or only
client_c has (age, marketing_opt_in) still gets a real column -- just null
for the clients that don't report it rather than being dropped or forced
into an unrelated field. Concepts that only look similar (client_b's VIP
tier, client_c's marketing consent, client_a's account status) are kept as
separate columns instead of collapsed into one, since merging them would
silently change their meaning.

Adding a fourth client only requires its columns to already be canonical
(silver's job); curate.py itself doesn't change.
"""
import functools

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.common import read_table, write_table

# {canonical column: Spark SQL type}. A type is required
# so a column absent from *every* client's silver table -- e.g. a brand
# new client that hasn't started reporting `age` yet -- can still be
# materialized as a properly typed null column instead of breaking the
# final select or producing an unwritable NullType column.
GOLD_CUSTOMER_SCHEMA = {
    "client_id": "STRING", "source_customer_id": "STRING", "first_name": "STRING",
    "last_name": "STRING", "email": "STRING", "phone": "STRING", "country": "STRING",
    "address": "STRING", "postal_code": "STRING", "signup_date": "DATE", "status": "STRING",
    "age": "INT", "loyalty_points": "INT", "is_vip": "BOOLEAN", "marketing_opt_in": "BOOLEAN",
    "_source_file": "STRING", "_silver_refined_ts": "TIMESTAMP",
}
GOLD_CUSTOMER_COLUMNS = list(GOLD_CUSTOMER_SCHEMA)

GOLD_TRANSACTION_SCHEMA = {
    "client_id": "STRING", "source_transaction_id": "STRING", "source_customer_id": "STRING",
    "transaction_ts": "TIMESTAMP", "amount": "DECIMAL(12,2)", "currency": "STRING",
    "channel": "STRING", "payment_type": "STRING", "quantity": "INT", "discount_code": "STRING",
    "is_refund": "BOOLEAN", "tax": "DECIMAL(12,2)", "notes": "STRING",
    "_source_file": "STRING", "_silver_refined_ts": "TIMESTAMP",
}
GOLD_TRANSACTION_COLUMNS = list(GOLD_TRANSACTION_SCHEMA)


def _select_known_columns(df, columns):
    return df.select(*[c for c in columns if c in df.columns])


def _ensure_schema(df, schema):
    """Add any canonical column missing from the unioned frame as a
    properly typed null column, and cast every column to its canonical
    type so clients that happen to agree on a name don't disagree on type."""
    for col, sql_type in schema.items():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(sql_type))
        else:
            df = df.withColumn(col, F.col(col).cast(sql_type))
    return df


def _dedupe(df, key_col, order_col="_silver_refined_ts"):
    """Keep the most recently refined row per key. Silver already routed
    unparseable/incomplete rows to quarantine, so any duplicate keys here
    are legitimate re-deliveries of the same source record."""
    window = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_gold_customers(silver_dfs):
    """silver_dfs: {client_id: refined customers DataFrame}."""
    projected = [_select_known_columns(df, GOLD_CUSTOMER_COLUMNS) for df in silver_dfs.values()]
    unioned = functools.reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), projected)
    unioned = _ensure_schema(unioned, GOLD_CUSTOMER_SCHEMA)
    unioned = unioned.withColumn(
        "customer_key", F.concat_ws("_", F.col("client_id"), F.col("source_customer_id"))
    )
    return _dedupe(unioned, "customer_key").select("customer_key", *GOLD_CUSTOMER_COLUMNS)


def build_gold_transactions(silver_dfs):
    """silver_dfs: {client_id: refined transactions DataFrame}."""
    projected = [_select_known_columns(df, GOLD_TRANSACTION_COLUMNS) for df in silver_dfs.values()]
    unioned = functools.reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), projected)
    unioned = _ensure_schema(unioned, GOLD_TRANSACTION_SCHEMA)
    unioned = (
        unioned.withColumn(
            "transaction_key", F.concat_ws("_", F.col("client_id"), F.col("source_transaction_id"))
        ).withColumn(
            "customer_key", F.concat_ws("_", F.col("client_id"), F.col("source_customer_id"))
        )
    )
    return _dedupe(unioned, "transaction_key").select(
        "transaction_key", "customer_key", *GOLD_TRANSACTION_COLUMNS
    )


def run_curate(spark, client_configs, silver_schema, gold_schema):
    """Union every client's silver tables into `{gold_schema}.customers`
    and `{gold_schema}.transactions`."""
    customer_dfs, transaction_dfs = {}, {}
    for client_id, cfg in client_configs.items():
        if "customers" in cfg:
            customer_dfs[client_id] = read_table(spark, f"{silver_schema}.{client_id}_customers")
        if "transactions" in cfg:
            transaction_dfs[client_id] = read_table(spark, f"{silver_schema}.{client_id}_transactions")

    results = {}
    if customer_dfs:
        gold_customers = build_gold_customers(customer_dfs)
        write_table(gold_customers, f"{gold_schema}.customers")
        results["customers"] = gold_customers.count()
    if transaction_dfs:
        gold_transactions = build_gold_transactions(transaction_dfs)
        write_table(gold_transactions, f"{gold_schema}.transactions")
        results["transactions"] = gold_transactions.count()
    return results
