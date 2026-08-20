"""Silver / refined layer: parse bronze strings into typed, canonically
named columns; normalize the handful of null-token spellings each client
uses ("", "NULL", "None", ...); and split good rows from rows that can't be
trusted, instead of silently dropping or coercing them.

Every parsing rule for a client/entity comes from that client's YAML config
(configs/client_*.yaml) -- column renames, date formats, decimal/int/boolean
casts. Only the one genuinely non-1:1 transform in the sample data (client_b's
combined full_name field) needs actual code, registered below in
SPECIAL_TRANSFORMS and referenced from config by name. That split keeps the
common 90% of client variance declarative while still leaving a clear,
narrow extension point for the rest.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from pipeline.common import CORRUPT_COL, NULL_TOKENS, read_table, write_table

AMOUNT_SQL_TYPE = "DECIMAL(12,2)"


def _normalize_nulls(col):
    trimmed = F.trim(col)
    return F.when(trimmed.isin(NULL_TOKENS), None).otherwise(trimmed)


def _clean_numeric_string(col):
    """Strip currency symbols and thousands separators before casting."""
    return F.regexp_replace(F.regexp_replace(F.trim(col), r"\$", ""), ",", "")


def _try_cast(col, sql_type):
    """Spark 4's default ANSI mode makes `.cast()` raise on malformed input
    instead of returning null (e.g. the deliberately-corrupt 'EXTRA' value
    in some decimal columns) -- try_cast is the ANSI-safe equivalent that
    still yields null. There's no `F.try_cast` helper in this PySpark
    version, so it's invoked via SQL expression; the column name comes only
    from our own trusted YAML configs, not external input."""
    return F.expr(f"try_cast(`{col}` AS {sql_type})")


def _split_full_name(df):
    """client_b reports one combined `full_name` field instead of separate
    first/last columns. Split on the first run of whitespace; a name with
    no whitespace (e.g. one preserved with an embedded '|' inside quotes)
    is kept whole in first_name with a null last_name rather than guessed
    at or dropped."""
    name = F.col("full_name")
    is_blank = name.isNull() | (F.trim(name) == "")
    parts = F.split(F.trim(name), r"\s+", 2)
    # F.get (not parts.getItem/[]) tolerates a single-word name -- an
    # out-of-range index otherwise raises under Spark's ANSI mode instead
    # of returning null.
    return df.withColumn(
        "first_name", F.when(is_blank, None).otherwise(F.get(parts, 0))
    ).withColumn("last_name", F.when(is_blank, None).otherwise(F.get(parts, 1)))


SPECIAL_TRANSFORMS = {"split_full_name": _split_full_name}


def refine_entity(bronze_df, entity_cfg):
    """Parse, type, and validate one client/entity's bronze rows.

    Returns (clean_df, quarantine_df). clean_df has canonical column names
    with real types. quarantine_df keeps every bronze column plus a
    `_quarantine_reason` so a human can see exactly why a row was set aside.
    """
    df = bronze_df.withColumn("client_id", F.col("_client_id"))

    for raw_col, target_col in entity_cfg.get("column_map", {}).items():
        df = df.withColumn(target_col, _normalize_nulls(F.col(raw_col)))

    transform_name = entity_cfg.get("special_transform")
    if transform_name:
        df = SPECIAL_TRANSFORMS[transform_name](df)

    for raw_col, spec in entity_cfg.get("date_fields", {}).items():
        # try_to_timestamp/try_to_date (rather than to_timestamp/to_date)
        # return null on an unparseable value under ANSI mode instead of
        # raising, matching how every other cast in this function degrades.
        parsed_ts = F.try_to_timestamp(F.col(raw_col), F.lit(spec["format"]))
        parsed = F.try_to_date(F.col(raw_col), spec["format"]) if spec.get("type") == "date" else parsed_ts
        df = df.withColumn(spec["target"], parsed)

    for raw_col, target_col in entity_cfg.get("int_fields", {}).items():
        # a non-numeric sentinel (e.g. client_c's "unknown") fails try_cast
        # and becomes null -- no separate null-token handling needed here.
        df = df.withColumn(target_col, _try_cast(raw_col, "INT"))

    for raw_col, target_col in entity_cfg.get("decimal_fields", {}).items():
        cleaned_col = f"__cleaned_{raw_col}"
        df = df.withColumn(cleaned_col, _clean_numeric_string(F.col(raw_col)))
        df = df.withColumn(target_col, _try_cast(cleaned_col, AMOUNT_SQL_TYPE)).drop(cleaned_col)

    for raw_col, spec in entity_cfg.get("boolean_fields", {}).items():
        val = F.trim(F.col(raw_col))
        df = df.withColumn(
            spec["target"],
            F.when(val.isin(spec["true_values"]), F.lit(True))
            .when(val.isin(spec["false_values"]), F.lit(False))
            .otherwise(F.lit(None).cast(BooleanType())),
        )

    if "default_currency" in entity_cfg:
        df = df.withColumn("currency", F.lit(entity_cfg["default_currency"]))

    df = df.withColumn("_silver_refined_ts", F.current_timestamp())

    required_fields = entity_cfg.get("required_fields", [])
    is_corrupt = F.col(CORRUPT_COL).isNotNull()
    missing_required = F.lit(False)
    for field in required_fields:
        missing_required = missing_required | F.col(field).isNull()

    missing_field_names = F.array_join(
        F.array(*[F.when(F.col(f).isNull(), F.lit(f)) for f in required_fields]), ","
    )
    reason = (
        F.when(is_corrupt, F.lit("unparseable_row"))
        .when(missing_required, F.concat(F.lit("missing_required_field:"), missing_field_names))
        .otherwise(F.lit(None))
    )
    df = df.withColumn("_quarantine_reason", reason)

    is_bad = is_corrupt | missing_required
    quarantine_df = df.filter(is_bad)
    clean_df = df.filter(~is_bad).drop("_quarantine_reason")
    return clean_df, quarantine_df


def run_refine(spark, client_configs, bronze_schema, silver_schema):
    """Refine every entity for every client into `{silver_schema}.
    {client_id}_{entity_name}` (clean) and `{silver_schema}.
    {client_id}_{entity_name}_quarantine`. Returns result dicts for
    reporting/logging."""
    results = []
    for client_id, cfg in client_configs.items():
        for entity_name in ("customers", "transactions"):
            if entity_name not in cfg:
                continue
            bronze_df = read_table(spark, f"{bronze_schema}.{client_id}_{entity_name}")
            clean_df, quarantine_df = refine_entity(bronze_df, cfg[entity_name])

            clean_table = f"{silver_schema}.{client_id}_{entity_name}"
            quarantine_table = f"{silver_schema}.{client_id}_{entity_name}_quarantine"
            write_table(clean_df, clean_table)
            write_table(quarantine_df, quarantine_table)

            results.append(
                {
                    "client_id": client_id,
                    "entity": entity_name,
                    "clean_count": clean_df.count(),
                    "quarantine_count": quarantine_df.count(),
                }
            )
    return results
