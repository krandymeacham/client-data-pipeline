"""Bronze / raw layer: land every client file with as little opinion as
possible.

Every column is read as a string (no type guessing yet -- that is silver's
job), the header row is skipped, and Spark's PERMISSIVE CSV mode captures
anything that doesn't match the declared column count -- ragged rows,
garbage lines, unterminated quotes -- into a single `_corrupt_record`
column instead of raising or silently dropping data. That one mechanism is
what lets a single reader handle every malformed-row pattern in the sample
data (mismatched delimiters, too-few-column rows, blank rows, dangling
quotes) without any client-specific special-casing.

Lineage metadata (client, source file, ingestion timestamp) is attached so
downstream layers -- and anyone auditing the data -- can always trace a row
back to exactly where it came from.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.common import CORRUPT_COL, write_table


def _raw_schema(columns):
    """Build a schema where every column is a nullable string.
    No type inference yet -- silver will handle parsing and validation.
    Always includes the _corrupt_record column for PERMISSIVE mode."""
    # Create string fields for all declared columns
    fields = [StructField(c, StringType(), True) for c in columns]
    # Add the corrupt record column to capture malformed rows
    fields.append(StructField(CORRUPT_COL, StringType(), True))
    return StructType(fields)


def read_raw(spark, client_id, entity_name, entity_cfg, input_dir):
    """Read one client/entity file into a bronze DataFrame with lineage columns."""
    # Build schema with all columns as strings plus the corrupt record column
    schema = _raw_schema(entity_cfg["columns"])
    file_path = f"{input_dir}/{client_id}/{entity_cfg['file']}"

    # Read CSV with PERMISSIVE mode: malformed rows go to _corrupt_record
    # instead of being dropped or causing failures
    df = (
        spark.read.format("csv")
        .schema(schema)  # Enforce string-only schema
        .option("delimiter", entity_cfg["delimiter"])  # Client-specific delimiter
        .option("header", str(entity_cfg.get("header", True)).lower())  # Skip header row
        .option("quote", entity_cfg.get("quote", '"'))  # Quote character for escaped fields
        .option("escape", '"')  # Escape character for quotes within quotes
        .option("multiLine", "false")  # Single-line records only
        .option("mode", "PERMISSIVE")  # Capture corrupt rows instead of failing
        .option("columnNameOfCorruptRecord", CORRUPT_COL)  # Where to put bad rows
        .load(file_path)
    )

    # Attach lineage metadata so every row can be traced back to its source
    return (
        df.withColumn("_client_id", F.lit(client_id))  # Which client
        .withColumn("_entity", F.lit(entity_name))  # Which entity (customers/transactions)
        .withColumn("_source_file", F.lit(entity_cfg["file"]))  # Original filename
        .withColumn("_ingestion_ts", F.current_timestamp())  # When we ingested it
    )


def ingest_client_entity(spark, client_id, entity_name, entity_cfg, input_dir, table_name):
    """Read one client/entity file and write it to its bronze table (one
    table per client per entity -- customers and transactions have
    genuinely different raw schemas, so merging them into a single table
    per client would mean a sparse, harder-to-query table for no benefit).
    Returns (dataframe, table_name, row_count) for the caller to log/inspect."""
    # Step 1: Read the raw CSV file with lineage metadata
    df = read_raw(spark, client_id, entity_name, entity_cfg, input_dir)
    # Step 2: Write to bronze table (CREATE OR REPLACE)
    write_table(df, table_name)
    # Step 3: Return result tuple for reporting
    return df, table_name, df.count()


def run_ingest(spark, client_configs, input_dir, bronze_schema):
    """Ingest every entity for every client into `{bronze_schema}.
    {client_id}_{entity_name}`. client_configs is {client_id:
    full_yaml_config}. Returns a list of result dicts for reporting/logging."""
    results = []
    # Loop through each client's configuration
    for client_id, cfg in client_configs.items():
        # Process each entity type (customers and transactions)
        for entity_name in ("customers", "transactions"):
            # Skip entities that this client doesn't provide
            if entity_name not in cfg:
                continue
            # Build bronze table name: bronze.{client_id}_{entity_name}
            table_name = f"{bronze_schema}.{client_id}_{entity_name}"
            # Ingest the file and get the row count
            _, _, row_count = ingest_client_entity(
                spark, client_id, entity_name, cfg[entity_name], input_dir, table_name
            )
            # Record result for this client/entity combination
            results.append(
                {
                    "client_id": client_id,
                    "entity": entity_name,
                    "table": table_name,
                    "row_count": row_count,
                }
            )
    return results
