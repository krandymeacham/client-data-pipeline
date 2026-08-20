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
    fields = [StructField(c, StringType(), True) for c in columns]
    fields.append(StructField(CORRUPT_COL, StringType(), True))
    return StructType(fields)


def read_raw(spark, client_id, entity_name, entity_cfg, input_dir):
    """Read one client/entity file into a bronze DataFrame with lineage columns."""
    schema = _raw_schema(entity_cfg["columns"])
    file_path = f"{input_dir}/{client_id}/{entity_cfg['file']}"

    df = (
        spark.read.format("csv")
        .schema(schema)
        .option("delimiter", entity_cfg["delimiter"])
        .option("header", str(entity_cfg.get("header", True)).lower())
        .option("quote", entity_cfg.get("quote", '"'))
        .option("escape", '"')
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .load(file_path)
    )

    return (
        df.withColumn("_client_id", F.lit(client_id))
        .withColumn("_entity", F.lit(entity_name))
        .withColumn("_source_file", F.lit(entity_cfg["file"]))
        .withColumn("_ingestion_ts", F.current_timestamp())
    )


def ingest_client_entity(
    spark, client_id, entity_name, entity_cfg, input_dir, raw_dir, output_format="delta"
):
    """Read one client/entity file and write it to the bronze layer. Returns
    (dataframe, output_path, row_count) for the caller to log/inspect."""
    df = read_raw(spark, client_id, entity_name, entity_cfg, input_dir)
    out_path = f"{raw_dir}/{client_id}/{entity_name}"
    write_table(df, out_path, output_format=output_format)
    return df, out_path, df.count()


def run_ingest(spark, client_configs, input_dir, raw_dir, output_format="delta"):
    """Ingest every entity for every client. client_configs is
    {client_id: full_yaml_config}. Returns a list of result dicts for
    reporting/logging."""
    results = []
    for client_id, cfg in client_configs.items():
        for entity_name in ("customers", "transactions"):
            if entity_name not in cfg:
                continue
            _, out_path, row_count = ingest_client_entity(
                spark, client_id, entity_name, cfg[entity_name], input_dir, raw_dir, output_format
            )
            results.append(
                {
                    "client_id": client_id,
                    "entity": entity_name,
                    "path": out_path,
                    "row_count": row_count,
                }
            )
    return results
