# Client Ingestion Pipeline

A config-driven Databricks/PySpark pipeline that ingests messy, inconsistent
flat files from three clients and standardizes them into a bronze / silver /
gold (raw / refined / curated) Delta Lake model.

## Architecture

```
input_files/{client}/{file}.txt
        |
        v
  BRONZE (output/raw/{client}/{entity})
    - read with the client's own delimiter/header/quote settings
    - every column read as a string; nothing parsed or dropped
    - Spark's PERMISSIVE CSV mode captures anything that doesn't match the
      expected column count into a single _corrupt_record column
    - lineage columns: _client_id, _entity, _source_file, _ingestion_ts
        |
        v
  SILVER (output/refined/{client}/{entity} + output/refined_quarantine/...)
    - config-driven column rename, type casts, null-token normalization
    - one non-declarative transform (splitting client_b's combined name
      field), registered by name from config
    - rows that were unparseable in bronze, or are missing a field this
      pipeline treats as mandatory, are routed to a quarantine table with
      a _quarantine_reason instead of being dropped or silently coerced
        |
        v
  GOLD (output/curated/{customers,transactions})
    - one canonical, typed schema per entity, unioned across all clients
    - a client missing a field just gets null in that column, typed
      correctly, rather than being dropped from the schema
    - deduped on a client-scoped surrogate key, keeping the most recently
      refined row
```

`pipeline/ingest.py`, `refine.py`, and `curate.py` map directly to bronze,
silver, and gold, and each exposes small, pure functions that operate on
DataFrames in memory (`read_raw`, `refine_entity`, `build_gold_customers`,
`build_gold_transactions`) plus a thin `run_*` wrapper that adds the actual
file I/O. That split is what makes `tests/` possible without spinning up
real Delta tables.

## Running in a fresh Databricks workspace

1. **Repos > Add Repo**, point it at this repository's Git URL.
2. Open `notebooks/00_run_pipeline.py` and attach it to any cluster. Delta
   Lake ships with the Databricks Runtime, so there's nothing extra to
   install.
3. **Run All.** The notebook copies `configs/` and `input_files/` from the
   repo checkout onto `BASE_PATH` (`/dbfs/tmp/client_ingestion` by default,
   overridable via the `base_path` widget), then calls `run.py`'s `main()`.
4. Re-running is safe — every layer writes with `mode("overwrite")`, and
   nothing depends on pre-existing state.

Preparing input data manually (instead of via the notebook's `dbutils.fs.cp`
step) is just as simple: upload the six files under `input_files/<client>/`
to `<BASE_PATH>/input_files/<client>/`, and the three YAML files under
`configs/` to `<BASE_PATH>/configs/`, preserving the directory structure.

### Running locally (no Databricks)

```bash
python -m venv venv && venv/Scripts/pip install -r requirements.txt
python run.py --base-path . --format delta
pytest tests/
```

`--format parquet` is available as a local fallback for environments that
can't resolve the `delta-spark` Maven coordinate (e.g. behind an SSL-
inspecting proxy) — see **Assumptions and limitations**.

## Key design decisions and tradeoffs

**Config-driven, not per-client code.** Every client's delimiter, header,
column renames, date formats, decimal/int/boolean casts, and required
fields live in `configs/client_*.yaml`. `pipeline/refine.py` has exactly one
function per parsing concern (rename, cast dates, cast decimals, cast
booleans), each looping over its slice of the config — there's no
`if client == "client_b"` anywhere. The one transform that genuinely isn't a
1:1 column rename (client_b's combined `full_name` field) is a named,
registered Python function referenced from config by string key, so the
declarative path stays honest about where it ends and a real extension
point begins, instead of pretending everything fits a rename table.

**Bronze stays untyped on purpose.** Every bronze column is a string, with
zero interpretation. That's what lets one generic PERMISSIVE-mode CSV
reader — configured only with each client's own delimiter/header/quote —
correctly quarantine every malformed-row pattern in the sample data
(garbage rows in the wrong delimiter, rows with too few fields, dangling
unterminated quotes, blank rows) without a single client-specific special
case. Typing and validation is silver's job, deliberately kept separate
from "did this line even parse."

**Quarantine, don't drop or silently coerce.** A row is quarantined if
Spark couldn't parse it (`_corrupt_record` is set) or if a field this
pipeline treats as mandatory (e.g. `amount` on a transaction) came back
null after parsing. Quarantined rows keep every original bronze column
plus a `_quarantine_reason`, so a human can see exactly what happened —
`missing_required_field:amount`, `unparseable_row` — instead of guessing
why row counts don't add up.

**Canonical gold schema = union of meaningful attributes, not lowest common
denominator.** Only `client_id`, an id, and a signup/created date are
truly common to all three clients' customer files; email, phone, country,
loyalty points, VIP status, and marketing consent are each reported by
only one or two clients. Rather than drop those fields or force dissimilar
concepts together (client_b's VIP tier, client_c's marketing opt-in, and
client_a's account status all *look* like a generic "status" column if you
squint, but conflating them would silently corrupt meaning), every
attribute gets a real, typed column in gold and is simply null for clients
that don't report it. `pipeline/curate.py`'s `GOLD_CUSTOMER_SCHEMA` /
`GOLD_TRANSACTION_SCHEMA` dicts *are* the answer to "what does a customer
look like?" for this pipeline.

**Composite surrogate keys, not raw source IDs.** `customer_key` is
`{client_id}_{source_customer_id}`, not the bare source ID, because
different clients' IDs are drawn from unrelated, colliding numeric ranges
(client_a customer `1` and client_c customer `1` are different people).
`transaction_key` and each transaction's `customer_key` FK are built the
same way, so joins across the curated tables are correct by construction.

**Delta over Parquet, with a documented local fallback.** Delta is
preferred by the assignment and is native on Databricks (no install
step). Locally, this environment's network policy blocked resolving the
`delta-spark` Maven coordinate over SSL, so local dev/testing supports
`--format parquet` as a fallback; the pipeline code itself is unaware of
which format it's writing (`pipeline/common.py:write_table`/`read_table`
take `output_format` as a parameter).

## How this handles change

- **A new client** is a new `configs/client_x.yaml` plus its files under
  `input_files/client_x/` — no changes to `pipeline/*.py`. If its
  attributes are genuinely new (not just named differently), add them to
  `GOLD_CUSTOMER_SCHEMA`/`GOLD_TRANSACTION_SCHEMA` in `curate.py` and
  they'll be null for every existing client automatically.
- **A new file format** (say, JSON instead of delimited text) needs a
  small branch in `pipeline/ingest.py`'s reader on a `format:` key in
  config; everything downstream of bronze is format-agnostic since it only
  ever sees typed/renamed columns.
- **A schema change within a file** (a column disappears, a new one shows
  up, values start arriving in a new date format) is exactly what
  `_corrupt_record` and the required-field quarantine check are built to
  surface without crashing the pipeline: rows that no longer fit the
  declared column count get quarantined and reported, rather than silently
  corrupting downstream data or halting the whole run. A genuinely new
  column requires updating that client's YAML, same as onboarding a new
  attribute for an existing client.

## Assumptions and limitations

- **client_c's transactions have no currency column.** `price_usd`'s
  naming implies USD; `default_currency: USD` in its config makes that
  assumption explicit (and easy to find and challenge) rather than leaving
  `currency` silently null for that client.
- **Referential integrity isn't enforced, only observable.** A transaction
  whose customer was quarantined (or never existed) still reaches gold; it
  simply won't join to a row in the curated customers table. Enforcing
  this would mean either quarantining transactions based on a *different*
  table's outcome (a cross-entity dependency this pipeline deliberately
  avoids) or delaying gold until every entity is refined. Flagging it as
  an observable data-quality fact seemed more honest for a bronze/silver/
  gold pipeline at this scope than silently dropping or refusing to load.
- **Names with no whitespace to split on** (client_b's combined
  `full_name` field, including the case where a name arrives with a
  literal `|` preserved inside quotes) go entirely into `first_name` with
  a null `last_name`, rather than being guessed at or quarantined.
- **Spark's default ANSI SQL mode** makes `.cast()` and out-of-range array
  indexing raise on malformed input instead of returning null — a real
  difference from older Spark behavior that this pipeline relies on to
  degrade gracefully. Every numeric/date cast uses `try_cast`/
  `try_to_timestamp`/`try_to_date`, and the name-splitter uses `F.get`
  instead of plain array indexing, specifically to preserve
  quarantine-not-crash behavior under ANSI mode.
- **Local Windows dev without a Hadoop `winutils.exe` on PATH** can't
  exercise Spark's local file *writer* (a well-known Spark-on-Windows
  limitation, unrelated to this pipeline's code) — unaffected by
  Databricks, Linux, or macOS. `tests/` avoid this entirely by asserting
  against in-memory DataFrames rather than round-tripping through disk.
