# Architecture

This repository implements a **local, file-based layered data warehouse**: source data is
scraped/ingested, then pushed through a fixed sequence of layers, each one adding structure,
typing, history, and quality guarantees. This document describes the generic engine
(`polars_src/`, `schema/`, `processing/`, `quality_checks/`) that any pipeline in this repo is
built on. It intentionally does not describe any specific pipeline's tables or business logic —
use the existing basketball pipeline (`schema/schema_*.py`, `processing/processing_*.py`)
as a worked reference when building a new one.

The `spark/` directory is a deprecated, structurally identical PySpark implementation of the
same engine. Do not extend it — new pipelines should be built on `polars_src/`.

## Layers

Every table belongs to exactly one layer. Layers are stacked strictly bottom-to-top; a table may
only read from its own layer or layers below it.

| Layer | Container name | Purpose |
|---|---|---|
| landingzone | `landingzone` | Minimally-typed ingested source data (CSV/JSON/Parquet as scraped/exported). One "container" per source file/API. |
| raw | `raw` | Typed, deduplicated, one row per source entity. First layer with `RecordID`/`from_date`/`to_date` (history tracking begins here). |
| datamodel | `datamodel` | Cleaned, renamed, conformed entities (dimensions/facts) built by joining/transforming raw tables. |
| enriched | `enriched` | Derived analytics tables built from datamodel (and other enriched) tables. |
| monitoring | `monitoring` | Cross-cutting: `monitoring_values` (signalling check results) and `record_tracing` (lineage graph). Written to automatically, not part of the layer stack a pipeline author designs against. |

On disk, each table lives at `data/<environment>/<container>/<location>[/<partition_column>=<value>]`,
written as Parquet (except landingzone sources, which may be CSV/JSON/Parquet as originally
ingested). `<environment>` is one of `develop` / `testing` / `production`, chosen when
constructing a `CustomDF`. Always build these paths with `pathlib.Path` (or `os.path.join`)
rather than hardcoded string literals — never write a literal `\` or `/` separator, so pipeline
code behaves the same on Windows and POSIX.

## Table definitions (`schema/`)

Every table is declared once, as an entry in a per-layer dictionary, in the matching
`schema/schema_<layer>.py` module (`schema_landingzone.py`, `schema_raw.py`,
`schema_datamodel.py`, `schema_enriched.py`, `schema_monitoring.py`). Each module exposes a
`get_<layer>_schema(table_name)` function; `polars_src/database.py`'s `get_table_definition(schema_name, table_name)`
is the single dispatcher all code goes through to resolve a table name to its definition —
it infers `schema_name` from the table name's suffix (e.g. `..._raw` → `raw_layer`).

A table definition is a dict with these keys:

- `columns` — `dict[str, pl.DataType]`, the full column set **including** `RecordID`, `from_date`,
  `to_date` for any layer above landingzone. Column order here is the canonical order.
- `container` — the layer's container name (must match one of the five above).
- `location` — subpath under the container; for landingzone this is often a source file/directory
  path, for other layers it's just the table's own name.
- `file_format` — `"csv"`, `"json"`, or `"parquet"`.
- `partition_column` — column name to partition the parquet output by, or `""` for no partitioning.
  Use partitioning when a table is naturally scoped/written per key (e.g. per source table, per
  season) so partitions can be regenerated independently.
- `quality_checks` — list of **blocking** check dicts (see Quality checks below). Signalling
  (non-blocking) checks live in a separate registry, not here.
- `json_data_path` (landingzone only, JSON sources) — a JSONPath expression (via `jsonpath_ng`)
  selecting the records to extract from each source JSON file.

Naming convention: table names always end with `_<container>`, e.g. `mytable_raw`,
`mytable_datamodel`, `mytable_enriched`. This suffix is what `get_table_definition` and
`CustomDF.__init__` use to resolve the layer automatically — never omit it.

Every schema module also defines a `some_table_<layer>` placeholder entry showing the exact shape
expected; copy that shape when adding a table to a layer.

## `CustomDF` — the core abstraction

`polars_src/custom_dataframes.py`'s `CustomDF` (subclass of `DataReader`, see below) is the object
all pipeline code is written against. Construct it as:

```python
CustomDF(table_name, initial_df=None, partition_name="", history="recent", environment="develop")
```

- If `initial_df` is omitted, the table is read from disk via `DataReader.read_source()`.
- If `initial_df` is provided, it becomes the table's in-memory data (used when a processing
  function computes a new/transformed table and needs to wrap it before writing or joining).
- `history`: `"recent"` returns only currently-active rows (`to_date == 2099-12-31`); `"complete"`
  returns full history.
- `partition_name` selects a single partition when the table's schema declares a
  `partition_column` (or `"all"` to read every partition).

### Lineage: the `map_` column

Every table above landingzone carries exactly one hidden `map_<table>_<layer>` struct column
(renamed per-instance with a random salt suffix to avoid collisions across joins) — this is how
record-level lineage is tracked without needing a separate lineage-computation pass. It is created
automatically by `DataReader.read_source()` from each row's `RecordID` and is invisible to normal
column lists. `CustomDF` exposes it via `self.map_col`. All the `custom_*` transformation methods
below know how to merge/carry this column correctly; **never manipulate it directly**, and never
`select`/`drop` it explicitly — use the `custom_*` methods, which handle it for you.

### Transformation methods

Use these instead of raw Polars DataFrame methods whenever the DataFrame lives on a `CustomDF`,
because they preserve/merge the lineage `map_` column automatically:

- `custom_select(columns)` — like `.select()`, but also keeps the map column (unless the table is
  landingzone, which has no map column yet).
- `custom_join(other, custom_on=..., custom_left_on=..., custom_right_on=..., custom_how=..., custom_suffix="_right")`
  — join two `CustomDF`s; merges both sides' lineage into one map column.
- `custom_join_asof(other, custom_on=..., custom_left_on_asof=..., custom_right_on_asof=..., custom_by=..., custom_strategy="backward", ...)`
  — as-of join with the same lineage-merging behavior; both inputs must be pre-sorted by the asof key.
- `custom_groupby(groupby_columns, *aggregation_exprs)` — group + aggregate (pass `pl.sum(...)`,
  `pl.mean(...)`, etc. as you would to `.agg()`); lineage columns for the group are unioned per group.
- `custom_union(other)` — vertical concat of two same-shaped `CustomDF`s with lineage merged.
- `custom_distinct()` — deduplicate rows while unioning their lineage.
- `custom_drop(columns)` — drop columns (map column untouched).
- `rename_columns(rename_dict)` — rename columns; raises `ValueError` if a source name isn't
  present in the current schema.
- `convert_data_types(column_list, data_type)` — cast a list of columns to a given Polars dtype,
  in place.

Each `custom_*` method returns a **new** `CustomDF` instance (except `rename_columns`/
`convert_data_types`, which mutate in place) — chain them functionally:

```python
players = CustomDF("players_datamodel")
teams = CustomDF("teams_datamodel")
result = (
    players
    .custom_select(["player_id", "team_id", "name"])
    .custom_join(teams, custom_on="team_id", custom_how="left")
    .custom_groupby(["team_id"], pl.count().alias("player_count"))
)
```

### Writing a table: `write_table()`

Calling `write_table()` on a `CustomDF` runs the full pipeline for landing a table:

1. `compare_tables()` — reads the existing on-disk version of the table (`history="complete"`)
   and applies SCD Type 2 merge logic (`apply_scd_type_2`, see below) against the new data.
2. `add_record_id()` — computes a stable `RecordID` (SHA hash of all non-lineage, non-`to_date`
   columns) for every row, positioning it (and the partition column, if any) as the last column(s).
3. `dump_map_column()` — if a `map_` column is present (i.e. layer is above landingzone), lineage
   edges for newly-created rows (`to_date == 2099-12-31`) are exploded out and appended to the
   monitoring `record_tracing` table, keyed by target table/RecordID and source table/RecordID.
   The map column is then dropped from the DataFrame before writing.
4. `validate_table_format()` — checks the DataFrame matches the declared schema shape, dedupes
   accidental duplicate `RecordID`s, and runs **blocking** quality checks (raises `ValueError`
   on violation, halting the pipeline).
5. Writes to Parquet at the table's path, partitioned by `partition_column` if declared.
6. Runs `check_signalling_issues()` (non-blocking quality checks recorded to `monitoring_values`) —
   skipped when writing `monitoring_values` itself, to avoid infinite recursion.

Any pipeline that produces a table should end with a call to `write_table()` on the final
`CustomDF`; everything else (SCD2, hashing, lineage, validation, monitoring) is handled for you.

## History: SCD Type 2 (`apply_scd_type_2`)

Every table above landingzone is versioned with `from_date`/`to_date` columns instead of being
overwritten. `polars_src/dataframe_helpers.py::apply_scd_type_2(new_table, existing_table)`
compares incoming rows against the currently-active existing rows (`to_date == 2099-12-31`) via a
content hash (all columns except `RecordID`/`from_date`/`to_date`/lineage):

- **Unchanged rows** (hash matches) keep their original `from_date`/`to_date`.
- **New rows** (hash only in incoming) get `from_date = today`, `to_date = 2099-12-31`.
- **Removed rows** (hash only in existing) are closed: `to_date` set to today.
- Rows already closed in the existing table are passed through unchanged.
- If the pipeline is re-run the same day, same-day rows in the existing table are treated as
  replaceable rather than versioned again, to avoid duplicate same-day versions.

`history="recent"` reads (the default) filter to `to_date == 2099-12-31`; `history="complete"`
reads return every version. This is what lets `get_record_history` and auditors reconstruct a
table's state as of any point in time.

## Lineage / record tracing

Two mechanisms combine to give full row-level lineage across every table in the warehouse:

- The `map_` struct column (see above) tracks which upstream `RecordID`s contributed to each row,
  carried through every join/groupby/union/distinct in memory.
- On write, `dump_map_column()` flattens that struct into rows of the `record_tracing` monitoring
  table: `(source_RecordID, source_table_name, target_RecordID, target_table_name)`, partitioned
  by `target_table_name`.

`polars_src/data_query_functions.py::get_record_history(table_name, record_id)` walks this table
recursively (with per-table caching) to print the full ancestry of any record, layer by layer,
back to its landingzone/raw origin. `connectors/export_edges.py` is a standalone script that dumps
the whole `record_tracing` table into a `(id, table_id, upstream, attributes)` edge-list JSON,
suitable for feeding into external graph-visualization tooling.

## Quality checks (`polars_src/data_quality_functions.py`, `quality_checks/`)

There are two independent kinds of checks, both defined as dicts of the shape
`{"check": <check name>, "columns": [...], ...check-specific kwargs}`:

**Blocking checks** — declared inline on a table's schema entry (`quality_checks` key). Run during
`validate_table_format()`/`write_table()`, before every write. Any violation prints the offending
rows and raises `ValueError`, halting the pipeline. Available check types (see
`calculate_blocking_issues`): `"values in range"`, `"values are unique"`, `"values have format"`.

**Signalling checks** — non-blocking, recorded as monitoring facts rather than raised. Registered
per table in `quality_checks/signalling_rules_datamodel.py`'s dict (looked up by
`quality_checks/signalling_rules.py::get_signalling_rules(table_name)`, which currently only
applies signalling checks to `*_datamodel` tables — extend that dispatcher if you want signalling
checks on other layers). Run automatically at the end of `write_table()` via
`check_signalling_issues()`, which always also computes a baseline "values are filled" check
(`calculate_filled_values`) for every non-lineage column. Results land in the `monitoring_values`
table (`signalling_id`, `check_id`, `column_name`, `check_name`, `total_count`, `valid_count`),
partitioned by table name, with `signalling_id`s assigned stably across runs
(`assign_signalling_id`) so the same check on the same column keeps the same id over time.
Available check types (see `calculate_signalling_issues`): `"values within list"`,
`"values in range"`, `"values are unique"`, `"values have format"`, `"values occur as expected"`,
`"values sum to 1"`, `"distinct values occur as expected"`, `"record has expected history"`
(cross-checks a record's `record_tracing` ancestry against an expected shape).

Add a new check *type* by adding a `check_*`/`get_violations_*` function pair in
`data_quality_functions.py` and wiring it into the `processing_dict`/`violation_dict` (blocking)
or the `if/elif` chain in `calculate_signalling_issues` (signalling).

## Reading data (`polars_src/data_readers.py`)

`DataReader` (the base class of `CustomDF`) resolves a table's on-disk path from its schema
(`data/<env>/<container>/<location>`) and reads it according to `file_format`:

- `csv` — `pl.read_csv` with the declared schema.
- `parquet` — `pl.read_parquet`; if `partition_column` is set, all partitions are read and
  optionally filtered to one partition (`partition_name`), or `"all"` to keep every partition.
- `json` — either a single `.json` file read as newline-delimited JSON, or (typical for landingzone
  sources) every `*.json` file in a directory, each parsed and filtered via the schema's
  `json_data_path` JSONPath expression, with a `file_name` column attached per record so the source
  file is always traceable.

If the resolved path doesn't exist yet, an empty DataFrame matching the schema is returned instead
of erroring — this lets a brand-new table's first write proceed through the same SCD2/write path
as every subsequent one. After reading, `history="recent"` filtering, landingzone `"NA"`/`"nan"` →
null normalization, and column-name cleaning (`clean_column_names`: strips/replaces characters
that are unsafe in Polars column names, e.g. spaces, `()`, `-`, `/`) are applied, followed by
lineage map-column creation for any layer above landingzone/monitoring.

## Processing modules (`processing/`)

Each layer has one processing module (`processing_raw.py`, `processing_datamodel.py`,
`processing_enriched.py`) exposing a single dispatcher function
(`generate_table_<layer>(table_name: str) -> bool`) that `if/elif`-branches on `table_name` and
runs that table's specific ETL logic using `CustomDF`, ending in `.write_table()`. This is the
convention new pipelines should follow: one dispatcher per layer, one branch per table, each
branch self-contained and reading only from lower layers (or, for enriched, also from other
enriched tables it depends on). An orchestration script (see `basketball-scaper.py` for the
existing example) then calls these dispatchers in dependency order — lower layers, and within a
layer, tables with no intra-layer dependencies before tables that join against them.

## Connectors (`connectors/`)

Pipeline-specific ingestion/export utilities that sit outside the layer engine:
- Ingestion connectors fetch external data (APIs, scraping, files) and land it under
  `data/<env>/landingzone/...` in the shape the landingzone schema for that source expects.
- Export connectors (e.g. `export_edges.py`) read finished tables/monitoring data back out for
  external consumption (dashboards, graph tools, etc.).

New pipelines should add their own connector module(s) here rather than embedding ingestion logic
in `processing/`.

## Building a new pipeline: checklist

1. **Define schemas.** For each new table, add an entry to the appropriate
   `schema/schema_<layer>.py` dict, following the `some_table_<layer>` template: `columns`,
   `container`, `location`, `file_format`, `partition_column`, `quality_checks`
   (`json_data_path` too, for landingzone JSON sources). Name it `<table>_<layer>`.
2. **Add an ingestion connector** (if landing new source data) under `connectors/` that writes
   into the landingzone path/format your schema declares.
3. **Write the raw-layer branch** in `processing/processing_raw.py`'s `generate_table_raw`:
   read the landingzone `CustomDF`(s), type/clean/dedupe, wrap in a new `CustomDF("<table>_raw", initial_df=...)`,
   call `.write_table()`.
4. **Write the datamodel-layer branch(es)** similarly in `processing_datamodel.py`, joining/
   conforming raw tables into clean entities.
5. **Write the enriched-layer branch(es)** in `processing_enriched.py`, composing datamodel (and
   other enriched) tables via the `custom_*` methods into analytics tables. Keep dependency order
   in mind — a table can only reference enriched tables generated before it in the run.
6. **Register quality checks**: blocking checks inline in the schema entry; signalling checks in
   `quality_checks/signalling_rules_datamodel.py` (or extend `signalling_rules.py` if you need
   signalling checks on a layer other than datamodel).
7. **Add tests** under `tests/`, mirroring the style in `tests/test_custom_dataframes.py` /
   `tests/test_dataframe_helpers.py` (construct a `CustomDF` with `environment="testing"` against
   fixture data under `data/testing/...`, or unit-test helper functions directly against small
   Polars DataFrames).
8. **Wire the orchestration script** to call the new `generate_table_*` functions in the correct
   layer/dependency order.

## Testing conventions

`tests/` uses `pytest` with `environment="testing"` pointing at fixtures under
`data/testing/landingzone/` (see `data/testing/landingzone/product_dimension_table.csv` /
`transaction_fact_table.csv`, referenced by the `test_*_landingzone` placeholder schema entries).
Prefer testing pure helper functions (`dataframe_helpers.py`, `data_quality_functions.py`) directly
with small in-memory `pl.DataFrame`s and `polars.testing.assert_frame_equal`; test `CustomDF`
behavior end-to-end against the testing fixtures.

## Tooling

- Dependency/environment management: `uv` (`pyproject.toml` + `uv.lock`).
- Formatting/linting: `ruff` (default configuration, no project-specific overrides).
- Python version: pinned via `.python-version`.
