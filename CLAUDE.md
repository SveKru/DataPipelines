# DataPipelines

Local, file-based layered data warehouse framework. Source data flows through fixed layers
(landingzone → raw → datamodel → enriched), each adding typing, history (SCD Type 2), lineage,
and quality checks. Storage is Parquet on local disk under `data/<environment>/<container>/...`.

**Start here:** [ARCHITECTURE.md](ARCHITECTURE.md) — the generic engine (layers, `CustomDF`,
schemas, SCD2, lineage, quality checks) and the checklist for building a new pipeline on top of
it. Read it before touching `polars_src/`, `schema/`, or `processing/`.

## Repo map

- `polars_src/` — the core engine. `custom_dataframes.py` (`CustomDF`, the main abstraction),
  `data_readers.py` (reading tables from disk), `dataframe_helpers.py` (SCD2, hashing, lineage
  helpers), `data_quality_functions.py` (check implementations), `data_query_functions.py`
  (lineage queries), `database.py` (schema dispatch).
- `schema/` — one `schema_<layer>.py` module per layer; each defines every table's columns,
  storage location, and blocking quality checks.
- `processing/` — one `processing_<layer>.py` module per layer; each exposes
  `generate_table_<layer>(table_name)`, dispatching to per-table ETL logic.
- `quality_checks/` — non-blocking ("signalling") check registrations, looked up by table name.
- `connectors/` — pipeline-specific ingestion (scraping/API) and export utilities that sit outside
  the layer engine.
- `spark/` — **deprecated** PySpark mirror of `polars_src/`. Do not extend; new work goes in
  `polars_src/`.
- `tests/` — pytest suite; fixtures under `data/testing/landingzone/`.
- `basketball-scaper.py` — the one existing concrete pipeline (basketball stats scraping →
  enriched analytics). Treat it as a worked reference/usage example, not as part of the generic
  engine.

## Conventions

- Table names always end in `_<layer>` (e.g. `players_raw`) — this suffix drives schema/layer
  resolution throughout the engine. Never omit it.
- Always build/transform tables via `CustomDF`'s `custom_*` methods, not raw Polars calls — they
  preserve the lineage `map_` column. Never read or write the `map_` column directly.
- Every table-producing branch ends with `.write_table()`, which handles SCD2 merge, hashing,
  lineage dump, validation, writing, and signalling checks — don't reimplement any of that
  manually in a processing branch.
- Formatting/linting: `ruff` (defaults). Dependency management: `uv`.

## Docs index

- [ARCHITECTURE.md](ARCHITECTURE.md) — generic engine design + how to build a new pipeline.
