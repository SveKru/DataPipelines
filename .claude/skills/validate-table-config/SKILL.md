---
name: validate-table-config
description: >
  Validate a table's configuration in this repo's Polars-based layered data-pipeline
  framework (schema definition, processing branch, and record-trace/lineage
  propagation) against the conventions in ARCHITECTURE.md. Use this whenever a table
  is added or modified in schema/schema_*.py or processing/processing_*.py — before
  running it, before a PR, or when asked to "check", "validate", "review", or "sanity
  check" a new table, a schema entry, or a pipeline branch. Also use it when
  something downstream looks wrong (a record_tracing gap, a signalling check that
  never fires, a write that silently loses lineage) to find the structural cause in
  the table's config rather than in the data itself.
---

# Validate table config

A table in this framework is correct only if three things line up: its **schema
entry**, its **processing branch**, and the **lineage** those two produce together.
Each piece can look fine in isolation and still be wrong — a schema entry with all
the right keys but the wrong dtype, a processing branch that reads the right tables
but drops their lineage halfway through, a "record has expected history" check that
was copy-pasted from a different table. This skill walks through all three for one
target table and reports what it finds.

This is a **static, structural** review — it reads source code, not data. It doesn't
run the pipeline or check whether the numbers it produces are correct; it checks
whether the pipeline author followed the framework's conventions well enough that the
engine (SCD2, hashing, lineage, quality checks) will do what ARCHITECTURE.md says it
does. Read `ARCHITECTURE.md` at the repo root first if you haven't already — every
check below cites the section of it that explains *why* the check matters.

## Before you start: identify the target

You need the table's full name (e.g. `playergameplusminus_enriched`) and, from its
`_<layer>` suffix, which two files hold its config:
- `schema/schema_<layer>.py` — the schema entry
- `processing/processing_<layer>.py` — the processing branch (`if/elif table_name == "<table>":`)

If you were given a diff instead of a table name, look at which schema dict gained/
changed an entry, or which `if/elif` branch changed, and extract the table name from
there. If several tables changed, run through this whole checklist once per table —
don't try to average findings across tables together, since a clean table and a
broken one next to each other will otherwise wash each other out in your summary.

## Check 1: Schema definition correctness

Read the target's entry in `schema/schema_<layer>.py`, and run the mechanical checker
against it first — it catches the shape/dtype problems that are tedious to verify by
eye and would otherwise blow up at runtime, not at review time:

```bash
uv run python .claude/skills/validate-table-config/scripts/check_schema_dict.py <table_name>
```

This confirms, per ARCHITECTURE.md's "Table definitions" section:
- the table name's `_<layer>` suffix matches the module it's defined in
- required keys are present: `columns`, `container`, `location`, `file_format`,
  `partition_column`, `quality_checks` (`json_data_path` too, for landingzone JSON
  sources)
- `columns` values are real `pl.*` Polars dtypes — not Python types (`str`, `int`),
  not strings (`"pl.String"`), which the engine cannot use as a schema
- for any layer above landingzone: `columns` includes `RecordID` (`pl.String`),
  `from_date` (`pl.Date`), `to_date` (`pl.Date`) — required because `write_table()`'s
  SCD2/hashing logic assumes they exist (see "History: SCD Type 2")
- `container` matches the layer (a mismatch means `DataReader` builds the wrong
  on-disk path and reads/writes to the wrong place)
- `file_format` is one of `csv`/`json`/`parquet`
- `partition_column`, if set, is actually one of the declared columns
- each `quality_checks` entry uses a check name/kwargs the engine actually
  implements (`values in range`, `values are unique`, `values have format` — see
  "Quality checks" in ARCHITECTURE.md; anything else raises `KeyError` at write time,
  not at review time)

The script's exit code tells you whether to keep digging: 0 means the shape is sound,
1 means read its findings before moving on — a broken schema entry will make every
downstream check in this skill unreliable too. Report what it found either way; don't
silently re-derive the same checks by eye once the script has already run them.

Two things the script can't check that you should read for yourself:
- Does `location` make sense for this table (not colliding with another table's
  path, not pointing at a totally unrelated directory)?
- For enriched/datamodel tables with a `quality_checks` entry that references another
  table's data (rare, but possible via custom check kwargs) — does that reference
  still make sense given what the processing branch actually reads?

## Check 2: `custom_*` usage in the processing branch

Find the target's branch in `processing/processing_<layer>.py` and run:

```bash
uv run python .claude/skills/validate-table-config/scripts/check_processing_branch.py processing/processing_<layer>.py <table_name>
```

This extracts just that one `if/elif` branch (from its header to the next branch at
the same indent level) and flags two things, per ARCHITECTURE.md's "CustomDF"
section:

1. **Raw Polars calls that bypass a `custom_*` equivalent** — `.data.select(`,
   `.data.join(`, `.data.join_asof(`, `.data.group_by(`/`.groupby(`, `.data.unique(`
   (or the same on `._df`). These don't raise an error; they just silently stop
   carrying the `map_` lineage column forward from that point on, so
   `record_tracing` quietly loses an edge. The corresponding `custom_select` /
   `custom_join` / `custom_join_asof` / `custom_groupby` / `custom_distinct` methods
   exist specifically to avoid this.

   **Use judgment on each hit, don't blanket-reject them.** A raw call on a
   DataFrame that hasn't been wrapped in a `CustomDF` yet (e.g. still coming
   straight off a landingzone read before any lineage exists) is completely fine —
   there's nothing to preserve yet. The problem is specifically when a DataFrame
   that *does* carry a `map_` column (i.e. it came from a `CustomDF` at or above
   raw layer) gets a raw call applied to `.data`/`._df` instead of the `custom_*`
   equivalent. Read enough of the surrounding branch to tell which case you're
   looking at before flagging it as a real issue — the existing codebase (e.g.
   `playeranalytics_enriched`) has several raw `.unique()` calls on datamodel-layer
   CustomDFs that are worth flagging for exactly this reason, so you're not being
   overly strict by raising them.

2. **The branch actually writes the table.** It should end by wrapping its final
   result in `CustomDF("<table_name>", initial_df=...)` and calling `.write_table()`
   — not writing parquet directly, not skipping the write. Skipping `write_table()`
   means SCD2, hashing, lineage dump, schema validation, and signalling checks all
   get skipped for this table (see "Writing a table" in ARCHITECTURE.md) — this is
   always worth flagging as blocking, since there's no legitimate reason for a table
   branch to skip it.

The script also prints every table name the branch constructs via `CustomDF(...)` —
keep this list, you need it for Check 3.

## Check 3: Record trace / lineage propagation

This check exists because lineage failures are invisible until someone tries to use
`get_record_history()` or a "record has expected history" signalling check and gets
an incomplete or wrong answer — by then it's much harder to tell whether the gap is
a real data issue or a config mistake made when the table was written. Catching it
here, structurally, is much cheaper.

1. **Landingzone tables have no lineage yet.** If the target table is itself a
   `_landingzone` table, this whole check doesn't apply — skip it and say so. (Lineage
   starts at raw layer, per ARCHITECTURE.md's "Lineage: the `map_` column".)

2. **Every source table the branch reads from should flow into the final result via
   `custom_*` methods only.** This is really the same finding as Check 2 viewed from
   the lineage angle: any raw-Polars call flagged in Check 2 on a DataFrame that
   carries a `map_` column means that source table's contribution to lineage was
   dropped somewhere in the branch, and `record_tracing` will never learn about that
   edge for this table's future writes. You don't need to redo the analysis — just
   connect Check 2's findings to this consequence explicitly in your report, since
   "you bypassed custom_select" and "record_tracing is missing an edge for this
   source table" are two ways of describing the same bug.

3. **Cross-check any declared "record has expected history" check.** Search the
   target's schema `quality_checks` list and
   `quality_checks/signalling_rules_datamodel.py` for a `"record has expected
   history"` entry for this table (see `record_has_expected_history` in
   `polars_src/data_quality_functions.py`, and the "Quality checks" section of
   ARCHITECTURE.md). If one exists, it declares an `expected_trace` with two parallel
   lists: `source_table_name` and `amount_records`. Compare
   `expected_trace["source_table_name"]` against the source-tables list Check 2's
   script printed for this branch:
   - Every table in `expected_trace["source_table_name"]` should actually be read by
     the branch (via `CustomDF(...)`) — an entry for a table the branch doesn't
     touch anymore is a stale check that will fail at runtime for every record.
   - Every table the branch reads from and merges via `custom_join`/`custom_union`/
     etc. into the final result should generally appear in `expected_trace` too,
     unless there's a clear reason a particular source's lineage wouldn't reach the
     final written rows (e.g. it's only used to compute an intermediate filter, not
     joined into the output). If you can't tell, say so and flag it as worth a
     second look rather than guessing.
   - `amount_records` should roughly match how many times each source table's
     `RecordID`s would be expected per target record, given the join logic in the
     branch (e.g. a 1:1 join contributes 1, a fan-out join or union of two sources
     contributes 2, etc.) — this is a judgment call based on reading the branch's
     joins, not something the scripts compute for you.

   If no such check is declared for this table, don't treat that as an issue by
   itself — it's optional. Just note it wasn't found, and move on.

## Reporting findings

Report per check category (Schema / `custom_*` usage / Record trace), not as one
flat list — the reader needs to know which layer of the problem they're looking at.
For each finding, be concrete rather than generic:

- **file + line** (from the script output, or your own reading)
- **what's wrong**, stated plainly
- **why it matters** — tie it back to the specific ARCHITECTURE.md mechanism it
  breaks (e.g. "breaks record_tracing for this edge", "raises ValueError at write
  time", "SCD2 will silently misbehave"), not a generic "this is bad practice"
- **suggested fix** — usually a one-line change (swap a method call, add a missing
  key, add/adjust a column)

End with a short pass/fail summary line per category (e.g. "Schema: OK", "custom_*
usage: 2 issues to review", "Record trace: 1 stale expected_trace entry") so someone
skimming can tell at a glance whether the table is ready or needs another pass.

Findings that are genuinely ambiguous (per the judgment calls flagged in Checks 2
and 3) should be reported as "worth a second look" rather than stated as definite
bugs — false confidence here is worse than an honest "not sure, here's why."
