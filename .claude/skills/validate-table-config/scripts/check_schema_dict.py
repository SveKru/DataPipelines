#!/usr/bin/env python
"""
Validate a single table's schema entry for structural correctness against the
conventions in ARCHITECTURE.md's "Table definitions" section.

This does NOT judge whether the table's business logic is right - only whether
the dict shape, key types, and dtypes match what the engine (polars_src/database.py,
polars_src/data_readers.py, polars_src/custom_dataframes.py) actually expects at
runtime. Run it before reading any processing code, since a malformed schema entry
will break every downstream check anyway.

Usage (run from the repo root, so `schema` and `polars_src` are importable):
    uv run python .claude/skills/validate-table-config/scripts/check_schema_dict.py <table_name>

Exit code 0 = no structural problems found. Exit code 1 = at least one problem
(each printed as one line: SEVERITY: message). This is a signal to read closely,
not a substitute for reading the actual schema/processing code.
"""

import sys
from pathlib import Path

# Make repo-root imports (schema.*, polars_src.*) work regardless of cwd.
# scripts/check_schema_dict.py -> scripts -> validate-table-config -> skills -> .claude -> repo root
REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

import polars as pl  # noqa: E402

LAYER_MODULES = {
    "landingzone": "schema.schema_landingzone",
    "raw": "schema.schema_raw",
    "datamodel": "schema.schema_datamodel",
    "enriched": "schema.schema_enriched",
    "monitoring": "schema.schema_monitoring",
}

REQUIRED_KEYS = {
    "columns",
    "container",
    "location",
    "file_format",
    "partition_column",
    "quality_checks",
}

VALID_FILE_FORMATS = {"csv", "json", "parquet"}

VALID_BLOCKING_CHECKS = {
    "values in range": {"columns", "range_start", "range_end"},
    "values are unique": {"columns"},
    "values have format": {"columns", "format"},
}

# For layers above landingzone, every table must carry these history/lineage columns
# with these exact dtypes (see ARCHITECTURE.md: "Table definitions" + "History: SCD Type 2").
REQUIRED_HISTORY_COLUMNS = {
    "RecordID": pl.String,
    "from_date": pl.Date,
    "to_date": pl.Date,
}


def is_polars_dtype(value) -> bool:
    """True for both dtype classes (pl.String) and dtype instances (pl.Decimal(25, 10))."""
    return isinstance(value, pl.datatypes.DataType) or (
        isinstance(value, type) and issubclass(value, pl.datatypes.DataType)
    )


def find_layer_for_table(table_name: str) -> str | None:
    for layer in LAYER_MODULES:
        if table_name.endswith(f"_{layer}"):
            return layer
    return None


def load_schema_dict(layer: str) -> dict:
    import importlib

    module = importlib.import_module(LAYER_MODULES[layer])
    getter_name = f"get_{layer}_schema"
    getter = getattr(module, getter_name)
    return getter()  # no table_name -> returns the full dict for this layer


def check_table(table_name: str) -> list[str]:
    findings = []

    layer = find_layer_for_table(table_name)
    if layer is None:
        findings.append(
            f"BLOCKING: table name '{table_name}' does not end with a recognized layer "
            f"suffix ({', '.join('_' + l for l in LAYER_MODULES)}). get_table_definition() "
            "resolves the layer from this suffix, so a mismatched/missing suffix means the "
            "table can never be read or written correctly."
        )
        return findings

    schema_dict = load_schema_dict(layer)

    if table_name not in schema_dict:
        findings.append(
            f"BLOCKING: '{table_name}' has no entry in schema/schema_{layer}.py's "
            f"get_{layer}_schema() dict. Nothing to validate - add the entry first."
        )
        return findings

    entry = schema_dict[table_name]

    # --- Required keys -----------------------------------------------------
    missing_keys = REQUIRED_KEYS - entry.keys()
    if missing_keys:
        findings.append(
            f"BLOCKING: missing required key(s) {sorted(missing_keys)} in the schema entry. "
            "All five (columns, container, location, file_format, partition_column, "
            "quality_checks) are required regardless of layer."
        )

    # --- container / layer agreement ---------------------------------------
    if entry.get("container") != layer:
        findings.append(
            f"BLOCKING: container='{entry.get('container')!r}' does not match the layer "
            f"implied by the table name suffix ('{layer}'). DataReader builds the on-disk "
            "path from `container`, so a mismatch silently reads/writes the wrong location."
        )

    # --- file_format ---------------------------------------------------------
    file_format = entry.get("file_format")
    if file_format not in VALID_FILE_FORMATS:
        findings.append(
            f"BLOCKING: file_format={file_format!r} is not one of {sorted(VALID_FILE_FORMATS)}. "
            "DataReader.read_source() raises ValueError at read time for anything else."
        )

    # --- columns dict: dtypes ------------------------------------------------
    columns = entry.get("columns")
    if not isinstance(columns, dict):
        findings.append("BLOCKING: 'columns' is not a dict - cannot validate dtypes further.")
    else:
        bad_dtypes = [
            col for col, dtype in columns.items() if not is_polars_dtype(dtype)
        ]
        if bad_dtypes:
            findings.append(
                f"BLOCKING: column(s) {bad_dtypes} use a non-Polars-dtype value (a Python "
                "type, a string, or something else). Every column must map to a `pl.*` "
                "dtype (e.g. pl.String, pl.Int64, pl.Decimal(25, 10)), matching every other "
                "table in this schema module."
            )

        # --- history/lineage columns for layers above landingzone ---------
        if layer != "landingzone":
            for col_name, expected_dtype in REQUIRED_HISTORY_COLUMNS.items():
                if col_name not in columns:
                    findings.append(
                        f"BLOCKING: '{layer}' layer table is missing required column "
                        f"'{col_name}'. Every table above landingzone must declare "
                        "RecordID (pl.String), from_date (pl.Date), and to_date (pl.Date) - "
                        "write_table()'s SCD2/hashing logic assumes they exist."
                    )
                elif columns[col_name] != expected_dtype:
                    findings.append(
                        f"BLOCKING: column '{col_name}' has dtype {columns[col_name]!r}, "
                        f"expected {expected_dtype!r}."
                    )

    # --- partition_column consistency --------------------------------------
    partition_column = entry.get("partition_column")
    if partition_column and isinstance(columns, dict) and partition_column not in columns:
        findings.append(
            f"BLOCKING: partition_column={partition_column!r} is not one of the declared "
            "columns. write_table() partitions the parquet output by this column name, so "
            "it must exist in the schema."
        )

    # --- json_data_path presence (landingzone JSON sources only) -----------
    if layer == "landingzone" and file_format == "json" and "json_data_path" not in entry:
        findings.append(
            "WARNING: file_format='json' but no 'json_data_path' key is set. "
            "DataReader._read_json() defaults this to \"\" via .get(), which may silently "
            "select the wrong records if the JSON source isn't a bare top-level list - "
            "confirm this is intentional."
        )

    # --- quality_checks shape ------------------------------------------------
    quality_checks = entry.get("quality_checks")
    if quality_checks is None:
        findings.append("BLOCKING: 'quality_checks' key missing (use [] if there are none).")
    elif not isinstance(quality_checks, list):
        findings.append("BLOCKING: 'quality_checks' must be a list (of check dicts).")
    else:
        for i, check in enumerate(quality_checks):
            check_name = check.get("check")
            if check_name not in VALID_BLOCKING_CHECKS:
                findings.append(
                    f"BLOCKING: quality_checks[{i}] has check={check_name!r}, which is not "
                    f"one of the blocking check types calculate_blocking_issues() implements "
                    f"({sorted(VALID_BLOCKING_CHECKS)}). A check name it doesn't recognize "
                    "will raise a KeyError at write time, not silently no-op."
                )
                continue
            required_kwargs = VALID_BLOCKING_CHECKS[check_name]
            missing_kwargs = required_kwargs - check.keys()
            if missing_kwargs:
                findings.append(
                    f"BLOCKING: quality_checks[{i}] (check={check_name!r}) is missing "
                    f"kwarg(s) {sorted(missing_kwargs)}."
                )
            if isinstance(check.get("columns"), list) and isinstance(columns, dict):
                unknown_cols = [c for c in check["columns"] if c not in columns]
                if unknown_cols:
                    findings.append(
                        f"WARNING: quality_checks[{i}] references column(s) {unknown_cols} "
                        "that aren't declared in this table's 'columns' dict."
                    )

    return findings


def main():
    if len(sys.argv) != 2:
        print("Usage: check_schema_dict.py <table_name>", file=sys.stderr)
        sys.exit(2)

    table_name = sys.argv[1]
    findings = check_table(table_name)

    if not findings:
        print(f"OK: no structural problems found in the schema entry for '{table_name}'.")
        sys.exit(0)

    print(f"Found {len(findings)} issue(s) in the schema entry for '{table_name}':\n")
    for f in findings:
        print(f"- {f}")
    sys.exit(1)


if __name__ == "__main__":
    main()
