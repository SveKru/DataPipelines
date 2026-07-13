#!/usr/bin/env python
"""
Extract a single table's processing branch from its processing_<layer>.py dispatcher
and flag places where raw Polars DataFrame calls bypass CustomDF's lineage-preserving
custom_* methods (see ARCHITECTURE.md: "CustomDF - the core abstraction" ->
"Transformation methods"), plus confirm the branch actually ends by writing the table.

This is a MECHANICAL, line-based scan of one `if/elif table_name == "<table>":` branch -
it does not parse Python into an AST, and it does not know the schema. Every hit it
prints is a candidate for a human/agent to look at in context, not an automatic verdict:
some raw-Polars calls on an *intermediate*, not-yet-a-CustomDF, plain `pl.DataFrame` are
completely fine (nothing to preserve lineage on yet); others silently drop the map_
column on a CustomDF's `.data` and quietly break record_tracing for that edge. Read each
flagged line to tell which case it is.

Usage (run from repo root):
    uv run python .claude/skills/validate-table-config/scripts/check_processing_branch.py \
        processing/processing_raw.py gamedata_raw
"""

import re
import sys
from pathlib import Path

# Methods that have a lineage-preserving CustomDF equivalent. Calling them directly on
# a `.data` / `._df` Polars DataFrame bypasses that equivalent silently - no exception,
# just a map_ column that stops growing for that step.
RAW_METHOD_TO_CUSTOM = {
    "select": "custom_select",
    "join": "custom_join / custom_join_asof",
    "join_asof": "custom_join_asof",
    "group_by": "custom_groupby",
    "groupby": "custom_groupby",
    "unique": "custom_distinct",
}

# Matches e.g. `foo.data.select(`, `foo._df.join_asof(`, `some_thing.data.group_by(`
RAW_CALL_RE = re.compile(
    r"\.(?:data|_df)\.(" + "|".join(RAW_METHOD_TO_CUSTOM) + r")\s*\("
)

BRANCH_HEADER_RE = re.compile(r'^(\s*)(?:el)?if table_name == "([^"]+)":\s*$')

# Matches `CustomDF("some_table_layer"` / `CustomDF('some_table_layer'` — used to list which
# tables a branch reads from, for cross-checking against a "record has expected history"
# quality check's declared expected_trace (see ARCHITECTURE.md: "Lineage / record tracing").
CUSTOMDF_CTOR_RE = re.compile(r'CustomDF\(\s*["\']([a-zA-Z0-9_]+)["\']')


def extract_branch(lines: list[str], table_name: str) -> tuple[int, int, list[str]] | None:
    """Return (start_line_1indexed, end_line_1indexed, body_lines) for the branch
    matching table_name, or None if not found. Body excludes the header line itself."""
    header_indent = None
    start_idx = None
    for i, line in enumerate(lines):
        m = BRANCH_HEADER_RE.match(line)
        if m and m.group(2) == table_name:
            header_indent = len(m.group(1))
            start_idx = i
            break
    if start_idx is None:
        return None

    body = []
    end_idx = start_idx
    for i in range(start_idx + 1, len(lines)):
        line = lines[i]
        stripped = line.strip()
        if stripped == "":
            body.append(line)
            end_idx = i
            continue
        indent = len(line) - len(line.lstrip(" "))
        # Dedent back to (or above) the header's indent level ends the branch -
        # this is either the next elif/else at the same level, or the end of the
        # enclosing function/if-chain.
        if indent <= header_indent:
            break
        body.append(line)
        end_idx = i

    return start_idx + 2, end_idx + 1, body  # 1-indexed, header excluded


def check_branch(file_path: Path, table_name: str) -> tuple[list[str], list[str]]:
    """Returns (findings, source_table_names). source_table_names lists every table
    name passed to a `CustomDF("...")` constructor call inside the branch (including
    the target table's own final CustomDF, if constructed by name) — use this to
    cross-check a declared "record has expected history" expected_trace."""
    findings = []
    lines = file_path.read_text(encoding="utf-8").splitlines(keepends=True)

    result = extract_branch(lines, table_name)
    if result is None:
        findings.append(
            f"BLOCKING: no `if/elif table_name == \"{table_name}\":` branch found in "
            f"{file_path}. Either the table isn't wired into this layer's dispatcher yet, "
            "or the table name doesn't match exactly (check for typos)."
        )
        return findings, []

    start_line, end_line, body = result

    for offset, line in enumerate(body):
        line_no = start_line + offset
        for match in RAW_CALL_RE.finditer(line):
            raw_method = match.group(1)
            custom_equiv = RAW_METHOD_TO_CUSTOM[raw_method]
            findings.append(
                f"line {line_no}: raw `.{raw_method}(` called directly on a DataFrame "
                f"(`.data`/`._df`) - consider `{custom_equiv}` instead if this DataFrame "
                f"belongs to a CustomDF that still needs its lineage tracked. Bypassing it "
                f"doesn't raise an error; it silently stops carrying the map_ column, which "
                f"means record_tracing loses this edge. If this is an intermediate plain "
                f"pl.DataFrame with no lineage yet (e.g. before it's wrapped in CustomDF), "
                f"this is fine - use judgment.\n    {line.strip()}"
            )

    branch_text = "".join(body)
    has_write_table = ".write_table()" in branch_text
    has_customdf_ctor = re.search(
        rf'CustomDF\(\s*["\']?{re.escape(table_name)}["\']?', branch_text
    )

    if not has_customdf_ctor:
        findings.append(
            f"WARNING: no `CustomDF(\"{table_name}\", ...)` construction found in this "
            "branch. write_table() is a CustomDF method - the branch's final result must "
            "be wrapped in a CustomDF named after this table before it can be written."
        )
    if not has_write_table:
        findings.append(
            "BLOCKING: branch does not call `.write_table()`. Without it, SCD2 merge, "
            "RecordID hashing, lineage dump, schema validation, and signalling checks all "
            "get skipped - the table is never actually landed through the engine's write path."
        )

    source_tables = sorted(set(CUSTOMDF_CTOR_RE.findall(branch_text)) - {table_name})

    return findings, source_tables


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: check_processing_branch.py <processing_file> <table_name>",
            file=sys.stderr,
        )
        sys.exit(2)

    file_path = Path(sys.argv[1])
    table_name = sys.argv[2]

    if not file_path.exists():
        print(f"File not found: {file_path}", file=sys.stderr)
        sys.exit(2)

    findings, source_tables = check_branch(file_path, table_name)

    if source_tables:
        print(f"Source tables read via CustomDF(...) in this branch: {source_tables}")
        print(
            "(cross-check this list against any 'record has expected history' "
            "expected_trace declared for this table)\n"
        )

    if not findings:
        print(f"OK: no raw-Polars-call or write_table() issues found for '{table_name}'.")
        sys.exit(0)

    print(f"Found {len(findings)} item(s) to review for '{table_name}' in {file_path}:\n")
    for f in findings:
        print(f"- {f}\n")
    sys.exit(1)


if __name__ == "__main__":
    main()
