import polars as pl
import os


def export_edges():
    # Example: Export edges between player and team datasets
    recrord_traces = pl.scan_parquet("data/develop/monitoring/record_tracing")

    target_traces = (
        recrord_traces.filter(pl.col("source_RecordID").is_not_null())
        .group_by(["target_table_name", "target_RecordID"])
        .agg(pl.col("source_RecordID").alias("upstream"))
        .rename(
            {
                "target_table_name": "table_id",
                "target_RecordID": "id",
            }
        )
        .with_columns(pl.lit({}).alias("attributes"))
    )

    missing_sources = (
        recrord_traces.filter(pl.col("source_table_name").str.ends_with("raw"))
        .select(
            [
                pl.col("source_RecordID").alias("id"),
                pl.col("source_table_name").alias("table_id"),
            ]
        )
        .with_columns(
            pl.lit([]).alias("upstream"),
            pl.lit({}).alias("attributes"),
        )
        .unique()
    )

    col_order = ["id", "table_id", "upstream", "attributes"]

    recrord_traces = pl.concat(
        [target_traces.select(col_order), missing_sources.select(col_order)],
        how="vertical",
    ).filter(pl.col("id").is_not_null())

    edge_path = "edges.ndjson"

    recrord_traces.sink_ndjson(edge_path)
    # 3. Post-process to make it a valid JSON list (via shell or python)
    # This approach avoids loading the whole dataset into Python memory

    with open("edges.ndjson", "r") as f_in, open("edges.txt", "w") as f_out:
        f_out.write("[")
        first = True
        for line in f_in:
            if not first:
                f_out.write(",\n")
            f_out.write(line.strip())
            first = False
        f_out.write("]")

    os.remove("edges.ndjson")


export_edges()
