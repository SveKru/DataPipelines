import re
import polars as pl
from functools import reduce
import os
from datetime import datetime


def create_map_column(df: pl.DataFrame, dataframe_name: str) -> pl.DataFrame:
    """
    Creates a new column in the dataframe with a map containing the dataframe name as the key and the 'RecordID' column as the value.
    """
    return df.with_columns(pl.col('RecordID').str.split(" ").alias(dataframe_name)).with_columns(
        pl.struct(pl.col(dataframe_name)).alias(f"map_{dataframe_name}")
    ).drop(dataframe_name)


def create_sha_values(df: pl.DataFrame, col_list: list) -> pl.DataFrame:
    """
    Creates SHA values for the specified columns in the DataFrame.
    """
    return df.with_columns(
        pl.concat_str(col_list, separator="|",ignore_nulls=True).hash().cast(pl.String).alias("shaValue")
    )


def get_data_location(schema: dict, partition_name: str = "") -> str:
    """
    Creates a table path based on the given schema and optional partition name.
    """
    if partition_name:
        return os.path.join(os.getcwd(), 'data', schema['container'], schema['location'], 
                          f"{schema['partition_column']}={partition_name}")
    return os.path.join(os.getcwd(), 'data', schema['container'], schema['location'])


def clean_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the column names of a DataFrame.
    """
    new_columns = {}
    for col in df.columns:
        if re.search(r"[-\\\/\(\)\s]", col):
            new_col_name = re.sub(r"[-\\\/]", " ", col)
            new_col_name = re.sub(r"[\(\)]", "", new_col_name)
            new_col_name = re.sub(r"\s+", "_", new_col_name)
            new_columns[col] = new_col_name
    return df.rename(new_columns) if new_columns else df


def apply_scd_type_2(new_table: pl.DataFrame, existing_table: pl.DataFrame) -> pl.DataFrame:
    """
    Applies Slowly Changing Dimension (SCD) Type 2 logic to merge new and existing dataframes.
    """
    processing_date = pl.lit(datetime.now()).cast(pl.Date)
    future_date = pl.lit("2099-12-31").cast(pl.Date)
    map_col = ""
    from_to_list = [pl.col('from_date'),pl.col('to_date')]
    
    # Check if the new table contains a map column
    if [col for col in new_table.columns if col.startswith("map_")]:
        # This is supposed to check if we are creating the the monitoring_valus table
        if not "signalling_id" in existing_table.columns:
            map_col = [
                col for col in new_table.schema if col.startswith("map_")][0]
            struct_cols = [col for col in new_table.schema[map_col].to_schema()]
            struct_keys = [pl.lit(['']).alias(col) for col in struct_cols]
            existing_table = existing_table.with_columns(*struct_keys).with_columns(
                pl.struct([pl.col(col) for col in struct_cols]).alias(map_col)
            )
            from_to_list += [pl.col(map_col)]
        else:
            map_col = "map_monitoring_values"

    value_columns = [col for col in new_table.columns 
                    if col not in ["RecordID", "from_date", "to_date"] 
                    and col != map_col]

    old_closed_records = existing_table.filter(pl.col("to_date") != future_date).select(
        value_columns + from_to_list
    )
    
    old_df = existing_table.filter(pl.col("to_date") == future_date).select(
        value_columns + from_to_list
    )

    old_df = create_sha_values(old_df, value_columns).rename({"shaValue": "shaValueOld"})
    new_data_frame = create_sha_values(new_table, value_columns).with_columns([
        pl.lit(datetime.now()).cast(pl.Date).alias("from_date"),
        future_date.alias("to_date")
    ]).rename({"shaValue": "shaValueNew"})

    combined_df = new_data_frame.select("shaValueNew").join(
        old_df.select("shaValueOld"),
        left_on="shaValueNew",
        right_on="shaValueOld",
        how="full"
    )
    
    all_records = old_closed_records

    identical_mask = (combined_df["shaValueOld"].is_not_null() & 
                     combined_df["shaValueNew"].is_not_null())
    if combined_df.filter(identical_mask).height > 0:
        identical_records = (combined_df.filter(identical_mask)
            .join(new_data_frame.drop("from_date"), on="shaValueNew")
            .join(old_df.select(["shaValueOld", "from_date"]), on="shaValueOld")
            .select(value_columns + from_to_list))

        if all_records.height == 0:
            all_records = identical_records
        else:
            all_records = pl.concat([all_records, identical_records])

    closed_mask = (combined_df["shaValueOld"].is_not_null() & 
                  combined_df["shaValueNew"].is_null())
    if combined_df.filter(closed_mask).height > 0:
        closed_records = (combined_df.filter(closed_mask)
            .join(old_df, on="shaValueOld")
            .select(value_columns + from_to_list)
            .with_columns(processing_date.alias("to_date")))

        if all_records.height == 0:
            all_records = closed_records
        else:
            all_records = pl.concat([all_records, closed_records])

    new_mask = (combined_df["shaValueOld"].is_null() & 
                combined_df["shaValueNew"].is_not_null())
    if combined_df.filter(new_mask).height > 0:
        new_records = (combined_df.filter(new_mask)
            .join(new_data_frame, on="shaValueNew")
            .select(value_columns + from_to_list))
        if all_records.height == 0:
            all_records = new_records
        else:
            all_records = pl.concat([all_records, new_records])
    return all_records


def assign_signalling_id(monitoring_values_df: pl.DataFrame, 
                        existing_monitoring_df: pl.DataFrame) -> pl.DataFrame:
    """
    Assigns signalling IDs to monitoring values.
    """
    max_issue = (existing_monitoring_df.with_columns(
        pl.col("signalling_id").fill_null(0).alias("signalling_id"))
        .select(pl.col("signalling_id").max())
        .item()
    ) or 0

    existing_monitoring_df = (existing_monitoring_df
        .select([
            pl.col(c).alias(f"{c}_old") for c in [
                "signalling_id", "column_name", "check_name", 
                "table_name", "check_id"
            ]
        ])
        .unique()
    )

    monitoring_values_intermediate = monitoring_values_df.join(
        existing_monitoring_df,
        left_on=["table_name", "column_name", "check_name", "check_id"],
        right_on=["table_name_old", "column_name_old", "check_name_old", "check_id_old"],
        how="left"
    )

    # Split and process based on signalling_id presence
    existing_signalling_id = monitoring_values_intermediate.filter(
        pl.col("signalling_id_old").is_not_null()
    )
    
    non_existing_signalling_id = (monitoring_values_intermediate
        .filter(pl.col("signalling_id_old").is_null())
        .with_columns([
            (pl.col("check_id").rank("ordinal").over("table_name") + max_issue).cast(pl.Int64).alias("signalling_id")
        ])
    )

    # Combine and finalize
    if existing_signalling_id.height == 0:
        monitoring_values_df = non_existing_signalling_id.with_columns(
            pl.coalesce("signalling_id_old", "signalling_id").alias("signalling_id")
        ).select([
            "signalling_id", "check_id", "column_name", "check_name",
            "total_count", "valid_count", "table_name"
        ])
    elif non_existing_signalling_id.height == 0:
        monitoring_values_df = existing_signalling_id.with_columns(
            pl.coalesce("signalling_id_old", "signalling_id").alias("signalling_id")
        ).select([
            "signalling_id", "check_id", "column_name", "check_name",
            "total_count", "valid_count", "table_name"
        ])
    else:
        monitoring_values_df = (pl.concat([existing_signalling_id, non_existing_signalling_id])
        .with_columns(
            pl.coalesce("signalling_id_old", "signalling_id").alias("signalling_id")
        )
        .select([
            "signalling_id", "check_id", "column_name", "check_name",
            "total_count", "valid_count", "table_name"
        ])
    )

    return monitoring_values_df
