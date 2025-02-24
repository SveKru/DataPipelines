import re
import ast
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.types as T
from pyspark.sql.functions import udf
from functools import reduce
import os


def create_map_column(dataframe: DataFrame, dataframe_name: str) -> DataFrame:
    """
    Creates a new column in the dataframe with a map containing the dataframe name as the key and the 'RecordID' column as the value.

    Args:
        spark_session (SparkSession): The Spark session.
        dataframe (DataFrame): The input dataframe.
        dataframe_name (str): The name of the dataframe.

    Returns:
        DataFrame: The dataframe with the new map column.
    """
    dataframe = dataframe.withColumn(
        f"map_{dataframe_name}",
        F.create_map(F.lit(dataframe_name), F.array(F.col("RecordID"))),
    )
    return dataframe


def create_sha_values(data_frame: DataFrame, col_list: list) -> DataFrame:
    """
    Creates SHA values for the specified columns in the DataFrame.

    Args:
        spark_session (SparkSession): The SparkSession object.
        data_frame (DataFrame): The input DataFrame.
        col_list (list): The list of column names to create SHA values for.

    Returns:
        DataFrame: The DataFrame with the 'shaValue' column added, containing the SHA values.

    """
    data_frame = data_frame.withColumn(
        "shaValue", F.sha2(F.concat_ws("|", *col_list), 256)
    )

    return data_frame


def get_data_location(schema: dict, partition_name: str = "") -> str:
    """
    Creates a table path based on the given environment, schema, and optional partition name.

    Args:
        environment (str): The environment name.
        schema (dict): The schema dictionary containing container, location, and partition_column.
        partition_name (str, optional): The name of the partition. Defaults to ''.

    Returns:
        str: The table path.

    """

    if partition_name:
        return os.path.join(os.getcwd(),'data',schema['container'],schema['location'],f"{schema['partition_column']}={partition_name}")

    return os.path.join(os.getcwd(),'data',schema['container'],schema['location'])


def create_table_name(environment: str, container: str, location: str) -> str:
    """
    Creates a table name by combining the environment, container, and location.

    Args:
        environment (str): The environment name.
        container (str): The container name.
        location (str): The location name.

    Returns:
        str: The formatted table name.
    """
    return f"`{environment}`.`{container}`.`{location.replace('.','')}`"


def clean_column_names(data_frame: DataFrame) -> DataFrame:
    """
    Cleans the column names of a DataFrame by removing special characters,
    replacing spaces with underscores, and removing parentheses.

    Args:
        data_frame (DataFrame): The input DataFrame with column names to be cleaned.

    Returns:
        DataFrame: The DataFrame with cleaned column names.
    """
    for col in data_frame.columns:
        if re.search(r"[-\\\/\(\)\s]", col):
            new_col_name = re.sub(r"[-\\\/]", " ", col)
            new_col_name = re.sub(r"[\(\)]", "", new_col_name)
            new_col_name = re.sub(r"\s+", "_", new_col_name)
            data_frame = data_frame.withColumnRenamed(col, new_col_name)
    return data_frame


def create_catalog_schema(environment: str, schema: dict) -> str:
    """
    Creates a catalog schema if it doesn't already exist.

    Args:
        environment (str): The environment in which the schema should be created.
        schema (dict): A dictionary containing the schema details, including the container name.

    Returns:
        str: The SQL string for creating the catalog schema.
    """

    create_catalog_schema_string = (
        f'CREATE SCHEMA IF NOT EXISTS {environment}.{schema["container"]};'
    )
    create_catalog_schema_owner = (
        f'ALTER SCHEMA {environment}.{schema["container"]} SET OWNER TO tiltDevelopers;'
    )

    return create_catalog_schema_string, create_catalog_schema_owner


def create_catalog_table_owner(table_name: str) -> str:
    """
    Creates a SQL string to set the owner of a table.

    Args:
        table_name (str): The name of the table.

    Returns:
        str: The SQL string to set the owner of the table.
    """

    create_catalog_table_owner_string = (
        f"ALTER TABLE {table_name} SET OWNER TO tiltDevelopers"
    )

    return create_catalog_table_owner_string


def apply_scd_type_2(new_table: DataFrame, existing_table: DataFrame) -> DataFrame:
    """
    Applies Slowly Changing Dimension (SCD) Type 2 logic to merge new and existing dataframes.

    Args:
        new_table (DataFrame): The new dataframe containing the updated records.
        existing_table (DataFrame): The existing dataframe containing the current records.

    Returns:
        DataFrame: The merged dataframe with updated records based on SCD Type 2 logic.
    """
    # Determine the processing date
    processing_date = F.current_date()
    future_date = F.lit("2099-12-31")
    map_col = ""
    from_to_list = [F.col("from_date"), F.col("to_date")]

    # Check if the new table contains a map column
    if [col for col in new_table.columns if col.startswith("map_")]:
        # This is supposed to check if we are creating the the monitoring_valus table
        if not "signalling_id" in existing_table.columns:
            map_col = [
                col for col in new_table.columns if col.startswith("map_")][0]
            existing_table = existing_table.withColumn(
                map_col, F.create_map().cast("Map<String, Array<String>>")
            )
            from_to_list += [F.col(map_col)]
        else:
            map_col = "map_monitoring_values"

    # Select the columns that contain values that should be compared
    value_columns = [
        F.col(col_name)
        for col_name in new_table.columns
        if col_name not in ["RecordID", "from_date", "to_date"]
        and col_name != map_col
    ]

    old_closed_records = existing_table.filter(F.col("to_date") != future_date).select(
        value_columns + from_to_list
    )
    old_df = existing_table.filter(F.col("to_date") == future_date).select(
        value_columns + from_to_list
    )

    # Add the SHA representation of the old records and rename to unique name
    old_df = create_sha_values(old_df, value_columns)
    old_df = old_df.withColumnRenamed("shaValue", "shaValueOld")

    # Add the SHA representation of the incoming records and rename to unique name
    new_data_frame = create_sha_values(new_table, value_columns)
    new_data_frame = new_data_frame.withColumn("from_date", processing_date).withColumn(
        "to_date", F.to_date(future_date)
    )
    new_data_frame = new_data_frame.withColumnRenamed(
        "shaValue", "shaValueNew")

    # Join the SHA values of both tables together
    combined_df = new_data_frame.select(F.col("shaValueNew")).join(
        old_df.select("shaValueOld"),
        on=old_df.shaValueOld == new_data_frame.shaValueNew,
        how="full",
    )

    # Set the base of all records to the already expired/ closed records
    all_records = old_closed_records
    # Records that did not change are taken from the existing set of data
    identical_records = combined_df.filter(
        (F.col("shaValueOld").isNotNull()) & (F.col("shaValueNew").isNotNull())
    )
    if identical_records.count() > 0:
        identical_records = combined_df.filter(
            (F.col("shaValueOld").isNotNull()) & (
                F.col("shaValueNew").isNotNull())
        ).join(new_data_frame.drop('from_date'), on="shaValueNew", how="inner"
               ).join(old_df.select(["shaValueOld", "from_date"]), on="shaValueOld", how="inner")
        identical_records = identical_records.select(
            value_columns + from_to_list)
        all_records = all_records.union(identical_records)

    # Records that do not exist anymore are taken from the existing set of data
    # Records are closed by filling the to_date column with the current date
    closed_records = combined_df.filter(
        (F.col("shaValueOld").isNotNull()) & (F.col("shaValueNew").isNull())
    )
    if closed_records.count() > 0:
        closed_records = combined_df.filter(
            (F.col("shaValueOld").isNotNull()) & (
                F.col("shaValueNew").isNull())
        ).join(old_df, on="shaValueOld", how="inner")
        closed_records = closed_records.select(value_columns + from_to_list)
        closed_records = closed_records.withColumn("to_date", processing_date)
        all_records = all_records.union(closed_records)

    # Records that are new are taken from the new set of data
    new_records = combined_df.filter(
        (F.col("shaValueOld").isNull()) & (F.col("shaValueNew").isNotNull())
    )
    if new_records.count() > 0:
        new_records = combined_df.filter(
            (F.col("shaValueOld").isNull()) & (
                F.col("shaValueNew").isNotNull())
        ).join(new_data_frame, on="shaValueNew", how="inner")
        new_records = new_records.select(value_columns + from_to_list)
        all_records = all_records.union(new_records)

    return all_records


def assign_signalling_id(
    monitoring_values_df: DataFrame, existing_monitoring_df: DataFrame
) -> DataFrame:

    max_issue = (
        existing_monitoring_df.fillna(0, subset="signalling_id")
        .select(F.max(F.col("signalling_id")).alias("max_signalling_id"))
        .collect()[0]["max_signalling_id"]
    )
    if not max_issue:
        max_issue = 0
    existing_monitoring_df = (
        existing_monitoring_df.select(
            [F.col(c).alias(c + "_old")
             for c in existing_monitoring_df.columns]
        )
        .select(
            [
                "signalling_id_old",
                "column_name_old",
                "check_name_old",
                "table_name_old",
                "check_id_old",
            ]
        )
        .distinct()
    )
    w = Window().partitionBy("table_name").orderBy(F.col("check_id"))
    join_conditions = [
        monitoring_values_df.table_name == existing_monitoring_df.table_name_old,
        monitoring_values_df.column_name == existing_monitoring_df.column_name_old,
        monitoring_values_df.check_name == existing_monitoring_df.check_name_old,
        monitoring_values_df.check_id == existing_monitoring_df.check_id_old,
    ]
    monitoring_values_intermediate = monitoring_values_df.join(
        existing_monitoring_df, on=join_conditions, how="left"
    )

    existing_signalling_id = monitoring_values_intermediate.where(
        F.col("signalling_id_old").isNotNull()
    )
    non_existing_signalling_id = monitoring_values_intermediate.where(
        F.col("signalling_id_old").isNull()
    )
    non_existing_signalling_id = non_existing_signalling_id.withColumn(
        "signalling_id", F.row_number().over(w) + F.lit(max_issue)
    )

    monitoring_values_intermediate = existing_signalling_id.union(
        non_existing_signalling_id
    )
    monitoring_values_intermediate = monitoring_values_intermediate.withColumn(
        "signalling_id", F.coalesce(
            F.col("signalling_id_old"), F.col("signalling_id"))
    )
    monitoring_values_df = monitoring_values_intermediate.select(
        [
            "signalling_id",
            "check_id",
            "column_name",
            "check_name",
            "total_count",
            "valid_count",
            "table_name",
        ]
    )

    return monitoring_values_df


def ledger_x_ecoinvent_matcher(ledger, ecoinvent):
  
    ledger_cols = ["ledger."+column for column in ledger.columns]
    init_df = ecoinvent.alias("ei").join(ledger.alias("ledger"),
                                         (
        (F.col('ei.geography') == F.col(f'ledger.ecoinvent_geography')) &
        (F.col('ei.isic_4digit') == F.col('ledger.isic_code')) &
        (F.col('ei.cpc_code') == F.col('ledger.cpc_code')) &
        (F.col('ei.activity_type') == F.col('ledger.activity_type'))
    ), how="right")
    complete_df = init_df.filter(init_df.activity_uuid_product_uuid.isNotNull()).drop(
        F.col("ei.geography"), F.col("ei.cpc_code"), F.col("ei.activity_type"))
    # unmatched_records = init_df.filter(init_df.activity_uuid_product_uuid.isNull()).select(ledger_cols)
    windowSpec = Window.partitionBy(
        "tiltledger_id", "reference_product_name", "activity_name", "activity_type").orderBy("priority")

    ledger_ecoinvent_mapping = complete_df.withColumn("row_number", F.dense_rank(
    ).over(windowSpec)).filter("row_number = 1").drop("row_number")
    return ledger_ecoinvent_mapping


def check_nonempty_tiltsectors_for_nonempty_isic_pyspark(df):
    isic = df.select(df.colRegex("`.*isic_code.*`")).columns[0]
    tilt_sec = df.select(df.colRegex("`.*tilt_sector.*`")).columns[0]
    tilt_subsec = df.select(df.colRegex("`.*tilt_subsector.*`")).columns[0]
    test_null_tiltsec = df.filter(
        df[isic].isNotNull()).select(tilt_sec, tilt_subsec)

    # Check for null rows in tilt_sec column
    null_tiltsec = test_null_tiltsec.filter(F.col(tilt_sec).isNull())
    # Check for null rows in tilt_subsec column
    null_tiltsubsec = test_null_tiltsec.filter(F.col(tilt_subsec).isNull())
    # Check if both columns have no null rows
    if null_tiltsec.count() == 0 and null_tiltsubsec.count() != 0:
        raise ValueError(
            "For every isic there should be a tilt_sector & tilt_subsector")
    print("Non-empty tiltsector for non-empty isic_code check passed")


def check_null_product_name_pyspark(df):
    product_name = df.select(df.colRegex("`.*product_name.*`")).columns[0]
    if df.filter(df[product_name].isNull()).count() > 0:
        raise ValueError("`product_name` can't have any null value")
    print("Empty product name check passed")


def sanitize_co2(df):
    # first check if the isic_4digit column exists
    if "isic_code" not in df.columns:
        # throw an error stating that the column is missing
        raise ValueError('isic_code column is missing')
    df = df.withColumn('isic_code', F.lpad(
        F.regexp_replace("isic_code", "'", ""), 4, '0'))
    return df


def column_check(df1):
    co2_footprint = df1.select(df1.colRegex("`.*co2_footprint.*`")).columns[0]
    tilt_sec = df1.select(df1.colRegex("`.*tilt_sector.*`")).columns[0]
    unit = df1.select(df1.colRegex("`.*unit.*`")).columns[0]

    important_cols = [co2_footprint, tilt_sec, unit]
    # raise error if the df1 or df2 is empty
    if df1.isEmpty():
        raise ValueError('Dataframe is empty')

    # check if the important columns needed for the calculation are present in the dataframe
    if len(important_cols) != 3:
        raise ValueError('Important columns are missing')

    print("Column presence check passed")


def prepare_co2(co2_df):
    isic = co2_df.select(co2_df.colRegex("`.*isic_code.*`")).columns[0]
    tilt_sec = co2_df.select(co2_df.colRegex("`.*tilt_sector.*`")).columns[0]
    co2_footprint = co2_df.select(
        co2_df.colRegex("`.*co2_footprint.*`")).columns[0]
    co2_df = co2_df.filter(co2_df[tilt_sec].isNotNull())
    co2_df = co2_df.filter(co2_df[isic].isNotNull())
    co2_df = co2_df.filter(co2_df[co2_footprint].isNotNull())
    return co2_df


def structure_postcode(postcode: str) -> str:
    """Structure raw postcode to be in the format '1234 AB'.

    Args:
        postcode (str): Raw postcode string, at least length of 6

    Returns:
        str: Structured postcode in the predefined format of '1234 AB'
    """

    # Use regex to extract the numbers and letters from the postcode
    num = F.regexp_extract(postcode, r"^(\d{4})", 1)
    alph = F.regexp_extract(postcode, r"([A-Za-z]{2})$", 1)

    # Format postcode into `1234 AB`
    return F.concat(num, F.lit(" "), F.upper(alph))


def format_postcode(postcode: str, city: str) -> str:
    """Format Europages postcode to match the postcodes of Company.Info for
    consistency

    Args:
        postcode (str): Raw Europages company postcode
        city (str): Raw Europages company city

    Returns:
        str: Europages postcode formatted alike Company.Info
    """
    # postcode mostly looks like: '1234'
    # city mostly looks like: 'ab city_name'

    # if postcode and city are identical, take the postcode; otherwise concatenate the two into '1234ab city_name'
    reference = F.when(postcode == city, postcode).otherwise(
        F.concat(postcode, city))

    # if reference is just the city or NA, just just return empty string
    reference = F.when(
        ~reference.isin(["etten-leur", "kruiningen"]) | reference.isNotNull(),
        structure_postcode(F.split(reference, " ")[0]),
    ).otherwise("")

    return reference


def keep_one_name(default_name: str, statutory_name: str) -> str:
    """Prioritise the statutory name for a Company.Info if available, otherwise
    keep the default institution name for the company name.

    Args:
        default_name (str): Default institution name of Company.Info company
        statutory_name (str|None): Statutory name of Company.Info company

    Returns:
        str: Either the default_name or statutory_name
    """
    # Take the statutory name (on KvK) if available, otherwise take the default name (from their marketing database)
    name = F.when(statutory_name.isNotNull(), F.lower(statutory_name)).otherwise(
        F.lower(default_name)
    )

    return name


def emissions_profile_compute(emission_data, ledger_ecoinvent_mapping,  output_type="combined"):

    # define a dictionary with the 6 different benchmark types
    benchmark_types = {
        "all": [],
        "isic_4digit": ["isic_code"],
        "tilt_sector": ["tilt_sector"],
        "unit": ["unit"],
        "unit_isic_4digit": ["unit", "isic_code"],
        "unit_tilt_sector": ["unit", "tilt_sector"]
    }

    groups = []

    if output_type == "combined":
        for bench_type, cols in benchmark_types.items():
            # Create a Window specification
            temp_df = emission_data
            if bench_type == "all":
                length = temp_df.select("co2_footprint").distinct().count()
                windowSpec = Window.orderBy("co2_footprint")
                # Add the dense rank column
                temp_df = temp_df.withColumn(
                    'dense_rank', F.dense_rank().over(windowSpec))
                # Divide the dense rank by length and create the profile_ranking column
                temp_df = temp_df.withColumn('profile_ranking', F.col(
                    'dense_rank') / F.lit(length)).drop(F.col('dense_rank'))
            else:
                all_columns = emission_data.columns
                grouping_columns = [column for column in all_columns if any(
                    pattern in column for pattern in cols)]
                windowSpec = Window.partitionBy(
                    grouping_columns).orderBy(F.col('co2_footprint'))
                temp_df = temp_df.withColumn(
                    'dense_rank', F.dense_rank().over(windowSpec))
                temp_df = temp_df.withColumn('length', F.count(
                    '*').over(Window.partitionBy(grouping_columns)))
                temp_df = temp_df.withColumn('profile_ranking', F.col(
                    'dense_rank') / F.col('length')).drop("dense_rank", "length")
            temp_df = temp_df.withColumn("benchmark_group", F.lit(bench_type))
            groups.append(temp_df)

        # Concatenate the DataFrames
        concatenated_df = reduce(lambda df1, df2: df1.unionAll(
            df2), groups).withColumnsRenamed({"reference_product_name": "product_name"})

        concatenated_df = ledger_ecoinvent_mapping.join(
            concatenated_df, on="activity_uuid_product_uuid", how="left").filter(F.col("benchmark_group").isNotNull())

        # Sum the profile_ranking values for each tiltledger_id
        average_df = concatenated_df.groupBy("tiltledger_id", "benchmark_group").agg(
            F.avg("profile_ranking").alias("average_profile_ranking"))

        average_co2_df = concatenated_df.groupBy("tiltledger_id", "benchmark_group").agg(
            F.avg("co2_footprint").alias("average_co2_footprint"))

        # Join the average_df with concatenated_df to add the average_profile_ranking column
        concatenated_df = concatenated_df.join(average_df, ["tiltledger_id", "benchmark_group"]).join(
            average_co2_df, ["tiltledger_id", "benchmark_group"])

        # Drop duplicate tilt records per benchmark type
        concatenated_df = concatenated_df.dropDuplicates(
            subset=["tiltledger_id", "benchmark_group"]).drop("profile_ranking")

        concatenated_df = concatenated_df.withColumn(
            "risk_category",
            F.when(F.col("average_profile_ranking") <= 1/3, "low")
            .when((F.col("average_profile_ranking") > 1/3) &
                  (F.col("average_profile_ranking") <= 2/3), "medium")
            .otherwise("high")
        )
    return concatenated_df


def emissions_profile_upstream_compute(emission_data_upstream, ledger_ecoinvent_mapping, output_type="combined"):

    if output_type == "combined":

        # define a dictionary with the 6 different benchmark types
        benchmark_types = {
            "all": [],
            "window_input_isic_4digit": ["input_isic_4digit"],
            "window_input_tilt_sector": ["input_tilt_sector"],
            "window_input_unit": ["input_unit"],
            "window_input_unit_isic_4digit": ["input_unit", "input_isic_4digit"],
            "window_input_unit_tilt_sector": ["input_unit", "input_tilt_sector"]
        }

        window_all = Window.orderBy("input_co2_footprint")
        window_input_isic_4digit = Window.partitionBy(
            "input_isic_code").orderBy("input_co2_footprint")
        window_input_tilt_sector = Window.partitionBy(
            "input_tilt_sector").orderBy("input_co2_footprint")
        window_input_unit = Window.partitionBy(
            "input_unit").orderBy("input_co2_footprint")
        window_input_unit_isic_4digit = Window.partitionBy(
            "input_unit", "input_isic_code").orderBy("input_co2_footprint")
        window_input_unit_tilt_sector = Window.partitionBy(
            "input_unit", "input_tilt_sector").orderBy("input_co2_footprint")

        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_all", F.dense_rank().over(window_all))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_isic_4digit", F.dense_rank().over(window_input_isic_4digit))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_tilt_sector", F.dense_rank().over(window_input_tilt_sector))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_unit", F.dense_rank().over(window_input_unit))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_unit_isic_4digit", F.dense_rank().over(window_input_unit_isic_4digit))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_unit_tilt_sector", F.dense_rank().over(window_input_unit_tilt_sector))

        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_all", F.col("rank_all")/F.max("rank_all").over(window_all.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_isic_4digit", F.col("rank_input_isic_4digit")/F.max("rank_input_isic_4digit").over(window_input_isic_4digit.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_tilt_sector", F.col("rank_input_tilt_sector")/F.max("rank_input_tilt_sector").over(window_input_tilt_sector.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_unit", F.col("rank_input_unit")/F.max("rank_input_unit").over(window_input_unit.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_unit_isic_4digit", F.col("rank_input_unit_isic_4digit")/F.max("rank_input_unit_isic_4digit").over(window_input_unit_isic_4digit.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "rank_input_unit_tilt_sector", F.col("rank_input_unit_tilt_sector")/F.max("rank_input_unit_tilt_sector").over(window_input_unit_tilt_sector.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))

        emission_data_upstream.data = emission_data_upstream.data.unpivot(["activity_uuid_product_uuid", "input_activity_uuid_product_uuid", "input_co2_footprint", emission_data_upstream.map_col], [
            "rank_all", "rank_input_isic_4digit", "rank_input_tilt_sector", "rank_input_unit", "rank_input_unit_isic_4digit", "rank_input_unit_tilt_sector"], "benchmark_group", "profile_ranking")

        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "benchmark_group", F.regexp_replace(F.col('benchmark_group'), 'rank_', ''))

        emission_data_upstream = ledger_ecoinvent_mapping.custom_join(
            emission_data_upstream, custom_on="activity_uuid_product_uuid", custom_how="left")

        emission_data_upstream.data = emission_data_upstream.data.filter(
            F.col("benchmark_group").isNotNull())

        emission_data_upstream = emission_data_upstream.custom_select(
            ["input_activity_uuid_product_uuid", "tiltledger_id", "benchmark_group", "profile_ranking", "input_co2_footprint"])

        emission_data_upstream = emission_data_upstream.custom_groupby(["tiltledger_id", "benchmark_group"], F.avg("profile_ranking").alias("average_input_profile_rank"),
                                                                       F.avg("input_co2_footprint").alias("average_input_co2_footprint"))

        emission_data_upstream.data = emission_data_upstream.data.withColumn(
            "risk_category",
            F.when(F.col("average_input_profile_rank") <= 1/3, "low")
            .when((F.col("average_input_profile_rank") > 1/3) &
                  (F.col("average_input_profile_rank") <= 2/3), "medium")
            .otherwise("high")
        )

        emission_data_upstream = emission_data_upstream.custom_select(
            ['tiltledger_id', 'benchmark_group', 'average_input_profile_rank', 'average_input_co2_footprint', 'risk_category'])

    return emission_data_upstream


def calculate_reductions(reductions_dataframe, name_replace_dict):
    # 1. Identify columns
    sector_cols = [
        col for col in reductions_dataframe.columns if '_sector' in col]
    subsector_cols = [
        col for col in reductions_dataframe.columns if '_subsector' in col]
    if sector_cols and subsector_cols:
        sector = sector_cols[0]
        subsector = subsector_cols[0]

        # 2. Sort is not needed in PySpark as Window function will handle the ordering
        reductions_dataframe = reductions_dataframe.orderBy(['scenario', 'region', sector, subsector, 'year'],
                                                            ascending=True)

        # 3. Define the window specification
        windowSpec = Window.partitionBy("scenario", "region", sector, subsector).rowsBetween(
            Window.unboundedPreceding, Window.currentRow)

        # 4. Calculate reductions
        reductions_dataframe = reductions_dataframe.withColumn(
            "first_value", F.first("value").over(windowSpec))
        reductions_dataframe = reductions_dataframe.withColumn(
            "reductions", (1 - (F.col("value") / F.col("first_value"))).cast("double"))
        reductions_dataframe = reductions_dataframe.withColumn(
            "reductions", F.round("reductions", 2))

        # 5. Filter only on year 2030 and 2050 targets
        reductions_dataframe = reductions_dataframe.filter(
            F.col("year").isin([2030, 2050]))

        # 6. Make the scenario names more concise
        for key, value in name_replace_dict.items():
            reductions_dataframe = reductions_dataframe.withColumn('scenario', F.when(
                F.col('scenario') == key, value).otherwise(F.col('scenario')))

        return reductions_dataframe
    else:
        raise ValueError("Required columns not found in DataFrame")


def scenario_preparing(data):
    # 1. Identify columns containing 'sector' and extract the scenario_type
    sector_columns = [col for col in data.columns if 'sector' in col]
    scenario_types = list(set(col.split("_")[0] for col in sector_columns))

    if scenario_types:
        scenario_type = scenario_types[0]

        # 2. Rename columns by removing the scenario_type prefix
        for col in data.columns:
            if col.startswith(scenario_type + "_"):
                new_col_name = col.replace(scenario_type, "scenario")
                data = data.withColumnRenamed(col, new_col_name)

        # 3. Add a new column 'type' with the scenario_type value for each row
        data = data.withColumn("scenario_type", F.lit(scenario_type))

        # 4. Rename columns
        data = data.withColumnRenamed(
            "scenario", "scenario_name").drop("first_value")
    return data


def get_combined_targets(ipr, weo):
    # Concatenate DataFrames
    combined_targets = ipr.custom_union(weo)

    # Check if 'reductions' column exists and its type is float64 (DoubleType in PySpark)
    try:
        reductions_field = [
            f for f in combined_targets.data.schema.fields if f.name == 'reductions'][0]
        if not isinstance(reductions_field.dataType, T.DoubleType):
            raise ValueError(
                f"`reductions` column in `combined_targets` is not `float64`")
    except IndexError:
        raise ValueError("`reductions` column not found in `combined_targets`")

    return combined_targets.custom_drop(["scenario_targets_ipr_id", "scenario_targets_weo_id"])


def sector_profile_compute(input_sector_profile_ledger_x):
    # Splitting the dataframe based on year
    df_2030 = input_sector_profile_ledger_x.filter(F.col("year") == 2030)
    df_2050 = input_sector_profile_ledger_x.filter(F.col("year") == 2050)

    # Setting different thresholds for each dataframe
    low_threshold_2030, high_threshold_2030 = 1/9, 1/3  # thresholds for 2030
    low_threshold_2050, high_threshold_2050 = 2/9, 2/3   # thresholds for 2050

    df_2030 = df_2030.withColumn(
        "risk_category",
        F.when(F.col("reductions") <= low_threshold_2030, "low")
        .when((F.col("reductions") > low_threshold_2030) & (F.col("reductions") <= high_threshold_2030), "medium")
        .otherwise("high")
    )

    df_2050 = df_2050.withColumn(
        "risk_category",
        F.when(F.col("reductions") <= low_threshold_2050, "low")
        .when((F.col("reductions") > low_threshold_2050) & (F.col("reductions") <= high_threshold_2050), "medium")
        .otherwise("high")
    )

    # Joining the two dataframes together
    combined_df = df_2030.unionByName(df_2050)

    combined_df = combined_df.withColumn(
        "benchmark_group",
        F.lower(F.concat_ws("_", F.col("scenario_type"),
                F.col("scenario_name"), F.col("year")))
    )
    # Rename specific columns
    combined_df = combined_df.withColumnsRenamed(
        {"reference_product_name": "product_name", "reductions": "profile_ranking"})

    return combined_df


def sector_profile_upstream_compute(input_sector_profile_ledger_upstream_x):
    # Splitting the dataframe based on year
    df_2030 = input_sector_profile_ledger_upstream_x.filter(
        F.col("input_year") == 2030)
    df_2050 = input_sector_profile_ledger_upstream_x.filter(
        F.col("input_year") == 2050)

    # Setting different thresholds for each dataframe
    low_threshold_2030, high_threshold_2030 = 1/9, 1/3  # thresholds for 2030
    low_threshold_2050, high_threshold_2050 = 2/9, 2/3   # thresholds for 2050

    df_2030 = df_2030.withColumn(
        "risk_category",
        F.when(F.col("input_reductions") <= low_threshold_2030, "low")
        .when((F.col("input_reductions") > low_threshold_2030) & (F.col("input_reductions") <= high_threshold_2030), "medium")
        .otherwise("high")
    )

    df_2050 = df_2050.withColumn(
        "risk_category",
        F.when(F.col("input_reductions") <= low_threshold_2050, "low")
        .when((F.col("input_reductions") > low_threshold_2050) & (F.col("input_reductions") <= high_threshold_2050), "medium")
        .otherwise("high")
    )

    # Joining the two dataframes together
    combined_df = df_2030.unionByName(df_2050)

    combined_df = combined_df.withColumn(
        "benchmark_group",
        F.lower(F.concat_ws("_", F.col("input_scenario_type"),
                F.col("input_scenario_name"), F.col("input_year")))
    )
    # Rename specific columns
    combined_df = combined_df.withColumnsRenamed(
        {"input_reductions": "profile_ranking"})

    return combined_df


def ei_geography_checker(upstream_data):
    # Filter the rows with row number <= 2
    check_input_data = upstream_data.filter(
        F.col("row_num") <= 2).drop("row_num")
    # Reset the index
    check_input_data = check_input_data.withColumn(
        "index", F.monotonically_increasing_id()).drop("index")

    check_different_geo_at_same_priority = check_input_data[check_input_data["input_priority"] == 16].dropDuplicates(
        subset=["input_activity_uuid_product_uuid", "input_geography"])

    # Check if any input product belongs to different geographies at the same priority
    if check_different_geo_at_same_priority.dropDuplicates(["input_activity_uuid_product_uuid"]).count() != check_different_geo_at_same_priority.count():
        raise ValueError(
            "Any input product should not belong to different geographies at the same priority `16`")

    check_multiple_NA = check_input_data.filter(F.col("input_priority").isNull(
    )).dropDuplicates(subset=["input_activity_uuid_product_uuid", "input_product_name"])

    if check_multiple_NA.dropDuplicates(["input_activity_uuid_product_uuid"]).count() != check_multiple_NA.count():
        raise ValueError(
            "Any input product should not have more than one NA input priority for an NA input geography")


def ledger_geography_checker(upstream_data):
    # Filter the rows with row number <= 2
    check_input_data = upstream_data.filter(
        F.col("row_num") <= 2).drop("row_num")
    # Reset the index
    check_input_data = check_input_data.withColumn(
        "index", F.monotonically_increasing_id()).drop("index")

    check_different_geo_at_same_priority = check_input_data[check_input_data["input_priority"] == 16].dropDuplicates(
        subset=["input_tiltledger_id", "input_geography"])

    # Check if any input product belongs to different geographies at the same priority
    if check_different_geo_at_same_priority.dropDuplicates(["input_tiltledger_id"]).count() != check_different_geo_at_same_priority.count():
        raise ValueError(
            "Any input product should not belong to different geographies at the same priority `16`")

    if check_multiple_NA.dropDuplicates(["input_tiltledger_id"]).count() != check_multiple_NA.count():
        raise ValueError(
            "Any input product should not have more than one NA input priority for an NA input geography")


def transition_risk_compute(transition_risk_product_level_data):
    trs_product = transition_risk_product_level_data.withColumn(
        "transition_risk_score",
        F.when(
            F.isnull(transition_risk_product_level_data["average_profile_ranking"]) | F.isnull(
                transition_risk_product_level_data["profile_ranking"]),
            None
        ).otherwise(
            (transition_risk_product_level_data["average_profile_ranking"] +
             transition_risk_product_level_data["profile_ranking"]) / 2
        )
    ).withColumn(
        "benchmark_group",
        F.when(
            F.isnull(transition_risk_product_level_data["average_profile_ranking"]) | F.isnull(
                transition_risk_product_level_data["profile_ranking"]),
            None
        ).otherwise(
            F.concat_ws(
                "_", transition_risk_product_level_data["scenario_year"], transition_risk_product_level_data["benchmark_group"])
        )
    )
    trs_product = trs_product.withColumn(
        "transition_risk_score",
        F.when(
            (trs_product["transition_risk_score"] < 0), 0
        ).when(
            (trs_product["transition_risk_score"] > 1), 1
        ).otherwise(trs_product["transition_risk_score"])
    )
    return trs_product
