from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from time import time


from spark.dataframe_helpers import (
    create_sha_values,
    create_catalog_schema,
    create_catalog_table_owner,
    apply_scd_type_2,
    assign_signalling_id,
)
from spark.data_quality_functions import (
    calculate_signalling_issues,
    calculate_blocking_issues,
)

from spark.database import get_table_definition
from spark.data_readers import DataReader


class CustomDF(DataReader):
    """
    A custom DataFrame class that wraps a PySpark DataFrame for additional functionality, like data tracing, quality checks and slowly changing dimensions.

    This class provides a convenient way to work with PySpark DataFrames. It includes methods for reading data from a table, writing data to a table, and performing various transformations on the data.

    Attributes:
        _name (str): The name of the table.
        _spark_session (SparkSession): The SparkSession object.
        _schema (dict): The schema of the table, retrieved from the get_table_definition function.
        _partition_name (str): The name of the partition column in the table.
        _history (str): The history setting for the table, either 'complete' or 'recent'.
        _env (str): The environment setting, default is 'develop'.
        _path (str): The path to the table.
        _df (DataFrame): The PySpark DataFrame containing the table data.

    Methods:
        table_path(): Builds the path for a table based on the container, location, and optional partition.
        read_table(): Reads data from a table based on the schema type and applies various transformations.
        write_table(): Writes data from the DataFrame to a table.
    """

    def __init__(
        self,
        table_name: str,
        spark_session: SparkSession,
        initial_df: DataFrame = None,
        partition_name: str = "",
        history: str = "recent",
    ):
        self._name = table_name
        self._spark_session = spark_session
        self._layer = table_name.split("_")[-1] + "_layer"
        self._schema = get_table_definition(self._layer, self._name)
        self._partition_name = partition_name
        self._history = history
        self._env = "develop"
        self._salt = str(time())[-10:].replace(".", "")
        DataReader.__init__(
            self,
            self._spark_session,
            self._env,
            self._schema,
            self._partition_name,
            self._history,
        )
        if initial_df:
            self._df = initial_df
        else:
            self._df = self.read_source()

        if self._df.select(self._df.colRegex(r"`^map_.*`")).columns:
            self._df = self._df.withColumnRenamed(
                self._df.select(self._df.colRegex(r"`^map_.*`")).columns[0], f'map_{self._name}_{self._salt}')
            self.map_col = f'map_{self._name}_{self._salt}'
        else:
            self.map_col = None

    def rename_columns(self, rename_dict):
        for name in rename_dict:
            if name in self._df.columns:
                pass
            else:
                raise ValueError(f"Value {name} does not exist in the specified schema")
        self._df = self._df.withColumnsRenamed(rename_dict)

    def compare_tables(self):
        """
        Compare an incoming DataFrame with an existing table, identifying new, identical, and closed records.

        This function compares an incoming DataFrame with an existing table and identifies the following types of records:
        - New records: Records in the incoming DataFrame that do not exist in the existing table.
        - Identical records: Records with unchanged values present in both the incoming and existing table.
        - Closed records: Records that exist in the existing table but are no longer present in the incoming DataFrame.

        The comparison is based on SHA values of selected columns, and the function returns a DataFrame containing all relevant records.

        Note: The function assumes that the incoming DataFrame and existing table have a common set of columns for comparison.

        Returns:
            DataFrame: A DataFrame containing records that are new, identical, or closed compared to the existing table.
        """

        # Read the already existing table
        old_df = CustomDF(
            self._name, self._spark_session, None, self._partition_name, "complete"
        )

        all_records = apply_scd_type_2(self._df, old_df.data)

        return all_records

    def validate_table_format(self):
        """
        Validates the format of a DataFrame against the specified table definition.

        This function checks if the DataFrame's structure matches the table definition, if the first row of the DataFrame matches the first row of the table, and if all rows in the DataFrame are unique. If any of these checks fail, a ValueError is raised.

        Args:
            None. The method uses the instance's SparkSession, DataFrame, and table name.

        Returns:
            bool: True if the DataFrame format is valid, False otherwise.

        Raises:
            ValueError: If the initial structure does not match or the head of the table does not match,
                        or if the data quality rules on a column are violated.
        """
        # Create an empty DataFrame with the table definition columns
        if self._schema["partition_column"]:
            check_df = self._spark_session.createDataFrame([], self._schema["columns"])
            check_df = check_df.withColumn(
                self._schema["partition_column"], F.lit(self._partition_name)
            )
        else:
            check_df = self._spark_session.createDataFrame([], self._schema["columns"])

        try:
            # Union the empty DataFrame with the provided DataFrame
            check_df = check_df.union(self._df)
            check_df.head()
        except Exception as e:
            # An exception occurred, indicating a format mismatch
            raise ValueError(
                "The initial structure can not be joined, because: " + str(e)
            ) from e

        # Compare the first row of the original DataFrame with the check DataFrame
        if (
            not self._df.orderBy(F.col("RecordID")).head().asDict()
            == check_df.orderBy(F.col("RecordID")).head().asDict()
        ):
            # The format of the DataFrame does not match the table definition
            raise ValueError("The head of the table does not match.")

        # Check if all of the rows are unique in the table
        
        if self._df.count() != self._df.distinct().count():
            # The format of the DataFrame does not match the table definition
            raise ValueError("Not all rows in the table are unqiue")

        # Perform additional quality checks on specific columns
        self.check_blocking_issues()

        return True

    def add_record_id(self) -> DataFrame:
        """
        Computes SHA-256 hash values for each row in the DataFrame and adds the hash as a new column 'RecordID'.

        This method computes SHA-256 hash values for each row in the DataFrame based on the values in all columns,
        excluding 'RecordID' and 'to_date'. The computed hash is then appended as a new column called 'RecordID'
        to the DataFrame. The order of columns in the DataFrame will affect the generated hash value.

        If a partition column is specified in the schema, the method ensures that it is the rightmost column in the DataFrame.

        Returns:
            pyspark.sql.DataFrame: A new DataFrame with an additional 'RecordID' column, where each row's
            value represents the SHA-256 hash of the respective row's contents.
        """
        # Select all columns that are needed for the creation of a record ID
        sha_columns = [
            F.col(col_name)
            for col_name in self._df.columns
            if col_name not in ["RecordID", "to_date"] and "map_" not in col_name
        ]

        # Create the SHA256 record ID by concatenating all relevant columns
        data_frame = create_sha_values(self._df, sha_columns)
        data_frame = data_frame.withColumnRenamed("shaValue", "RecordID")

        # Reorder the columns, to make sure the partition column is the most right column in the data frame
        if self._partition_name:
            col_order = [
                x
                for x in data_frame.columns
                if x not in ["RecordID", self._schema["partition_column"]]
            ] + ["RecordID", self._schema["partition_column"]]
        else:
            col_order = [x for x in data_frame.columns if x not in ["RecordID"]] + [
                "RecordID"
            ]

        data_frame = data_frame.select(col_order)

        return data_frame

    def check_blocking_issues(self):

        blocking_checks = self._schema["quality_checks"]

        calculate_blocking_issues(self._df, blocking_checks)

    def check_signalling_issues(self):
        """
        Perform signalling checks on a specified table and update the monitoring values table.

        This function performs signalling checks on a given table by executing various data quality checks as defined in
        the 'signalling_checks_dictionary'. The results of these checks are recorded in the 'monitoring_values' table.

        Returns:
            None. The method updates the 'monitoring_values' table in-place.

        Note:
            - Signalling checks are defined in the 'signalling_checks_dictionary'.
            - The function reads the specified table and calculates filled values using the 'calculate_filled_values' function.
            - It then iterates through the signalling checks, records the results, and updates the 'monitoring_values' table.
            - The specific checks performed depend on the definitions in the 'signalling_checks_dictionary'.
        """

        # Force to read the table again, to make sure that the table is not empty and we can calculate the signalling issues
        base_monitoring_df = self.read_source()

        signalling_checks = {}
        # Check if there are additional data quality monitoring checks to be executed
        if self._name in signalling_checks_dictionary.keys():
            signalling_checks = signalling_checks_dictionary[self._name]

        # Generate the monitoring values table to be written
        monitoring_values_df = calculate_signalling_issues(
            base_monitoring_df, signalling_checks, self._spark_session
        )
        monitoring_values_df = monitoring_values_df.withColumn(
            "table_name", F.lit(self._name)
        )

        existing_monitoring_df = CustomDF(
            "monitoring_values",
            self._spark_session,
            partition_name=self._name,
            history="complete",
        )

        monitoring_values_df = assign_signalling_id(
            monitoring_values_df, existing_monitoring_df.data
        )

        # Write the table to the location
        complete_monitoring_partition_df = CustomDF(
            "monitoring_values",
            self._spark_session,
            partition_name=self._name,
            initial_df=monitoring_values_df,
        )
        complete_monitoring_partition_df.write_table()

    def write_table(self):
        """
        Writes the DataFrame to a table with the specified table name and optional partition.

        This method writes the DataFrame to a table in the Hive catalog. If a partition is specified, the DataFrame is written to that partition of the table. If the table does not exist, it is created.

        Returns:
            None. The method writes the DataFrame to a table in-place.

        Raises:
            ValueError: If the DataFrame is empty or if the table name is not specified.

        Note:
            - The DataFrame is written in the Delta format.
            - If a partition is specified, the DataFrame is written to that partition of the table.
            - If the table does not exist, it is created.
        """

        # Compare the newly created records with the existing tables
        self._df = self.compare_tables()

        # Add the SHA value to create a unique ID within tilt
        self._df = self.add_record_id()

        # Since the map column is only available starting from the raw layer, we can not dump/write it when writing to the raw layer.
        if self._schema["container"] not in ["landingzone", "monitoring", "raw"]:
            self.dump_map_column()

        table_check = self.validate_table_format()

        if table_check:
            if self._schema["partition_column"]:
                self._df.write.partitionBy(self._schema["partition_column"]).mode(
                    "overwrite"
                ).format("parquet").save(self._data_path)
            else:
                self._df.write.mode("overwrite").format("parquet").save(self._data_path)
        else:
            raise ValueError("Table format validation failed.")

        if self._name != "monitoring_values":
            self.check_signalling_issues()

    def dump_map_column(self):
        """
        Dumps the map column from the DataFrame to a Delta table.

        This method creates a Delta table and dumps the map column from the DataFrame into it.
        The process happens when writing the table, so the map column is dumped to the record_trace table.
        Here we use the new recordID of the generated record and explode the valus in the map column.
        This process also include the creation and generation of the record trace table in case it does not exist yet.

        Returns:
            None
        """
        # Define the table name using the environment variable
        table_name = f"{self._env}.monitoring.record_tracing"

        # Get the schema for the "record_trace" table
        trace_schema = get_table_definition(self._layer, "record_trace")

        # Generate SQL strings to create the schema and set the owner of the schema
        create_schema_string, set_owner_schema_string = create_catalog_schema(
            self._env, trace_schema
        )

        # Generate SQL strings to create the table and set the owner of the table
        create_string = create_catalog_table(table_name, trace_schema)
        set_owner_string = create_catalog_table_owner(table_name)

        # Execute the SQL strings
        self._spark_session.sql(create_schema_string)
        self._spark_session.sql(set_owner_schema_string)
        self._spark_session.sql(create_string)
        self._spark_session.sql(set_owner_string)

        # Find the map_column

        # Filter the DataFrame to only include rows that are newly created. Only for the new rows we want to dump the new record traces.
        dump_df = self._df.where(F.col("to_date") == "2099-12-31")

        # Add a new column 'target_table_name' with the name of the table, and rename 'RecordID' to 'target_RecordID'
        dump_df = dump_df.withColumn(
            "target_table_name", F.lit(self._name)
        ).withColumnRenamed("RecordID", "target_RecordID")

        # Explode the map column to be able to write the array format to a standard sql table
        dump_df = dump_df.select('*', F.explode(self.map_col).alias('source_table_name', 'source_RecordID_list')
                                 ).select('*', F.explode(F.col('source_RecordID_list')).alias('source_RecordID'))

        # Select only the columns 'source_RecordID', 'source_table_name', 'target_RecordID', and 'target_table_name'
        dump_df = dump_df.select(
            F.col("source_RecordID"),
            F.col("source_table_name"),
            F.col("target_RecordID"),
            F.col("target_table_name"),
        )

        # Read the existing record tracing table into a DataFrame
        df = self._spark_session.read.format("delta").table(table_name)

        # Filter the DataFrame to only include rows where for the current table
        df = df.filter(F.col("target_table_name") == self._name)

        # Eliminate the duplicate records from dump_df
        dump_df = dump_df.drop_duplicates()

        # Union the two DataFrames and write the result to the table, partitioned by the name of the table
        dump_df.union(df).distinct().write.partitionBy("target_table_name").mode(
            "overwrite"
        ).format("delta").saveAsTable(table_name)

        # Drop the map column from the original DataFrame
        self._df = self._df.drop(F.col(self.map_col))

    def convert_data_types(self, column_list: list, data_type):
        """
        Converts the data type of specified columns in the DataFrame.

        Args:
            column_list (list): A list of column names in the DataFrame for which the data type needs to be converted.
            data_type (DataType): The target data type to which the column data should be converted.

        Returns:
            None. The method updates the DataFrame in-place.
        """
        for column in column_list:
            self._df = self._df.withColumn(column, F.col(column).cast(data_type))

    def custom_join(
        self, custom_other: "CustomDF", custom_on: str = None, custom_how: str = None
    ):
        """
        Joins the current CustomDF instance with another CustomDF instance based on the provided conditions.

        Args:
            other (CustomDF): The other CustomDF instance to join with.
            custom_on (str, optional): The column to join on. Defaults to None.
            custom_how (str, optional): Type of join (inner, outer, left, right). Defaults to "inner".

        Raises:
            Exception: If the `custom_on` column does not exist in both dataframes.

        Returns:
            CustomDF: A new CustomDF instance that is the result of the join.
        """

        if not custom_how:
            raise ValueError(
                "Please specify the type of join (inner, outer, left, right)"
            )

        copy_self_df = self._df
        copy_other_df = custom_other.data
        copy_df = copy_self_df.join(copy_other_df, on=custom_on, how=custom_how)
        copy_df = copy_df.withColumn(
            'map_temp', F.create_map().cast('map<string,array<String>>'))
        copy_df = copy_df.withColumn(self.map_col, F.coalesce(
            F.col(self.map_col), F.col('map_temp')))
        copy_df = copy_df.withColumn(custom_other.map_col, F.coalesce(
            F.col(custom_other.map_col), F.col('map_temp')))
        copy_df = copy_df.drop(F.col('map_temp'))
        copy_df = copy_df.withColumn(
            self.map_col, F.map_zip_with(
                self.map_col, custom_other.map_col, lambda k, v1, v2: F.when(v1.isNull(), v2).when(v2.isNull(), v1).otherwise(F.array_union(v1, v2)))
        )
        copy_df = copy_df.drop(
            F.col(custom_other.map_col))

        return CustomDF(
            self._name,
            self._spark_session,
            copy_df,
            self._partition_name,
            self._history,
        )

    def custom_select(self, columns: list):
        """
        Selects the specified columns from the DataFrame. If the DataFrame is not coming from the landing zone, the existence of the map column is assumed.

        Args:
            columns (list): A list of column names to select.

        Returns:
            CustomDF: A new CustomDF object with the selected columns.
        """
        copy_df = self._df
        if self._schema["container"] not in ["landingzone"]:
            copy_df = copy_df.select(*columns, F.col(f"map_{self._name}_{self._salt}"))
        else:
            copy_df = copy_df.select(*columns)

        return CustomDF(
            self._name,
            self._spark_session,
            copy_df,
            self._partition_name,
            self._history,
        )

    def custom_distinct(self):
        """
        Returns the distinct rows from the DataFrame.

        Returns:
            CustomDF: A new CustomDF instance with distinct rows.
        """
        replace_na_value = str(time())
        copy_df = self._df
        copy_df = copy_df.fillna(replace_na_value)
        cols = [F.col(col) for col in self._df.columns if not col.startswith("map_")]

        copy_df = copy_df.select(*cols, F.explode(
            F.col(self.map_col)).alias('exploded_table', 'exploded_list'))\
            .select(*cols, F.col('exploded_table'), F.explode(F.col('exploded_list')).alias('exploded'))

        copy_df = copy_df\
            .groupBy(cols + ['exploded_table'])\
            .agg(F.collect_set(F.col('exploded')).alias('exploded_fold'))\
            .groupBy(cols)\
            .agg(F.collect_list(F.col('exploded_table')).alias('table_list'), F.collect_list(F.col('exploded_fold')).alias('fold_list'))\
            .withColumn(self.map_col, F.map_from_arrays(F.col('table_list'), F.col('fold_list')))\
            .select(*cols, F.col(self.map_col))

        copy_df = copy_df.replace(replace_na_value, None)
        return CustomDF(
            self._name,
            self._spark_session,
            copy_df,
            self._partition_name,
            self._history,
        )

    def custom_average(self, groupby_columns: list, average_column: str) -> 'CustomDF':

        copy_df = self._df

        average_df = copy_df.groupBy(groupby_columns).agg(
            F.avg(F.col(average_column)).alias(average_column))

        copy_df = copy_df.select(*groupby_columns, F.explode(
            F.col(self.map_col)).alias('exploded_table', 'exploded_list'))\
            .select(*groupby_columns, F.col('exploded_table'), F.explode(F.col('exploded_list')).alias('exploded'))

        copy_df = copy_df\
            .groupBy(groupby_columns + ['exploded_table'])\
            .agg(F.collect_set(F.col('exploded')).alias('exploded_fold'))\
            .groupBy(groupby_columns)\
            .agg(F.collect_list(F.col('exploded_table')).alias('table_list'), F.collect_list(F.col('exploded_fold')).alias('fold_list'))\
            .withColumn(self.map_col, F.map_from_arrays(F.col('table_list'), F.col('fold_list')))\
            .select(*groupby_columns, F.col(self.map_col))

        copy_df = average_df.alias('average_df')\
            .join(copy_df.alias('copy_df'), on=[F.col('average_df.'+column) == F.col('copy_df.'+column) for column in groupby_columns], how='inner')\
            .select([F.col('average_df.'+column) for column in average_df.columns]+[F.col('copy_df.'+self.map_col)])

        return CustomDF(self._name, self._spark_session, copy_df, self._partition_name, self._history)

    def custom_groupby(self, groupby_columns: list, *arguments) -> 'CustomDF':
        """
        Performs a custom groupby operation on the DataFrame.

        Args:
            groupby_columns (list): A list of column names to group by.
            *arguments: Additional arguments to be passed to the `agg` function, like sum, average. These functions should be written using the F representation for the average functions

        Returns:
            CustomDF: A new instance of the CustomDF class representing the grouped DataFrame.

        Example:
            CustomDF.custom_groupby(['groupby_col_1','groupby_col_2'],F.sum(F.col('col_to_sum_over')),F.avg(F.col('avg_col')))

        """

        copy_df = self._df

        group_df = copy_df.groupBy(groupby_columns).agg(*arguments)

        copy_df = copy_df.select(*groupby_columns, F.explode(
            F.col(self.map_col)).alias('exploded_table', 'exploded_list'))\
            .select(*groupby_columns, F.col('exploded_table'), F.explode(F.col('exploded_list')).alias('exploded'))

        copy_df = copy_df\
            .groupBy(groupby_columns + ['exploded_table'])\
            .agg(F.collect_set(F.col('exploded')).alias('exploded_fold'))\
            .groupBy(groupby_columns)\
            .agg(F.collect_list(F.col('exploded_table')).alias('table_list'), F.collect_list(F.col('exploded_fold')).alias('fold_list'))\
            .withColumn(self.map_col, F.map_from_arrays(F.col('table_list'), F.col('fold_list')))\
            .select(*groupby_columns, F.col(self.map_col))

        copy_df = group_df.alias('group_df')\
            .join(copy_df.alias('copy_df'), on=[F.col('group_df.'+column).eqNullSafe(F.col('copy_df.'+column)) for column in groupby_columns], how='inner')\
            .select([F.col('group_df.'+column) for column in group_df.columns]+[F.col('copy_df.'+self.map_col)])

        return CustomDF(self._name, self._spark_session, copy_df, self._partition_name, self._history)

    def custom_union(self, custom_other: 'CustomDF'):
        """
        Unions the current CustomDF instance with another CustomDF instance.

        Args:
            other (CustomDF): The other CustomDF instance to union with.

        Returns:
            CustomDF: A new CustomDF instance that is the result of the union.
        """

        cols = [F.col(col)
                for col in self._df.columns if col != self.map_col]

        copy_df = custom_other.data
        copy_df = copy_df.withColumnRenamed(
            custom_other.map_col, self.map_col)
        copy_df = self._df.unionAll(copy_df)

        df = (
            copy_df.select(*cols, F.explode(
                F.col(self.map_col)).alias('exploded_table', 'exploded_list'))
            .select(*cols, F.col('exploded_table'), F.explode(F.col('exploded_list')).alias('exploded'))
        )

        self._df = (
            df.groupBy(cols + ["exploded_table"])
            .agg(F.collect_set(F.col("exploded")).alias("exploded_fold"))
            .groupBy(cols)
            .agg(F.collect_list(F.col('exploded_table')).alias('table_list'), F.collect_list(F.col('exploded_fold')).alias('fold_list'))
            .withColumn(self.map_col, F.map_from_arrays(F.col('table_list'), F.col('fold_list')))
            .select(*cols, F.col(self.map_col))
        )

        return CustomDF(
            self._name,
            self._spark_session,
            self._df,
            self._partition_name,
            self._history,
        )

    def custom_drop(self, columns: list):
        """
        Drops the specified columns from the DataFrame.

        Args:
            columns (list): A list of column names to drop from the DataFrame.

        Returns:
            CustomDF: A new CustomDF instance with the specified columns dropped.
        """
        copy_df = self._df
        copy_df = copy_df.drop(*columns)
        return CustomDF(
            self._name,
            self._spark_session,
            copy_df,
            self._partition_name,
            self._history,
        )

    @property
    def data(self):
        """
        Returns the Spark DataFrame associated with the CustomDF instance.

        Returns:
            DataFrame: The Spark DataFrame associated with the CustomDF instance.
        """
        return self._df

    @data.setter
    def data(self, new_df: DataFrame):
        """
        Sets the DataFrame associated with the CustomDF instance.

        This method allows you to replace the DataFrame associated with the CustomDF instance with a new DataFrame.

        Args:
            new_df (DataFrame): The new DataFrame to associate with the CustomDF instance.

        Returns:
            None. The method replaces the DataFrame in-place.

        Note:
            - The new DataFrame should have the same schema as the original DataFrame.
            - The new DataFrame replaces the original DataFrame in-place.
        """
        self._df = new_df

    @property
    def name(self):
        """
        Returns the name of the CustomDF instance.

        Returns:
            str: The name of the CustomDF instance.
        """
        return self._name
