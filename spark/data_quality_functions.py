from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
import pyspark.sql.functions as F


def check_value_within_list(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Filter a DataFrame based on a list of values within a specific column and return the count of valid rows.

    This function takes a DataFrame and filters it to include only rows where the values in the specified
    column match any of the values in the provided 'value_list'. It then returns the count of valid rows
    that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input DataFrame to filter.
    - column_name (list): A list containing the column in the DataFrame to filter by.
    - value_list (list): A list of values to compare with the values in the specified column.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' match any
      of the values in 'value_list'.
    """
    column_name = kwargs.get('columns')
    value_list = kwargs.get('value_list')

    valid_count = dataframe.filter(
        F.col(column_name[0]).isin(value_list)).count()
    return valid_count


def calculate_filled_values(dataframe: DataFrame, spark_session: SparkSession) -> DataFrame:
    """
    Calculates the number of filled values in each column of a DataFrame.

    Args:
        dataframe (DataFrame): The input DataFrame.
        spark_session (SparkSession): The SparkSession object.

    Returns:
        DataFrame: A DataFrame containing the column name, total count, valid count, and other metadata.

    """
    df_cols = [col for col in dataframe.columns if ('map_' not in col) and (
        col not in ['from_date', 'to_date', 'RecordID'])]
    total_count = dataframe.count()
    df = dataframe.select([F.count(F.when((F.isnull(c)) | (F.col(c) == 'NA') | (
        F.col(c) == 'nan'), False)).alias(c) for c in df_cols])

    df_row = []

    for row in df.collect():
        for invalid_col in row.asDict():
            df_row.append(Row(column_name=invalid_col,
                          invalid_count=row[invalid_col]))

    df = spark_session.createDataFrame(df_row)

    df = df.withColumn('total_count', F.lit(total_count).cast(IntegerType())) \
        .withColumn('valid_count', F.lit(total_count).cast(IntegerType()) - F.col('invalid_count').cast(IntegerType())) \
        .withColumn('check_name', F.lit('Check if values are filled')) \
        .withColumn('check_id', F.lit('tilt_1')) \
        .withColumn('signalling_id', F.lit(None)) \
        .withColumnRenamed('invalid_count_column', 'column_name')
    col_order = ['signalling_id', 'check_id', 'column_name',
                 'check_name', 'total_count', 'valid_count']
    df = df.select(col_order).filter(~F.col('column_name').isin(
        ['from_date', 'to_date', 'RecordID']))

    return df


def check_values_in_range(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Filter a Spark DataFrame to include rows where values in a specified column are within a given range,
    and return the count of valid rows.

    This function takes a Spark DataFrame and filters it to include only rows where the values in the specified
    'column_name' fall within the inclusive range specified by 'range_start' and 'range_end'. It then returns
    the count of valid rows that meet this criterion.

    Parameters:
    - dataframe (DataFrame): The input Spark DataFrame to filter.
    - column_name (list): A list containing the column in the DataFrame to filter by.
    - range_start (int): The inclusive lower bound of the range for filtering.
    - range_end (int): The inclusive upper bound of the range for filtering.

    Returns:
    - valid_count (int): The count of rows in the DataFrame where the values in 'column_name' fall within
      the specified range.   
    """

    column_name = kwargs.get('columns')
    range_start = kwargs.get('range_start')
    range_end = kwargs.get('range_end')

    valid_count = dataframe.filter(
        col(column_name[0]).between(range_start, range_end)).count()

    return valid_count


def check_values_unique(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Check the uniqueness of values in a specified column of a DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame to check.
        **kwargs: Additional keyword arguments.
            column_name (str): The name of the column to check.

    Returns:
        int: The count of distinct values in the specified column.
    """

    column_name = kwargs.get('columns')

    valid_count = dataframe.select(*[F.col(col)
                                   for col in column_name]).distinct().count()
    return valid_count


def check_values_format(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Checks the format of values in a specified column of a DataFrame.

    Args:
        dataframe (DataFrame): The DataFrame to be checked.
        **kwargs: Additional keyword arguments.
            columns (list of str): The name(s) of the column(s) to check.
            format (str): The regular expression pattern to match against the values.

    Returns:
        int: The count of values in the specified column(s) that match the given format.
    """

    column_name = kwargs.get('columns')
    row_format = kwargs.get('format')

    valid_count = dataframe.filter(
        F.col(column_name[0]).rlike(row_format)).count()
    return valid_count


def check_values_consistent(dataframe: DataFrame, column_name: list, compare_df: DataFrame, join_columns: list) -> int:
    """
    Checks the consistency of values in a specified column between the input DataFrame and a comparison table.

    Args:
        spark_session (SparkSession): The SparkSession instance.
        dataframe (DataFrame): The DataFrame to be checked for consistency.
        column_name (list): A list containing the column in the DataFrame which values will be checked.
        compare_table (str): The name of the comparison table in the same SparkSession.
        join_columns (list): A list of column names used for joining the input DataFrame and the comparison table.

    Returns:
        int: The count of rows where the values in the specified column match between the two tables.

    Note:
        - This function performs a left join between the input DataFrame and the comparison table using the specified join columns.
        - It compares values in the 'column_name' from both tables.
        - Rows where the values match are counted, and the count is returned as an integer.

    """
    compare_df = compare_df.select(
        join_columns + [F.col(column_name[0]).alias('compare_' + column_name[0])])

    joined_df = dataframe.select(
        [column_name[0]] + join_columns).join(compare_df, on=join_columns, how='left')
    valid_count = joined_df.filter(
        F.col(column_name[0]) == F.col('compare_' + column_name[0])).count()

    return valid_count


def check_expected_value_count(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Check the count of rows in a DataFrame grouped by specific columns and compare it to an expected count.

    With this function we want to check if a combination of certain columns exists a certain amount of times in a table.
    For example when 6 benchmarks are expected per company and each benchmark has 3 risk levels, a total of 18 rows per company would be expected.

    Parameters:
    - spark_session (SparkSession): The Spark session.
    - dataframe (DataFrame): The input DataFrame to be analyzed.
    - groupby_columns (list): A list of column names to group the DataFrame by.
    - expected_count (int): The expected count to compare with.

    Returns:
    - int: The count of rows in the DataFrame that match the expected count after grouping.

    """
    groupby_columns = kwargs.get('columns')
    expected_count = kwargs.get('expected_count')

    groupby_columns_list = [F.col(column) for column in groupby_columns]
    valid_rows = dataframe.groupby(groupby_columns_list).agg(F.count('RecordID').alias(
        'count')).filter(F.col('count') == expected_count).select(groupby_columns_list)
    valid_count = dataframe.join(
        valid_rows, how='inner', on=groupby_columns).count()

    return valid_count


def check_expected_distinct_value_count(dataframe: DataFrame, **kwargs) -> int:
    """
    Check the count of distinct values in specific columns of a DataFrame grouped by other columns
    and compare it to an expected count.

    With this function we check the count of different values across one group. 
    For example when checking if every company has a record for all of the benchmarks, it is possible that a benchmark exists multiple times by being subdivided into multiple sub scenarios.
    At this point we then need to check the unique amount of benchmarks to get the actual amount of applied benchmarks and not rows.

    Parameters:
    - spark_session (SparkSession): The Spark session.
    - dataframe (DataFrame): The input DataFrame to be analyzed.
    - groupby_columns (list): A list of column names to group the DataFrame by.
    - expected_count (int): The expected count of distinct values to compare with.
    - distinct_columns (list): A list of column names for which distinct values will be counted.

    Returns:
    - int: The count of rows in the DataFrame that match the expected count of distinct values after grouping.

    """

    groupby_columns = kwargs.get('columns')
    expected_count = kwargs.get('expected_count')
    distinct_columns = kwargs.get('distinct_columns')

    groupby_columns_list = [F.col(column) for column in groupby_columns]
    distinct_columns_list = [F.col(column) for column in distinct_columns]
    valid_rows = dataframe.groupby(groupby_columns_list).agg(F.countDistinct(
        *distinct_columns_list).alias('count')).filter(F.col('count') == expected_count).select(groupby_columns_list)
    valid_count = dataframe.join(
        valid_rows, how='inner', on=groupby_columns).count()

    return valid_count


def column_sums_to_1(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Check if the sum of values in a specific column of a DataFrame, grouped by other columns,
    equals 1 and return the count of rows that meet this condition.

    In this check the aim is to make sure that columns like a share sum up to 100% or 1 in the case of our data. 
    Due to rounding differences in fractional shares, the implementation is to check if a sum lies between 98% and 102%.

    Parameters:
    - spark_session (SparkSession): The Spark session.
    - dataframe (DataFrame): The input DataFrame to be analyzed.
    - groupby_columns (list): A list of column names to group the DataFrame by.
    - sum_column (str): The column whose values will be summed and compared to 1.

    Returns:
    - int: The count of rows in the DataFrame where the sum of values in the 'sum_column' equals 1 after grouping.

    """
    groupby_columns = kwargs.get('columns')
    sum_column = kwargs.get('sum_column')

    groupby_columns_list = [F.col(column) for column in groupby_columns]
    valid_rows = dataframe.groupby(groupby_columns_list).agg(F.sum(sum_column).alias(
        'sum')).filter(F.col('sum').between(0.98, 1.02)).select(groupby_columns_list)
    valid_count = dataframe.join(
        valid_rows, how='inner', on=groupby_columns).count()

    return valid_count


def calculate_blocking_issues(dataframe: DataFrame, blocking_check_dict: dict) -> DataFrame:
    """
    Calculates blocking issues based on the given dataframe and blocking check dictionary.

    Args:
        dataframe (DataFrame): The input dataframe.
        blocking_check_dict (dict): A dictionary containing blocking check configurations.

    Returns:
        DataFrame: The resulting dataframe after calculating blocking issues.

    Raises:
        ValueError: If a blocking issue violation is detected.

    """
    current_df = dataframe.where(F.col('to_date') == '2099-12-31')

    total_records = current_df.count()

    for blocking_check in blocking_check_dict:

        processing_dict = {
            'values in range': check_values_in_range(current_df, **blocking_check),
            'values are unique': check_values_unique(current_df, **blocking_check),
            'values have format': check_values_format(current_df, **blocking_check)
        }

        range_test_int = processing_dict[blocking_check['check']]

        blocking_count = total_records - range_test_int

        if blocking_count != 0:
            raise ValueError(
                f"Blocking issue violation detected: {blocking_check['check']}")


def calculate_signalling_issues(dataframe: DataFrame, signalling_check_dict: dict, spark_session: SparkSession) -> DataFrame:
    """
    Calculate signalling issues based on a set of predefined checks for a given DataFrame.

    This function computes signalling issues in a DataFrame by applying a set of predefined checks,
    as specified in the `signalling_check_dict`. It generates a summary DataFrame containing the results.

    Parameters:
    - spark_session (SparkSession): The Spark session for working with Spark DataFrame.
    - dataframe (DataFrame): The input DataFrame on which the signalling checks will be applied.
    - signalling_check_dict (dict): A dictionary containing the specifications of signalling checks.
    - signalling_check_dummy (DataFrame): A template DataFrame for constructing the result.

    Returns:
    - DataFrame: A summary DataFrame with signalling issues, including the following columns:
        - 'column_name': The name of the column being checked.
        - 'check_name': The type of check being performed.
        - 'total_count': The total number of rows in the DataFrame.
        - 'valid_count': The count of valid values after applying the check.
        - 'check_id': A constant identifier for the specific check.

    Signalling Check Types:
    - 'values within list': Checks if values are within a specified list.
    - 'values in range': Checks if values are within a specified numerical range.
    - 'values are unique': Checks if values in the column are unique.
    - 'values have format': Checks if values match a specified format.
    - 'values are consistent': Checks if values in the column are consistent with values in another table.

    Note: Additional columns will be added based on the specific check type.
    """

    df = calculate_filled_values(dataframe, spark_session)
    total_count = dataframe.count()

    for signalling_check in signalling_check_dict:
        check_types = signalling_check.get('check')
        column_name = signalling_check.get('columns')

        if check_types == 'values within list':

            value_list = signalling_check.get('value_list')
            valid_count = check_value_within_list(
                dataframe, **signalling_check)
            input_list = '","'.join([str(val) for val in value_list])[:100]
            description_string = f'values within list of: "{input_list}"'
            check_id = 'tilt_2'

        elif check_types == 'values in range':

            range_start = signalling_check.get('range_start')
            range_end = signalling_check.get('range_end')
            valid_count = check_values_in_range(
                dataframe, **signalling_check)
            description_string = f'values between {str(range_start)} and {str(range_end)}'
            check_id = 'tilt_3'

        elif check_types == 'values are unique':

            valid_count = check_values_unique(dataframe, **signalling_check)
            description_string = f"unique values in column `{column_name}`"
            check_id = 'tilt_4'

        elif check_types == 'values have format':

            check_format = signalling_check.get('format')
            valid_count = check_values_format(
                dataframe, **signalling_check)
            description_string = f'values have format {check_format}'
            check_id = 'tilt_5'

        # elif check_types == 'values are consistent':

        #     table_to_compare = signalling_check.get('compare_table')
        #     columns_to_join = signalling_check.get('join_columns')
        #     df_to_compare = read_table(spark_session, table_to_compare)
        #     valid_count = check_values_consistent(spark_session,dataframe, column_name, df_to_compare, columns_to_join)
        #     input_list = '","'.join([str(val) for val in columns_to_join])[:100]
        #     description_string = f'values are consistent with column(s) "{input_list}" from table {table_to_compare}'
        #     check_id = 'tilt_6'

        elif check_types == 'values occur as expected':

            count_expected = signalling_check.get('expected_count')
            valid_count = check_expected_value_count(
                dataframe, **signalling_check)
            description_string = f'values occur {count_expected} times'
            check_id = 'tilt_7'

        elif check_types == 'values sum to 1':

            sum_col = signalling_check.get('sum_column')
            valid_count = column_sums_to_1(
                dataframe, **signalling_check)
            description_string = f'values in column "{sum_col}" sum to 1'
            check_id = 'tilt_8'

        elif check_types == 'distinct values occur as expected':

            count_expected = signalling_check.get('expected_count')
            distinct_columns = signalling_check.get('distinct_columns')
            valid_count = check_expected_distinct_value_count(
                dataframe, **signalling_check)
            input_list = '","'.join([str(val)
                                    for val in distinct_columns])[:100]
            description_string = f'{count_expected} distinct values occur in column {input_list}'
            check_id = 'tilt_9'

        df_row = [Row(signalling_id=1, check_id=check_id, column_name=','.join(
            column_name), check_name=description_string, total_count=total_count, valid_count=valid_count)]
        signalling_check_df = spark_session.createDataFrame(df_row, StructType([
            StructField('signalling_id', IntegerType(), False),
            StructField('check_id', StringType(), False),
            StructField('column_name', StringType(), True),
            StructField('check_name', StringType(), True),
            StructField('total_count', IntegerType(), True),
            StructField('valid_count', IntegerType(), True)
        ]
        )).withColumn('signalling_id', F.lit(None))

        df = df.union(signalling_check_df).select(
            ['signalling_id', 'check_id', 'column_name', 'check_name', 'total_count', 'valid_count'])

    return df
