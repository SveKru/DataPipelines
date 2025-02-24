import polars as pl
from polars import DataFrame
from typing import List, Dict, Any


def check_value_within_list(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Filter a DataFrame based on a list of values within a specific column and return the count of valid rows.
    """
    column_name = kwargs.get('columns')
    value_list = kwargs.get('value_list')

    valid_count = dataframe.filter(pl.col(column_name[0]).is_in(value_list)).height
    return valid_count


def calculate_filled_values(dataframe: DataFrame) -> DataFrame:
    """
    Calculates the number of filled values in each column of a DataFrame.
    """
    df_cols = [col for col in dataframe.columns if ('map_' not in col) and (
        col not in ['from_date', 'to_date', 'RecordID'])]
    total_count = dataframe.height

    # Calculate null counts for each column
    results = []
    for c in df_cols:
        if dataframe.schema[c] == pl.String:
            invalid_count = dataframe.filter(
                (pl.col(c).is_null()) | (pl.col(c) == 'NA') | (pl.col(c) == 'nan')
            ).height
        elif dataframe.schema[c] == pl.Decimal(25,10):
            invalid_count = dataframe.filter(
                (pl.col(c).is_null()) 
            ).height
        elif dataframe.schema[c].is_numeric():
            invalid_count = dataframe.filter(
                (pl.col(c).is_nan()) | (pl.col(c).is_null()) 
            ).height
        else:
            invalid_count = dataframe.filter(
                (pl.col(c).is_null()) 
            ).height
        
        results.append({
            'column_name': c,
            'check_id': 'dq_1',
            'check_name': 'Check if values are filled',
            'total_count': total_count,
            'valid_count': total_count - invalid_count
        })

    df = pl.DataFrame(results).with_columns(pl.lit(None).cast(pl.Int64).alias('signalling_id'))
    col_order = ['signalling_id', 'check_id', 'column_name',
                 'check_name', 'total_count', 'valid_count']
    df = df.select(col_order).filter(
        ~pl.col('column_name').is_in(['from_date', 'to_date', 'RecordID'])
    )

    return df


def check_values_in_range(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Filter a DataFrame to include rows where values in a specified column are within a given range.
    """
    column_name = kwargs.get('columns')
    range_start = kwargs.get('range_start')
    range_end = kwargs.get('range_end')

    valid_count = dataframe.filter(
        pl.col(column_name[0]).is_between(range_start, range_end)
    ).height

    return valid_count


def check_values_unique(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Check the uniqueness of values in specified columns of a DataFrame.
    """
    column_name = kwargs.get('columns')
    valid_count = dataframe.select(column_name).unique().height
    return valid_count


def check_values_format(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Checks the format of values in a specified column of a DataFrame using regex.
    """
    column_name = kwargs.get('columns')
    row_format = kwargs.get('format')

    valid_count = dataframe.filter(
        pl.col(column_name[0]).str.contains(row_format)
    ).height
    return valid_count


def check_values_consistent(dataframe: DataFrame, column_name: List[str], compare_df: DataFrame, join_columns: List[str]) -> int:
    """
    Checks the consistency of values in specified columns between two DataFrames.
    """
    compare_df = compare_df.select(
        join_columns + [pl.col(column_name[0]).alias(f'compare_{column_name[0]}')]
    )

    joined_df = dataframe.select(
        [column_name[0]] + join_columns
    ).join(compare_df, on=join_columns, how='left')
    
    valid_count = joined_df.filter(
        pl.col(column_name[0]) == pl.col(f'compare_{column_name[0]}')
    ).height

    return valid_count


def check_expected_value_count(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Check if groups have the expected count of rows.
    """
    groupby_columns = kwargs.get('columns')
    expected_count = kwargs.get('expected_count')

    valid_rows = (
        dataframe.group_by(groupby_columns)
        .agg(pl.count('RecordID').alias('count'))
        .filter(pl.col('count') == expected_count)
        .select(groupby_columns)
    )
    
    valid_count = dataframe.join(
        valid_rows, on=groupby_columns, how='inner'
    ).height

    return valid_count


def check_expected_distinct_value_count(dataframe: DataFrame, **kwargs) -> int:
    """
    Check if groups have the expected count of distinct values.
    """
    groupby_columns = kwargs.get('columns')
    expected_count = kwargs.get('expected_count')
    distinct_columns = kwargs.get('distinct_columns')

    valid_rows = (
        dataframe.group_by(groupby_columns)
        .agg([pl.n_unique(col).alias('count') for col in distinct_columns])
        .filter(pl.col('count') == expected_count)
        .select(groupby_columns)
    )
    
    valid_count = dataframe.join(
        valid_rows, on=groupby_columns, how='inner'
    ).height

    return valid_count


def column_sums_to_1(dataframe: DataFrame, **kwargs: dict) -> int:
    """
    Check if the sum of values in groups equals approximately 1 (between 0.98 and 1.02).
    """
    groupby_columns = kwargs.get('columns')
    sum_column = kwargs.get('sum_column')

    valid_rows = (
        dataframe.group_by(groupby_columns)
        .agg(pl.sum(sum_column).alias('sum'))
        .filter((pl.col('sum') >= 0.98) & (pl.col('sum') <= 1.02))
        .select(groupby_columns)
    )
    
    valid_count = dataframe.join(
        valid_rows, on=groupby_columns, how='inner'
    ).height

    return valid_count

def record_has_expected_history(dataframe: DataFrame, **kwargs: dict):

    expected_trace = kwargs.get('expected_trace')
    record_trace_table = kwargs.get('record_trace_table')

    record_trace_table = record_trace_table.join(dataframe.select('RecordID'),left_on=['target_RecordID'],right_on=['RecordID'],how='inner')
    record_trace_table = record_trace_table.group_by('target_RecordID','source_table_name').agg(pl.len().alias('amount_records'))
    print(record_trace_table.glimpse())
    expected_df = pl.DataFrame(expected_trace).join(record_trace_table.select('target_RecordID'),how='cross').select(['target_RecordID','source_table_name','amount_records'])

    invalid_count = record_trace_table.join(expected_df,on=['target_RecordID','source_table_name','amount_records'],how='anti').select('target_RecordID').unique().height

    return invalid_count


def calculate_blocking_issues(dataframe: DataFrame, blocking_check_dict: List[Dict[str, Any]]) -> None:
    """
    Calculate blocking issues and raise ValueError if any are found.
    """
    current_df = dataframe.filter(pl.col('to_date') == pl.date(2099,12,31))
    total_records = current_df.height

    for blocking_check in blocking_check_dict:
        processing_dict = {
            'values in range': check_values_in_range,
            'values are unique': check_values_unique,
            'values have format': check_values_format
        }

        check_func = processing_dict[blocking_check['check']]
        range_test_int = check_func(current_df, **blocking_check)
        blocking_count = total_records - range_test_int

        if blocking_count != 0:
            raise ValueError(
                f"Blocking issue violation detected: {blocking_check['check']}")


def calculate_signalling_issues(dataframe: DataFrame, signalling_check_dict: List[Dict[str, Any]]) -> DataFrame:
    """
    Calculate signalling issues based on various checks.
    """
    df = calculate_filled_values(dataframe)
    total_count = dataframe.height
    results = []

    for signalling_check in signalling_check_dict:
        check_types = signalling_check.get('check')
        column_name = signalling_check.get('columns')
        valid_count = None
        description_string = None
        check_id = None

        if check_types == 'values within list':
            value_list = signalling_check.get('value_list')
            valid_count = check_value_within_list(dataframe, **signalling_check)
            input_list = '","'.join([str(val) for val in value_list])[:100]
            description_string = f'values within list of: "{input_list}"'
            check_id = 'dq_2'

        elif check_types == 'values in range':
            range_start = signalling_check.get('range_start')
            range_end = signalling_check.get('range_end')
            valid_count = check_values_in_range(dataframe, **signalling_check)
            description_string = f'values between {str(range_start)} and {str(range_end)}'
            check_id = 'dq_3'

        elif check_types == 'values are unique':
            valid_count = check_values_unique(dataframe, **signalling_check)
            description_string = f"unique values in column `{','.join(column_name)}`"
            check_id = 'dq_4'

        elif check_types == 'values have format':
            check_format = signalling_check.get('format')
            valid_count = check_values_format(dataframe, **signalling_check)
            description_string = f'values have format {check_format}'
            check_id = 'dq_5'

        elif check_types == 'values occur as expected':
            count_expected = signalling_check.get('expected_count')
            valid_count = check_expected_value_count(dataframe, **signalling_check)
            description_string = f'values occur {count_expected} times'
            check_id = 'dq_7'

        elif check_types == 'values sum to 1':
            sum_col = signalling_check.get('sum_column')
            valid_count = column_sums_to_1(dataframe, **signalling_check)
            description_string = f'values in column "{sum_col}" sum to 1'
            check_id = 'dq_8'

        elif check_types == 'distinct values occur as expected':
            count_expected = signalling_check.get('expected_count')
            distinct_columns = signalling_check.get('distinct_columns')
            valid_count = check_expected_distinct_value_count(dataframe, **signalling_check)
            input_list = '","'.join([str(val) for val in distinct_columns])[:100]
            description_string = f'{count_expected} distinct values occur in column {input_list}'
            check_id = 'dq_9'


        elif check_types == 'record has expected history':
            table_name = signalling_check.get('table_name')
            signalling_check['record_trace_table'] = pl.read_parquet(rf'data\development\record_trace_table\table_name={table_name}')
            valid_count = total_count - record_has_expected_history(dataframe, **signalling_check)
            description_string = f'values have the expected record trace'
            check_id = 'dq_10'

        if valid_count is not None:
            results.append({
                'signalling_id': None,
                'check_id': check_id,
                'column_name': ','.join(column_name),
                'check_name': description_string,
                'total_count': total_count,
                'valid_count': valid_count
            })

    if results:
        signalling_check_df = pl.DataFrame(results)
        df = pl.concat([df, signalling_check_df], how='vertical').select(
            ['signalling_id', 'check_id', 'column_name', 'check_name', 'total_count', 'valid_count']
        )

    return df