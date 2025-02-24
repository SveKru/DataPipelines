import polars as pl
import pytest
from datetime import date
from polars.testing import assert_frame_equal
from polars_src.data_quality_functions import (
    check_value_within_list,
    calculate_filled_values,
    check_values_in_range,
    check_values_unique,
    check_values_format,
    check_values_consistent,
    check_expected_value_count,
    check_expected_distinct_value_count,
    column_sums_to_1,
    record_has_expected_history,
    calculate_blocking_issues,
    calculate_signalling_issues
)

def test_check_value_within_list():
    dataframe = pl.DataFrame({
        'col1': ['a', 'b', 'c', 'd', 'e']
    })
    kwargs = {
        'columns': ['col1'],
        'value_list': ['a', 'b', 'c']
    }
    result = check_value_within_list(dataframe, **kwargs)
    assert result == 3

def test_calculate_filled_values():
    dataframe = pl.DataFrame({
        'col1': ['a', 'b', None, 'd', 'e'],
        'col2': [1, 2, 3, None, 5]
    })

    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col2'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_filled_values(dataframe)
    assert_frame_equal(result, validation_df)

def test_check_values_in_range():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 3, 4, 5]
    })
    kwargs = {
        'columns': ['col1'],
        'range_start': 2,
        'range_end': 4
    }
    result = check_values_in_range(dataframe, **kwargs)
    assert result == 3

def test_check_values_unique():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 2, 4, 5]
    })
    kwargs = {
        'columns': ['col1']
    }
    result = check_values_unique(dataframe, **kwargs)
    assert result == 4

def test_check_values_format():
    dataframe = pl.DataFrame({
        'col1': ['abc', 'def', 'ghi', '123', '456']
    })
    kwargs = {
        'columns': ['col1'],
        'format': r'^[a-z]+$'
    }
    result = check_values_format(dataframe, **kwargs)
    assert result == 3

def test_check_values_consistent():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'join_col': ['a', 'b', 'c', 'd', 'e']
    })
    compare_df = pl.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'join_col': ['a', 'b', 'c', 'd', 'e']
    })
    result = check_values_consistent(dataframe, ['col1'], compare_df, ['join_col'])
    assert result == 5

def test_check_expected_value_count():
    dataframe = pl.DataFrame({
        'col1': ['a', 'a', 'b', 'b', 'b'],
        'RecordID': [1, 2, 3, 4, 5]
    })
    kwargs = {
        'columns': ['col1'],
        'expected_count': 2
    }
    result = check_expected_value_count(dataframe, **kwargs)
    assert result == 2

def test_check_expected_distinct_value_count():
    dataframe = pl.DataFrame({
        'col1': ['a', 'a', 'b', 'b', 'b'],
        'col2': [1, 2, 3, 4, 5]
    })
    kwargs = {
        'columns': ['col1'],
        'expected_count': 2,
        'distinct_columns': ['col2']
    }
    result = check_expected_distinct_value_count(dataframe, **kwargs)
    assert result == 2

def test_column_sums_to_1():
    dataframe = pl.DataFrame({
        'col1': ['a', 'a', 'b', 'b', 'b'],
        'value': [0.5, 0.5, 0.3, 0.3, 0.4]
    })
    kwargs = {
        'columns': ['col1'],
        'sum_column': 'value'
    }
    result = column_sums_to_1(dataframe, **kwargs)
    assert result == 5

def test_calculate_blocking_issues_values_in_range():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'to_date': [date(2099, 12, 31)] * 5
    })
    blocking_check_dict = [{
        'check': 'values in range',
        'columns': ['col1'],
        'range_start': 1,
        'range_end': 5
    }]
    try:
        calculate_blocking_issues(dataframe, blocking_check_dict)
    except ValueError:
        pytest.fail("Unexpected ValueError raised")

def test_calculate_blocking_issues_values_in_range_violation():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 3, 4, 6],
        'to_date': [date(2099, 12, 31)] * 5
    })
    blocking_check_dict = [{
        'check': 'values in range',
        'columns': ['col1'],
        'range_start': 1,
        'range_end': 5
    }]
    with pytest.raises(ValueError, match="Blocking issue violation detected: values in range"):
        calculate_blocking_issues(dataframe, blocking_check_dict)

def test_calculate_blocking_issues_values_are_unique():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'to_date': [date(2099, 12, 31)] * 5
    })
    blocking_check_dict = [{
        'check': 'values are unique',
        'columns': ['col1']
    }]
    try:
        calculate_blocking_issues(dataframe, blocking_check_dict)
    except ValueError:
        pytest.fail("Unexpected ValueError raised")

def test_calculate_blocking_issues_values_are_unique_violation():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 2, 4, 5],
        'to_date': [date(2099, 12, 31)] * 5
    })
    blocking_check_dict = [{
        'check': 'values are unique',
        'columns': ['col1']
    }]
    with pytest.raises(ValueError, match="Blocking issue violation detected: values are unique"):
        calculate_blocking_issues(dataframe, blocking_check_dict)

def test_calculate_blocking_issues_values_have_format():
    dataframe = pl.DataFrame({
        'col1': ['abc', 'def', 'ghi', 'jkl', 'mno'],
        'to_date': [date(2099, 12, 31)] * 5
    })
    blocking_check_dict = [{
        'check': 'values have format',
        'columns': ['col1'],
        'format': r'^[a-z]+$'
    }]
    try:
        calculate_blocking_issues(dataframe, blocking_check_dict)
    except ValueError:
        pytest.fail("Unexpected ValueError raised")

def test_calculate_blocking_issues_values_have_format_violation():
    dataframe = pl.DataFrame({
        'col1': ['abc', 'def', 'ghi', '123', '456'],
        'to_date': [date(2099, 12, 31)] * 5
    })
    blocking_check_dict = [{
        'check': 'values have format',
        'columns': ['col1'],
        'format': r'^[a-z]+$'
    }]
    with pytest.raises(ValueError, match="Blocking issue violation detected: values have format"):
        calculate_blocking_issues(dataframe, blocking_check_dict)

def test_calculate_signalling_issues_values_within_list():
    dataframe = pl.DataFrame({
        'col1': ['a', 'b', 'c', 'd', 'e'],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'values within list',
        'columns': ['col1'],
        'value_list': ['a', 'b', 'c']
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_2'],
            'column_name': ['col1', 'col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', 'values within list of: "a","b","c"'],
            'total_count': [5, 5, 5],
            'valid_count': [5, 4, 3]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    assert_frame_equal(result, validation_df)

def test_calculate_signalling_issues_values_in_range():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'values in range',
        'columns': ['col1'],
        'range_start': 2,
        'range_end': 4
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_3'],
            'column_name': ['col1', 'col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', 'values between 2 and 4'],
            'total_count': [5, 5, 5],
            'valid_count': [5, 4, 3]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    assert_frame_equal(result, validation_df)

def test_calculate_signalling_issues_values_are_unique():
    dataframe = pl.DataFrame({
        'col1': [1, 2, 2, 4, 5],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'values are unique',
        'columns': ['col1']
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_4'],
            'column_name': ['col1', 'col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', 'unique values in column `col1`'],
            'total_count': [5, 5, 5],
            'valid_count': [5, 4, 4]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    assert_frame_equal(result, validation_df)

def test_calculate_signalling_issues_values_have_format():
    dataframe = pl.DataFrame({
        'col1': ['abc', 'def', 'ghi', '123', '456'],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'values have format',
        'columns': ['col1'],
        'format': r'^[a-z]+$'
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_5'],
            'column_name': ['col1', 'col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', 'values have format ^[a-z]+$'],
            'total_count': [5, 5, 5],
            'valid_count': [5, 4, 3]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    assert_frame_equal(result, validation_df)

def test_calculate_signalling_issues_values_occur_as_expected():
    dataframe = pl.DataFrame({
        'col1': ['a', 'a', 'b', 'b', 'b'],
        'RecordID': [1, 2, 3, 4, 5],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'values occur as expected',
        'columns': ['col1'],
        'expected_count': 2
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_7'],
            'column_name': ['col1', 'col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', 'values occur 2 times'],
            'total_count': [5, 5, 5],
            'valid_count': [5, 4, 2]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    assert_frame_equal(result, validation_df)

def test_calculate_signalling_issues_values_sum_to_1():
    dataframe = pl.DataFrame({
        'col1': ['a', 'a', 'b', 'b', 'b'],
        'value': [0.5, 0.5, 0.3, 0.3, 0.4],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'values sum to 1',
        'columns': ['col1'],
        'sum_column': 'value'
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_1', 'dq_8'],
            'column_name': ['col1', 'value','col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', 'Check if values are filled', 'values in column "value" sum to 1'],
            'total_count': [5, 5, 5, 5],
            'valid_count': [5, 5, 4, 5]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    print(result)
    assert_frame_equal(result, validation_df)

def test_calculate_signalling_issues_distinct_values_occur_as_expected():
    dataframe = pl.DataFrame({
        'col1': ['a', 'a', 'b', 'b', 'b'],
        'col2': [1, 2, 3, None, 5]
    })
    signalling_check_dict = [{
        'check': 'distinct values occur as expected',
        'columns': ['col1'],
        'expected_count': 2,
        'distinct_columns': ['col2']
    }]
    validation_df = pl.DataFrame(
        {
            'signalling_id': [None, None, None],
            'check_id': ['dq_1', 'dq_1', 'dq_9'],
            'column_name': ['col1', 'col2', 'col1'],
            'check_name': ['Check if values are filled', 'Check if values are filled', '2 distinct values occur in column col2'],
            'total_count': [5, 5, 5],
            'valid_count': [5, 4, 2]
        }
    ).with_columns(pl.col('signalling_id').cast(pl.Int64))
    result = calculate_signalling_issues(dataframe, signalling_check_dict)
    assert_frame_equal(result, validation_df)

def test_record_has_expected_history():
    dataframe = pl.DataFrame({
        'RecordID': [1, 2, 3, 4, 5]
    })

    expected_trace = [
        {'target_RecordID': 1, 'source_table_name': 'table1', 'amount_records': 2},
        {'target_RecordID': 2, 'source_table_name': 'table2', 'amount_records': 3},
        {'target_RecordID': 1, 'source_table_name': 'table3', 'amount_records': 1},
    ]

    mock_df = pl.DataFrame({
        'source_table_name': ['table1', 'table1', 'table2', 'table2', 'table2', 'table3'],
        'source_RecordID': ['a', 'b', 'd', 'e', 'f', 'g'],
        'target_table_name': ['target_table', 'target_table', 'target_table', 'target_table', 'target_table', 'target_table'],
        'target_RecordID': [1, 1, 2, 2, 2, 1],
    })

    kwargs = {
        'expected_trace': expected_trace,
        'record_trace_table': mock_df
    }

    result = record_has_expected_history(dataframe, **kwargs)
    assert result == 0

if __name__ == "__main__":
    pytest.main()
    





















































































# def test_record_has_expected_history():
#     dataframe = pl.DataFrame({
#         'RecordID': [1, 2, 3, 4, 5]
#     })

#     expected_trace = [
#         {'target_RecordID': 1, 'source_table_name': 'table1', 'amount_records': 2},
#         {'target_RecordID': 2, 'source_table_name': 'table2', 'amount_records': 3},
#         {'target_RecordID': 3, 'source_table_name': 'table3', 'amount_records': 1},
#     ]

#     mock_df =  pl.DataFrame({
#             'source_table_name': ['table1', 'table1', 'table2', 'table2','table3'],
#             'source_RecordID': ['a', 'b', 'd', 'e','f'],
#             'target_table_name': ['target_table', 'target_table', 'target_table', 'target_table', 'target_table'],
#             'target_RecordID': [1, 1, 2, 2, 3],
#         })

#     kwargs = {
#         'table_name': 'sample_table',
#         'expected_trace': mock_df
#     }

#     result = record_has_expected_history(dataframe, **kwargs)
#     assert result == 2











if __name__ == "__main__":
    pytest.main()