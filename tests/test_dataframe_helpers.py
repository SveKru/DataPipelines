import polars as pl
import pytest
import os
from polars_src.dataframe_helpers import (
    create_map_column, 
    create_sha_values, 
    get_data_location, 
    clean_column_names, 
    apply_scd_type_2, 
    assign_signalling_id
    )
from polars.testing import assert_frame_equal
from datetime import date

class TestDataFrameHelpers:

    def test_create_map_column(self):
        df = pl.DataFrame({
            "RecordID": ["123", "456", "789"]
        })
        result = create_map_column(df, "test_df")
        expected = pl.DataFrame({
            "RecordID": ["123", "456", "789"],
            "map_test_df": [{"test_df": ["123"]}, {"test_df": ["456"]}, {"test_df": ["789"]}]
        })
        assert_frame_equal(result,expected)

    def test_create_sha_values(self):
        df = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"]
        })
        excepted = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],
            "shaValue": ['83494463727554711','15276044730183553259','14156979967077821316']
        })
        result = create_sha_values(df, ["col1", "col2"])
        assert "shaValue" in result.columns
        assert_frame_equal(result,excepted)

    def test_get_data_location(self):
        schema = {
            "container": "my_container",
            "location": "my_location",
            "partition_column": "my_partition"
        }
        partition_name = date(2023,1,1)
        result = get_data_location(schema, partition_name)
        expected = os.path.join(os.getcwd(), 'data', 'my_container', 'my_location', 'my_partition=2023-01-01')
        assert result == expected

        result_no_partition = get_data_location(schema)
        expected_no_partition = os.path.join(os.getcwd(), 'data', 'my_container', 'my_location')
        assert result_no_partition == expected_no_partition

    def test_clean_column_names(self):
        df = pl.DataFrame({
            "col-1": [1, 2, 3],
            "col(2)": [4, 5, 6],
            "col 3": [7, 8, 9]
        })
        result = clean_column_names(df)
        expected = pl.DataFrame({
            "col_1": [1, 2, 3],
            "col2": [4, 5, 6],
            "col_3": [7, 8, 9]
        })
        assert_frame_equal(result,expected)

    def test_no_change_apply_scd_type_2(self):

        incoming_data = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],
            "RecordID": ["123", "456", "789"]
        })

        existing_data = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],
            "RecordID": ["123", "456", "789"],
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31), date(2099,12,31)]
        })

        expected = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],            
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31), date(2099,12,31)]
        })

        result = apply_scd_type_2(incoming_data, existing_data)

        assert_frame_equal(result,expected)

    def test_only_added_apply_scd_type_2(self):

        incoming_data = pl.DataFrame({
            "col1": ["a", "b", "c", "d"],
            "col2": ["1", "2", "3", "4"],
            "RecordID": ["123", "456", "789", "101112"]
        })

        existing_data = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],
            "RecordID": ["123", "456", "789"],
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31), date(2099,12,31)]
        })

        expected = pl.DataFrame({
            "col1": ["a", "b", "c", "d"],
            "col2": ["1", "2", "3", "4"],            
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1),date.today()],
            "to_date": [date(2099,12,31), date(2099,12,31), date(2099,12,31), date(2099,12,31)]
        })

        result = apply_scd_type_2(incoming_data, existing_data)

        assert_frame_equal(result,expected)

    def test_only_deleted_apply_scd_type_2(self):

        incoming_data = pl.DataFrame({
            "col1": ["a", "b"],
            "col2": ["1", "2"],
            "RecordID": ["123", "456"]
        })

        existing_data = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],
            "RecordID": ["123", "456", "789"],
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31), date(2099,12,31)]
        })

        expected = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],            
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31),date.today()]
        })

        result = apply_scd_type_2(incoming_data, existing_data)

        assert_frame_equal(result,expected)

    def test_dropped_and_added_apply_scd_type_2(self):

        incoming_data = pl.DataFrame({
            "col1": ["a", "b", "d"],
            "col2": ["1", "2", "4"],
            "RecordID": ["123", "456", "101112"]
        })

        existing_data = pl.DataFrame({
            "col1": ["a", "b", "c"],
            "col2": ["1", "2", "3"],
            "RecordID": ["123", "456", "789"],
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31), date(2099,12,31)]
        })

        expected = pl.DataFrame({
            "col1": ["a", "b", "c", "d"],
            "col2": ["1", "2", "3", "4"],            
            "from_date": [date(2023,1,1), date(2023,1,1), date(2023,1,1),date.today()],
            "to_date": [date(2099,12,31), date(2099,12,31), date.today(), date(2099,12,31)]
        })

        result = apply_scd_type_2(incoming_data, existing_data)

        assert_frame_equal(result,expected)

    def test_no_change_assign_signalling_id(self):

        incoming_monitoring_df = pl.DataFrame({
            'signalling_id': [None, None],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col2'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4],
            'table_name': ['some_table', 'some_table']
        })

        existing_monitoring_df = pl.DataFrame({
            'signalling_id': [1, 2],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col2'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4],
            'table_name': ['some_table', 'some_table'],
            "from_date": [date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31)]
        })

        expected = pl.DataFrame({
            'signalling_id': [1, 2],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col2'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4],
            'table_name': ['some_table', 'some_table']
        })
        result = assign_signalling_id(incoming_monitoring_df, existing_monitoring_df)
        assert_frame_equal(result,expected)

    def test_new_assign_signalling_id(self):

        incoming_monitoring_df = pl.DataFrame({
            'signalling_id': [None, None],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col3'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4],
            'table_name': ['some_table', 'some_table']
        }).with_columns(pl.col('signalling_id').cast(pl.Int64))

        existing_monitoring_df = pl.DataFrame({
            'signalling_id': [1, 2],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col2'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4],
            'table_name': ['some_table', 'some_table'],
            "from_date": [date(2023,1,1), date(2023,1,1)],
            "to_date": [date(2099,12,31), date(2099,12,31)]
        })

        expected = pl.DataFrame({
            'signalling_id': [1, 3],
            'check_id': ['dq_1', 'dq_1'],
            'column_name': ['col1', 'col3'],
            'check_name':['Check if values are filled','Check if values are filled'],
            'total_count': [5, 5],
            'valid_count': [4, 4],
            'table_name': ['some_table', 'some_table']
        })
        result = assign_signalling_id(incoming_monitoring_df, existing_monitoring_df)
        assert_frame_equal(result,expected)

if __name__ == "__main__":
    pytest.main()
