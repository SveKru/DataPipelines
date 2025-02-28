import polars as pl
import pytest
from datetime import date
from polars.testing import assert_frame_equal
from polars_src.custom_dataframes import CustomDF

def test_read_custom_dataframe():
    test_df = pl.DataFrame({
        'transaction_id':[1,2,3,4,5]
        ,'amount':[5,10,20,1,-1]
        ,'product_fk':[1,1,2,3,3]
    })

    test_schema = {
        'columns': {
            'transaction_id': pl.Int64,
            'amount': pl.Int64,
            'product_fk': pl.Int64,
        },
        'container': 'landingzone',
        'location': 'transaction_fact_table.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        }

    df = CustomDF('test_transaction_fact_table_landingzone',environment='testing')

    assert df._name == 'test_transaction_fact_table_landingzone'
    assert df._layer == "landingzone_layer"
    assert df._schema == test_schema
    assert_frame_equal(df.data,test_df)

def test_default_parameters_custom_dataframe():

    df = CustomDF('test_transaction_fact_table_landingzone')
    assert df._env == 'develop'
    assert df._history == 'recent'
    assert df._partition_name == ''

def test_rename_columns_custom_dataframes():
    test_df = pl.DataFrame({
        'transaction_id_new':[1,2,3,4,5]
        ,'amount':[5,10,20,1,-1]
        ,'product_fk':[1,1,2,3,3]
    })

    df = CustomDF('test_transaction_fact_table_landingzone',environment='testing')
    
    df.rename_columns({'transaction_id':'transaction_id_new'})

    assert_frame_equal(df.data,test_df)

def test_rename_invalid_column_custom_dataframes():
    
    df = CustomDF('test_transaction_fact_table_landingzone',environment='testing')
    with pytest.raises(ValueError):
        df.rename_columns({'transaction_id_invalid':'transaction_id_new'})
