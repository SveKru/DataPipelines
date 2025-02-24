"""
This module contains the definition of the monitoring table used in the datahub to record data quality checks.

"""
import polars as pl

def get_monitoring_schema(table_name: str = '') -> dict:

    schema_dict = {
        'monitoring_values': {
            'columns':  {
                'signalling_id': pl.Int64,
                'check_id': pl.String,
                'column_name': pl.String,
                'check_name': pl.String,
                'total_count': pl.Int64,
                'valid_count': pl.Int64,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
    },
            'container': 'monitoring',
            'location': 'monitoring_values',
            'file_format': 'parquet',
            'partition_column': 'table_name',
            'quality_checks': []
        },
        'record_tracing': {
            'columns':  {
                'source_RecordID': pl.String,
                'source_table_name': pl.String,
                'target_RecordID': pl.String,
            },
            'container': 'monitoring',
            'location': 'record_tracing',
            'file_format': 'parquet',
            'partition_column': 'target_table_name',
            'quality_checks': []
        },
    }

    if not table_name:
        return schema_dict
    
    if table_name not in schema_dict.keys():
        raise ValueError(f"Table {table_name} does not exist in the landingzone")
    
    return schema_dict[table_name]
