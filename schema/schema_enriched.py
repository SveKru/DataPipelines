"""
This module contains the definition of the tables used in the datahub from the raw layer.

Each table is defined as a dictionary with the following keys:
- 'columns': A StructType object defining the schema of the table.
- 'container': The name of the container where the table is stored.
- 'location': The location of the table within the container.
- 'type': The data type of the table.
- 'partition_column': The name of the partition column in the table.
- 'quality_checks': A list of quality checks to be performed on the table.

The `get_table_definition` function is used to retrieve the definition of a specific table.

Example:
    'table_name_container': {
        'columns' :  {
            'string_column', pl.String,
            'integer_column', pl.Int64,
            'decimal_column', pl.Decimal,
            'Date_column', pl.Date,
        }, 
        'container': 'container_name',
        'location': 'location_in_container',
        'type': ['csv','parquet'],
        'partition_column' : 'name_of_partition_column',
        'quality_checks': [{
                                'check': 'values are unique',
                                'columns': ['string_columns']
            },{
                                'check': 'values have format',
                                'columns': ['string_column'],
                                'format': r"[a-zA-Z\-]"
            }]
    }
"""

import polars as pl

def get_enriched_schema(table_name: str = '') -> dict:
    """
    Template for a table:

    'table_name_container': {
        'columns' :  {
            'string_column', pl.String,
            'integer_column', pl.Int64,
            'decimal_column', pl.Decimal,
            'Date_column', pl.Date,
        }, 
        'container': 'container_name',
        'location': 'location_in_container',
        'type': ['csv','parquet'],
        'partition_column' : 'name_of_partition_column',
        'quality_checks': [{
                                'check': 'values are unique',
                                'columns': ['string_columns']
            },{
                                'check': 'values have format',
                                'columns': ['string_column'],
                                'format': r"[a-zA-Z\-]"
            }]
    }
    """
    schema_dict = {
        'some_table_enriched': {
        'columns' :  {
            'string_column': pl.String,
            'integer_column': pl.Int32,
            'decimal_column': pl.Decimal(25,10),
            'date_column': pl.Date,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        }, 
        'container': 'enriched',
        'location': 'some_table',
        'file_format': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'flight_airport_departures_enriched':{
        'columns': {
            'Flight_Date':pl.Date,
            'Airline_Name':pl.String,
            'Airport_Name':pl.String,
            'Amount_Flights':pl.UInt32,
            'Average_Delay':pl.Float64,
            'Average_Taxi':pl.Float64,
            'Max_Delay':pl.Int64,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'enriched',
        'location': 'flight_airport_departures',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },

    }

    if not table_name:
        return schema_dict
    
    if table_name not in schema_dict.keys():
        raise ValueError(f"Table {table_name} does not exist in the raw")
    
    return schema_dict[table_name]