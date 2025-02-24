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

def get_datamodel_schema(table_name: str = '') -> dict:
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
        'some_table_datamodel': {
        'columns' :  {
            'string_column': pl.String,
            'integer_column': pl.Int32,
            'decimal_column': pl.Decimal(25,10),
            'date_column': pl.Date,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        }, 
        'container': 'datamodel',
        'location': 'some_table',
        'file_format': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'flights_datamodel':{
        'columns': {
            'Flight_Date':pl.Date,
            'Weekday':pl.Int64,
            'Airline_Code':pl.String,
            'Flight_Number':pl.String,
            'Tail_Number':pl.String,
            'Origin_Airport':pl.String,
            'Destination_Airport':pl.String,
            'Scheduled_Departure':pl.Datetime,
            'Departure_Time':pl.Datetime,
            'Departure_Delay':pl.Int64,
            'Taxi_Out':pl.Int64,
            'Wheels_Off':pl.Datetime,
            'Scheduled_Time':pl.Int64,
            'Elapsed_Time':pl.Int64,
            'Air_Time':pl.Int64,
            'Distance':pl.Int64,
            'Wheels_On':pl.Datetime,
            'Taxi_In':pl.Int64,
            'Scheduled_Arrival':pl.Datetime,
            'Arrival_Time':pl.Datetime,
            'Arrival_Delay':pl.Int64,
            'Diverted':pl.Boolean,
            'Cancelled':pl.Boolean,
            'Cancellation_Reason':pl.String,
            'Air_System_Delay':pl.Int64,
            'Security_Delay':pl.Int64,
            'Airline_Delay':pl.Int64,
            'Late_Aircraft_Delay':pl.Int64,
            'Weather_Delay':pl.Int64,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'datamodel',
        'location': 'flights',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'airports_datamodel':{
        'columns': {
            'IATA_Airport_Code': pl.String,
            'Airport': pl.String,
            'City': pl.String,
            'State': pl.String,
            'Country': pl.String,
            'Latitude': pl.Decimal(25,10),
            'Longitude': pl.Decimal(25,10),
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'datamodel',
        'location': 'airports',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': [
            {
                'check':'values are unique',
                'columns':['IATA_Airport_Code']
            }
        ]
        },
        'airlines_datamodel':{
        'columns': {
            'IATA_Airline_Code': pl.String,
            'Airline': pl.String,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'datamodel',
        'location': 'airlines',
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