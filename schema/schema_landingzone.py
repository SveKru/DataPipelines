"""
This module contains the definition of the tables used in the datahub from the landingzone layer.

Each table is defined as a dictionary with the following keys:
- 'columns': A StructType object defining the schema of the table.
- 'container': The name of the container where the table is stored.
- 'location': The location of the table within the container.
- 'file_type': The data type of the table.
- 'partition_column': The name of the partition column in the table.
- 'quality_checks': A list of quality checks to be performed on the table.

The `get_table_definition` function is used to retrieve the definition of a specific table.

Example:
    'table_name_container': {
        'columns' :  {
            'string_column': pl.String,
            'integer_column': pl.Int32,
            'decimal_column': pl.Decimal(25,10),
            'Date_column': pl.date,
        }, 
        'container': 'container_name',
        'location': 'location_in_container',
        'file_format': 'file_type',
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

def get_landingzone_schema(table_name: str = '') -> dict:
    """
    Template for a table:

    'table_name_container': {
        'columns' :  {
            'string_column': pl.String,
            'integer_column': pl.Int32,
            'decimal_column': pl.Decimal(25,10),
            'Date_column': pl.date,
        }, 
        'container': 'container_name',
        'location': 'location_in_container',
        'file_format': 'file_type',
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
        'some_table_landingzone': {
            'columns' :  {
                'string_column': pl.String,
                'integer_column': pl.Int32,
                'decimal_column': pl.Decimal(25,10),
                'date_column': pl.Date,
            }, 
            'container': 'landingzone',
            'location': 'some_location.csv',
            'file_format': 'csv',
            'partition_column' : 'name_of_partition_column',
            'quality_checks': []
        },
        'test_product_dimension_table_landingzone':{
        'columns': {
            'pk_column': pl.Int64,
            'value_string': pl.String,
            'value_int': pl.Int64,
        },
        'container': 'landingzone',
        'location': 'product_dimension_table.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        },
        'test_transaction_fact_table_landingzone':{
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
        },
        'airports_landingzone':{
        'columns': {
            'IATA_CODE': pl.String,
            'AIRPORT': pl.String,
            'CITY': pl.String,
            'STATE': pl.String,
            'COUNTRY': pl.String,
            'LATITUDE': pl.Decimal(25,10),
            'LONGITUDE': pl.Decimal(25,10)
        },
        'container': 'landingzone',
        'location': 'airports.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        },
        'airlines_landingzone':{
        'columns': {
            'IATA_CODE': pl.String,
            'AIRLINE': pl.String
        },
        'container': 'landingzone',
        'location': 'airlines.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        },
        'cancellation_codes_landingzone':{
        'columns': {
            'CANCELLATION_REASON': pl.String,
            'CANCELLATION_DESCRIPTION': pl.String
        },
        'container': 'landingzone',
        'location': 'cancellation_codes.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        },
        'flights_landingzone':{
        'columns': {
            'YEAR':pl.Int64,
            'MONTH':pl.Int64,
            'DAY':pl.Int64,
            'DAY_OF_WEEK':pl.Int64,
            'AIRLINE':pl.String,
            'FLIGHT_NUMBER':pl.String,
            'TAIL_NUMBER':pl.String,
            'ORIGIN_AIRPORT':pl.String,
            'DESTINATION_AIRPORT':pl.String,
            'SCHEDULED_DEPARTURE':pl.String,
            'DEPARTURE_TIME':pl.String,
            'DEPARTURE_DELAY':pl.Int64,
            'TAXI_OUT':pl.Int64,
            'WHEELS_OFF':pl.String,
            'SCHEDULED_TIME':pl.Int64,
            'ELAPSED_TIME':pl.Int64,
            'AIR_TIME':pl.Int64,
            'DISTANCE':pl.Int64,
            'WHEELS_ON':pl.String,
            'TAXI_IN':pl.Int64,
            'SCHEDULED_ARRIVAL':pl.String,
            'ARRIVAL_TIME':pl.String,
            'ARRIVAL_DELAY':pl.Int64,
            'DIVERTED':pl.Int64,
            'CANCELLED':pl.Int64,
            'CANCELLATION_REASON':pl.String,
            'AIR_SYSTEM_DELAY':pl.Int64,
            'SECURITY_DELAY':pl.Int64,
            'AIRLINE_DELAY':pl.Int64,
            'LATE_AIRCRAFT_DELAY':pl.Int64,
            'WEATHER_DELAY':pl.Int64
        },
        'container': 'landingzone',
        'location': 'flights.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        },
        'timezone_landingzone':{
        'columns': {
            'Airport': pl.String,
            'TimeZone': pl.String
        },
        'container': 'landingzone',
        'location': 'timezone.csv',
        'file_format': 'csv',
        'partition_column': '',
        'quality_checks': []
        },
        'gamedata_landingzone':{
            'columns': {
                "teamIdIntern":pl.String, 
                "teamIdExtern":pl.String,
                "time":pl.String, 
                "localId":pl.String, 
                "visitId":pl.String
        },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$'
        },
        'teamdata_landingzone':{
            'columns': {
                "teamIdIntern":pl.String, 
                "teamIdExtern":pl.String, 
                "name":pl.String, 
                "shortName":pl.String
        },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$.teams'
        },
        'playergamedata_landingzone':{
            'columns':{
                "actorId":pl.String,
                "uuid":pl.String,
                "teamId":pl.String,
                "name":pl.String,
                "playerInternTeam":pl.String,
                "dorsal":pl.String,
                "starting":pl.String,
                "captain":pl.String,
                "timePlayed":pl.String,
            },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$.teams[*].players[*]'
        },
        'playersubstitionsgamedata_landingzone':{
        'columns':{
            "uuid": pl.String,
            "actorId": pl.String,
            "teamId": pl.String,
            "inOutsList": pl.List(pl.Struct(
                {
                    "type": pl.String,
                    "minuteAbsolut": pl.Int64,
                    "pointDiff": pl.Int64,
                }
            )),
        },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$.teams[*].players[*]'
        },
        'playergamestatsdata_landingzone':{
        'columns':{
            "uuid": pl.String,
            "actorId": pl.String,
            "teamId": pl.String,
            "data": pl.Struct(
                {
                'score':pl.Int64,
                'shotsOfOneAttempted':pl.Int64,
                'shotsOfOneSuccessful':pl.Int64,
                'shotsOfTwoAttempted':pl.Int64,
                'shotsOfTwoSuccessful':pl.Int64,
                'shotsOfThreeAttempted':pl.Int64,
                'shotsOfThreeSuccessful':pl.Int64,
                'rebounds':pl.Int64,
                'assists':pl.Int64,
                'steals':pl.Int64,
                'personal':pl.Int64,
                'valoration':pl.Int64
                }
            ),
        },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$.teams[*].players[*]'
        },
        'teamgamestatsdata_landingzone':{
            'columns':{
                "teamIdExtern":pl.String,
                "data":pl.Struct(
                    {
                    'score':pl.Int64,
                    'shotsOfOneAttempted':pl.Int64,
                    'shotsOfOneSuccessful':pl.Int64,
                    'shotsOfTwoAttempted':pl.Int64,
                    'shotsOfTwoSuccessful':pl.Int64,
                    'shotsOfThreeAttempted':pl.Int64,
                    'shotsOfThreeSuccessful':pl.Int64,
                    'rebounds':pl.Int64,
                    'assists':pl.Int64,
                    'steals':pl.Int64,
                    'valoration':pl.Int64,
                    'personal':pl.Int64
                    }
                )
            },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$.teams[*]'
        },
        'playergameshotsdata_landingzone':{
        'columns':{
            "uuid": pl.String,
            "actorId": pl.String,
            "teamId": pl.String,
            "data": pl.Struct(
                {
                'shootingOfTwoSuccessfulPoint':pl.List(pl.Struct(
                    {
                        'period':pl.Int64,
                        'min':pl.Int64,
                        'xnormalize':pl.Decimal(25,10),
                        'ynormalize':pl.Decimal(25,10),
                    }
                )),
                'shootingOfThreeSuccessfulPoint':pl.List(pl.Struct(
                    {
                        'period':pl.Int64,
                        'min':pl.Int64,
                        'xnormalize':pl.Decimal(25,10),
                        'ynormalize':pl.Decimal(25,10),
                    }
                )),
                'shootingOfTwoFailedPoint':pl.List(pl.Struct(
                    {
                        'period':pl.Int64,
                        'min':pl.Int64,
                        'xnormalize':pl.Decimal(25,10),
                        'ynormalize':pl.Decimal(25,10),
                    }
                )),
                'shootingOfThreeFailedPoint':pl.List(pl.Struct(
                    {
                        'period':pl.Int64,
                        'min':pl.Int64,
                        'xnormalize':pl.Decimal(25,10),
                        'ynormalize':pl.Decimal(25,10),
                    }
                )),
                }
            ),
        },
        'container': 'landingzone',
        'location': 'scraping/data',
        'file_format': 'json',
        'partition_column': '',
        'quality_checks': [],
        'json_data_path': '$.teams[*].players[*]'
        },

    }

    if not table_name:
        return schema_dict
    
    if table_name not in schema_dict.keys():
        raise ValueError(f"Table {table_name} does not exist in the landingzone")
    
    return schema_dict[table_name]