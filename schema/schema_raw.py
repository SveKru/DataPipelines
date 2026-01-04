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

def get_raw_schema(table_name: str = '') -> dict:
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
        'some_table_raw': {
        'columns' :  {
            'string_column': pl.String,
            'integer_column': pl.Int32,
            'decimal_column': pl.Decimal(25,10),
            'date_column': pl.Date,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        }, 
        'container': 'raw',
        'location': 'some_table',
        'file_format': 'parquet',
        'partition_column' : '',
        'quality_checks': []
        },
        'airports_raw':{
        'columns': {
            'IATA_CODE': pl.String,
            'AIRPORT': pl.String,
            'CITY': pl.String,
            'STATE': pl.String,
            'COUNTRY': pl.String,
            'LATITUDE': pl.Decimal(25,10),
            'LONGITUDE': pl.Decimal(25,10),
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'airports',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': [
            {
                'check':'values are unique',
                'columns':['IATA_CODE']
            }
        ]
        },
        'airlines_raw':{
        'columns': {
            'IATA_CODE': pl.String,
            'AIRLINE': pl.String,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'airlines',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'cancellation_codes_raw':{
        'columns': {
            'CANCELLATION_REASON': pl.String,
            'CANCELLATION_DESCRIPTION': pl.String,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'cancellation_codes',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'flights_raw':{
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
            'WEATHER_DELAY':pl.Int64,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'flights',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': [
            {
                'check':'values in range',
                'columns':['YEAR'],
                'range_start':1980,
                'range_end':2025
            },
            {
                'check':'values have format',
                'columns':['AIRLINE'],
                'format':r'[A-Z0-9]{2}'
            }
        ]
        },
        'timezone_raw':{
        'columns': {
            'Airport': pl.String,
            'TimeZone': pl.String,
            'TimeZoneOffset': pl.Int64,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'timezone',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'gamedata_raw':{
        'columns': {
            "teamIdIntern":pl.String, 
            "teamIdExtern":pl.String,
            "time":pl.Datetime, 
            "localId":pl.String, 
            "visitId":pl.String,
            "idMatchIntern": pl.String,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'gamedata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'teamdata_raw':{
        'columns': {
            "teamIdIntern":pl.String, 
            "teamIdExtern":pl.String, 
            "name":pl.String, 
            "shortName":pl.String,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'teamdata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playergamedata_raw':{
            'columns':{
                "actorId":pl.String,
                "playerUuid":pl.String,
                "teamId":pl.String,
                "name":pl.String,
                "playerInternTeam":pl.String,
                "dorsal":pl.Int16,
                "starting":pl.Boolean,
                "captain":pl.Boolean,
                "timePlayed":pl.Int16,
                'idMatchIntern': pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'playergamedata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playersubstitionsgamedata_raw':{
            'columns':{
                "playerUuid":pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                'type': pl.String,
                'minuteAbsolut':pl.Int64,
                'pointDiff':pl.Int64,
                'idMatchIntern': pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'playersubstitionsgamedata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playergamestatsdata_raw':{
            'columns':{
            "playerUuid": pl.String,
            "actorId": pl.String,
            "teamId": pl.String,
            'score':pl.Int64,
            'shotsOfOneAttempted':pl.Int64,
            'shotsOfOneSuccessful':pl.Int64,
            'shotsOfTwoAttempted':pl.Int64,
            'shotsOfTwoSuccessful':pl.Int64,
            'shotsOfThreeAttempted':pl.Int64,
            'shotsOfThreeSuccessful':pl.Int64,
            'gamePlayed':pl.Int16,
            'timePlayed':pl.Int16,
            'rebounds':pl.Int64,
            'assists':pl.Int64,
            'steals':pl.Int64,
            'personal':pl.Int64,
            'valoration':pl.Int64,
            'idMatchIntern': pl.String,
            'from_date': pl.Date,
            'to_date': pl.Date,
            'RecordID': pl.String,
        },
        'container': 'raw',
        'location': 'playergamestatsdata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'teamgamestatsdata_raw':{
            'columns':{
                "team_uuid":pl.String,
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
                'valoration':pl.Int64,
                'idMatchIntern': pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'teamgamestatsdata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playergameshotsdata2ptmade_raw':{
            'columns':{
                'game_uuid':pl.String,
                'player_uuid':pl.String,
                'shot_type':pl.String,
                'period':pl.Int64,
                'minute':pl.Int64,
                'xnormalize':pl.Decimal(25, 10),
                'ynormalize':pl.Decimal(25, 10),
                'outcome':pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'playergameshotsdata2ptmade',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playergameshotsdata3ptmade_raw':{
            'columns':{
                'game_uuid':pl.String,
                'player_uuid':pl.String,
                'shot_type':pl.String,
                'period':pl.Int64,
                'minute':pl.Int64,
                'xnormalize':pl.Decimal(25, 10),
                'ynormalize':pl.Decimal(25, 10),
                'outcome':pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'playergameshotsdata3ptmade',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playergameshotsdata2ptmissed_raw':{
            'columns':{
                'game_uuid':pl.String,
                'player_uuid':pl.String,
                'shot_type':pl.String,
                'period':pl.Int64,
                'minute':pl.Int64,
                'xnormalize':pl.Decimal(25, 10),
                'ynormalize':pl.Decimal(25, 10),
                'outcome':pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'playergameshotsdata2ptmissed',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'playergameshotsdata3ptmissed_raw':{
            'columns':{
                'game_uuid':pl.String,
                'player_uuid':pl.String,
                'shot_type':pl.String,
                'period':pl.Int64,
                'minute':pl.Int64,
                'xnormalize':pl.Decimal(25, 10),
                'ynormalize':pl.Decimal(25, 10),
                'outcome':pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'playergameshotsdata3ptmissed',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'gameteamscoresdata_raw':{
            'columns':{
                "idMatchIntern":pl.String,
                'local':pl.Int64,
                'visit':pl.Int64,
                'minuteQuarter':pl.String,
                'minuteAbsolute':pl.String,
                'period':pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'gameteamscoresdata',
        'file_format': 'parquet',
        'partition_column': '',
        'quality_checks': []
        },
        'gamedatametadata_raw':{
            'columns':{
                'year':pl.Int64,
                'competition_code':pl.String,
                'phase':pl.String,
                'group':pl.String,
                'long_name':pl.String,
                'uuid':pl.String,
                'from_date': pl.Date,
                'to_date': pl.Date,
                'RecordID': pl.String,
            },
        'container': 'raw',
        'location': 'gamedatametadata',
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