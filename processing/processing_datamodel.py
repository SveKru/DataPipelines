from polars_src.custom_dataframes import CustomDF
import polars as pl
from datetime import datetime
from pytz import timezone


def generate_table_datamodel(table_name:str) -> bool:

    if table_name == 'flights_datamodel':
        flights_raw = CustomDF('flights_raw')

        timezone_raw = CustomDF('timezone_raw').custom_select(['Airport','TimeZoneOffset'])

        flights_raw = flights_raw.custom_join(timezone_raw, custom_left_on=pl.col('ORIGIN_AIRPORT'),custom_right_on=pl.col('Airport'),custom_how='left')

        flights_raw.data = flights_raw.data.rename({'TimeZoneOffset':'DepartureTimeZoneOffset'})

        flights_raw = flights_raw.custom_join(timezone_raw, custom_left_on=pl.col('DESTINATION_AIRPORT'),custom_right_on=pl.col('Airport'),custom_how='left')

        flights_raw.data = flights_raw.data.rename({'TimeZoneOffset':'DestinationTimeZoneOffset'})

        flights_raw.data  = flights_raw.data.with_columns((pl.col('DestinationTimeZoneOffset') - pl.col('DepartureTimeZoneOffset')).alias('TimeZoneOffset').cast(pl.Int64))

        flights_raw.data = flights_raw.data.with_columns(pl.date(pl.col('YEAR'),pl.col('MONTH'),pl.col('DAY')).alias('Flight_Date'))

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col('YEAR'),
                pl.col('MONTH'),
                pl.col('DAY'),
                pl.col('SCHEDULED_DEPARTURE').str.slice(0,2).cast(pl.Int64),
                pl.col('SCHEDULED_DEPARTURE').str.slice(2,2).cast(pl.Int64)
            ).alias('Scheduled_Departure'))

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col('YEAR'),
                pl.col('MONTH'),
                pl.col('DAY'),
                pl.col('SCHEDULED_DEPARTURE').str.slice(0,2).cast(pl.Int64),
                pl.col('SCHEDULED_DEPARTURE').str.slice(2,2).cast(pl.Int64)
            )\
            .dt.offset_by(pl.concat_str([pl.col('DEPARTURE_DELAY'),pl.lit('m')]))\
            .alias('Departure_Time'))

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col('YEAR'),
                pl.col('MONTH'),
                pl.col('DAY'),
                pl.col('SCHEDULED_DEPARTURE').str.slice(0,2).cast(pl.Int64),
                pl.col('SCHEDULED_DEPARTURE').str.slice(2,2).cast(pl.Int64)
            )\
            .dt.offset_by(pl.concat_str([pl.col('DEPARTURE_DELAY'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.col('TAXI_OUT'),pl.lit('m')]))\
            .alias('Wheels_Off'))
        
        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col('YEAR'),
                pl.col('MONTH'),
                pl.col('DAY'),
                pl.col('SCHEDULED_DEPARTURE').str.slice(0,2).cast(pl.Int64),
                pl.col('SCHEDULED_DEPARTURE').str.slice(2,2).cast(pl.Int64)
            )\
            .dt.offset_by(pl.concat_str([pl.col('DEPARTURE_DELAY'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.col('TAXI_OUT'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.col('AIR_TIME'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.coalesce(pl.col('TimeZoneOffset'),pl.lit('0')),pl.lit('m')]))\
            .alias('Wheels_On'))
        
        
        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col('YEAR'),
                pl.col('MONTH'),
                pl.col('DAY'),
                pl.col('SCHEDULED_DEPARTURE').str.slice(0,2).cast(pl.Int64),
                pl.col('SCHEDULED_DEPARTURE').str.slice(2,2).cast(pl.Int64)
            )\
            .dt.offset_by(pl.concat_str([pl.col('DEPARTURE_DELAY'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.col('TAXI_OUT'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.col('AIR_TIME'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.col('TAXI_IN'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.coalesce(pl.col('TimeZoneOffset'),pl.lit('0')),pl.lit('m')]))\
            .alias('Arrival_Time'))
        
        
        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col('YEAR'),
                pl.col('MONTH'),
                pl.col('DAY'),
                pl.col('SCHEDULED_DEPARTURE').str.slice(0,2).cast(pl.Int64),
                pl.col('SCHEDULED_DEPARTURE').str.slice(2,2).cast(pl.Int64)
            )\
            .dt.offset_by(pl.concat_str([pl.col('SCHEDULED_TIME'),pl.lit('m')]))\
            .dt.offset_by(pl.concat_str([pl.coalesce(pl.col('TimeZoneOffset'),pl.lit('0')),pl.lit('m')]))\
            .alias('Scheduled_Arrival'))
        
        flights_raw.data = flights_raw.data.with_columns(
            pl.col('DIVERTED').cast(pl.Boolean).alias('Diverted'),
            pl.col('CANCELLED').cast(pl.Boolean).alias('Cancelled')
        )

        rename_columns_dict = {
            'DAY_OF_WEEK':'Weekday',
            'AIRLINE':'Airline_Code',
            'FLIGHT_NUMBER':'Flight_Number',
            'TAIL_NUMBER':'Tail_Number',
            'ORIGIN_AIRPORT':'Origin_Airport',
            'DESTINATION_AIRPORT':'Destination_Airport',
            'DEPARTURE_DELAY':'Departure_Delay',
            'TAXI_OUT':'Taxi_Out',
            'SCHEDULED_TIME':'Scheduled_Time',
            'ELAPSED_TIME':'Elapsed_Time',
            'AIR_TIME':'Air_Time',
            'DISTANCE':'Distance',
            'TAXI_IN':'Taxi_In',
            'ARRIVAL_DELAY':'Arrival_Delay',
            'CANCELLATION_REASON':'Cancellation_Reason',
            'AIR_SYSTEM_DELAY':'Air_System_Delay',
            'SECURITY_DELAY':'Security_Delay',
            'AIRLINE_DELAY':'Airline_Delay',
            'LATE_AIRCRAFT_DELAY':'Late_Aircraft_Delay',
            'WEATHER_DELAY':'Weather_Delay'
        }

        flights_raw.rename_columns(rename_columns_dict)

        flights_raw = flights_raw.custom_select(
            [
            'Flight_Date',
            'Weekday',
            'Airline_Code',
            'Flight_Number',
            'Tail_Number',
            'Origin_Airport',
            'Destination_Airport',
            'Scheduled_Departure',
            'Departure_Time',
            'Departure_Delay',
            'Taxi_Out',
            'Wheels_Off',
            'Scheduled_Time',
            'Elapsed_Time',
            'Air_Time',
            'Distance',
            'Wheels_On',
            'Taxi_In',
            'Scheduled_Arrival',
            'Arrival_Time',
            'Arrival_Delay',
            'Diverted',
            'Cancelled',
            'Cancellation_Reason',
            'Air_System_Delay',
            'Security_Delay',
            'Airline_Delay',
            'Late_Aircraft_Delay',
            'Weather_Delay',
            'from_date',
            'to_date'
            ]
        )

        flights_datamodel = CustomDF('flights_datamodel',initial_df=flights_raw.data)
        flights_datamodel.write_table()

    elif table_name == 'airlines_datamodel':

        airlines_raw = CustomDF('airlines_raw')

        airlines_rename_dict = {
            'IATA_CODE':'IATA_Airline_Code',
            'AIRLINE':'Airline',
        }

        airlines_raw.rename_columns(airlines_rename_dict)

        airlines_datamodel = CustomDF('airlines_datamodel',initial_df=airlines_raw.data)
        airlines_datamodel.write_table()

    elif table_name == 'airports_datamodel':
        
        airports_raw = CustomDF('airports_raw')

        airport_rename_dict = {
            'IATA_CODE':'IATA_Airport_Code',
            'AIRPORT':'Airport',
            'CITY':'City',
            'STATE':'State',
            'COUNTRY':'Country',
            'LATITUDE':'Latitude',
            'LONGITUDE':'Longitude',
        }

        airports_raw.rename_columns(airport_rename_dict)

        airports_datamodel = CustomDF('airports_datamodel',initial_df=airports_raw.data)
        airports_datamodel.write_table()

    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')


    return True