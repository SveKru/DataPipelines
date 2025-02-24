from polars_src.custom_dataframes import CustomDF
import polars as pl
import datetime, pytz


def generate_table_raw(table_name:str) -> bool:

    if table_name == 'flights_raw':
        flights_landingzone = CustomDF('flights_landingzone')


        flights_raw = CustomDF('flights_raw',initial_df=flights_landingzone.data)
        flights_raw.write_table()

    elif table_name =='cancellation_codes_raw':
        cancellation_codes_landingzone = CustomDF('cancellation_codes_landingzone')


        cancellation_codes_raw = CustomDF('cancellation_codes_raw',initial_df=cancellation_codes_landingzone.data)
        cancellation_codes_raw.write_table()

    elif table_name == 'airlines_raw':
        airlines_landingzone = CustomDF('airlines_landingzone')


        airlines_raw = CustomDF('airlines_raw',initial_df=airlines_landingzone.data)
        airlines_raw.write_table()

    elif table_name == 'airports_raw':
        airports_landingzone = CustomDF('airports_landingzone')


        airports_raw = CustomDF('airports_raw',initial_df=airports_landingzone.data)
        airports_raw.write_table()
        
    elif table_name == 'timezone_raw':
        timezone_landingzone = CustomDF('timezone_landingzone')
        timezone_landingzone.data = timezone_landingzone.data.hstack(timezone_landingzone.data.map_rows(lambda t: datetime.datetime.now(pytz.timezone(t[1])).strftime('%z'))).rename({'map':'TimeZoneOffset'})
        timezone_landingzone.data = timezone_landingzone.data.with_columns((pl.when(pl.col('TimeZoneOffset').str.slice(0,1)=="+").then(1).otherwise(-1) * ((pl.col('TimeZoneOffset').str.slice(1,2).cast(pl.Int64) * 60) + (pl.col('TimeZoneOffset').str.slice(3,2).cast(pl.Int64)))).alias('TimeZoneOffset').cast(pl.Int64))
        timezone_landingzone = timezone_landingzone.custom_select(['Airport','TimeZone','TimeZoneOffset'])
        timezone_raw = CustomDF('timezone_raw',initial_df=timezone_landingzone.data)
        timezone_raw.write_table()
    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')


    return True