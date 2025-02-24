from polars_src.custom_dataframes import CustomDF
import polars as pl
from datetime import datetime
from pytz import timezone


def generate_table_enriched(table_name:str) -> bool:

    if table_name == 'flight_airport_departures_enriched':

        flights = CustomDF('flights_datamodel')
        airports = CustomDF('airports_datamodel')
        airlines = CustomDF('airlines_datamodel')

        flights = flights.custom_select(['Flight_Date','Airline_Code','Origin_Airport','Taxi_Out','Departure_Delay'])
        airports = airports.custom_select(['IATA_Airport_Code','Airport'])
        airlines = airlines.custom_select(['IATA_Airline_Code','Airline'])

        flights = flights.custom_join(airports,custom_how='left',custom_left_on=['Origin_Airport'],custom_right_on=['IATA_Airport_Code'])
        flights = flights.custom_join(airlines,custom_how='left',custom_left_on=['Airline_Code'],custom_right_on=['IATA_Airline_Code'])

        flights = flights.custom_groupby(['Flight_Date','Airline','Airport'],pl.len().alias('Amount_Flights'),
            pl.mean('Departure_Delay').alias('Average_Delay'),
            pl.mean('Taxi_Out').alias('Average_Taxi'),
            pl.max('Departure_Delay').alias('Max_Delay')
            )
        
        flights.data = flights.data.with_columns(pl.lit(None).cast(pl.Date).alias('from_date'),pl.lit(None).cast(pl.Date).alias('to_date'))

        rename_columns_dict = {
            'Airline':'Airline_Name',
            'Airport':'Airport_Name'
        }

        flights.rename_columns(rename_columns_dict)

        flights = flights.custom_select(
            [
            'Flight_Date',
            'Airline_Name',
            'Airport_Name',
            'Amount_Flights',
            'Average_Delay',
            'Average_Taxi',
            'Max_Delay',
            'from_date',
            'to_date'
            ]
        )

        flight_airport_departures = CustomDF('flight_airport_departures_enriched',initial_df=flights.data)
        flight_airport_departures.write_table()
        
    else:
        raise ValueError(
            f'The table: {table_name} is not specified in the processing functions')


    return True