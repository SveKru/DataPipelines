
def get_signalling_checks_datamodel(table_name: str) -> list:

    signalling_checks_dict = {
        'airports_datamodel':[
            {
            'check': 'values have format',
            'columns': ['IATA_Airport_Code'],
            'format': r"^[\w]{3}$"
            },
            {
            'check': 'values in range',
            'columns': ['Latitude'],
            'range_start': -90,
            'range_end': 90
            },
            {
            'check': 'values in range',
            'columns': ['Longitude'],
            'range_start': -180,
            'range_end': 180
            },
            {
            'check': 'values in list',
            'columns': ['Country'],
            'value_list': ['USA']
            },
            {
            'check': 'values are unique',
            'columns': ['IATA_Airport_Code']
            },

        ],
        'airlines_datamodel':[
            {
            'check': 'values are unique',
            'columns': ['IATA_Airline_Code']
            },
        ],
        'flights_datamodel':[
            {
            'check': 'values have format',
            'columns': ['Airline_Code'],
            'format': r"^[a-zA-Z]{2}$"
            },
            {
            'check': 'values have format',
            'columns': ['Tail_Number'],
            'format': r"^[\w]{6}$"
            },
            {
            'check': 'values occur as expected',
            'columns': ['Flight_Date','Flight_Number'],
            'expected_count' : 1
            },
            {
            'check':'record has expected history',
            'columns':['all'],
            'table_name':'flights_datamodel',
            'expected_trace':{
                'source_table_name':['flights_raw','timezone_raw'],
                'amount_records':[1,2]
            }
            },
        ],
    }

    return signalling_checks_dict.get(table_name,[])