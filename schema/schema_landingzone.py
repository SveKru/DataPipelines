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


def get_landingzone_schema(table_name: str = "") -> dict:
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
        "some_table_landingzone": {
            "columns": {
                "string_column": pl.String,
                "integer_column": pl.Int32,
                "decimal_column": pl.Decimal(25, 10),
                "date_column": pl.Date,
            },
            "container": "landingzone",
            "location": "some_location.csv",
            "file_format": "csv",
            "partition_column": "name_of_partition_column",
            "quality_checks": [],
        },
        "test_product_dimension_table_landingzone": {
            "columns": {
                "pk_column": pl.Int64,
                "value_string": pl.String,
                "value_int": pl.Int64,
            },
            "container": "landingzone",
            "location": "product_dimension_table.csv",
            "file_format": "csv",
            "partition_column": "",
            "quality_checks": [],
        },
        "test_transaction_fact_table_landingzone": {
            "columns": {
                "transaction_id": pl.Int64,
                "amount": pl.Int64,
                "product_fk": pl.Int64,
            },
            "container": "landingzone",
            "location": "transaction_fact_table.csv",
            "file_format": "csv",
            "partition_column": "",
            "quality_checks": [],
        },
        "gamedata_landingzone": {
            "columns": {
                "time": pl.String,
                "localId": pl.String,
                "visitId": pl.String,
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$",
        },
        "teamdata_landingzone": {
            "columns": {
                "teamIdIntern": pl.String,
                "teamIdExtern": pl.String,
                "name": pl.String,
                "shortName": pl.String,
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams",
        },
        "playergamedata_landingzone": {
            "columns": {
                "actorId": pl.String,
                "uuid": pl.String,
                "teamId": pl.String,
                "name": pl.String,
                "playerInternTeam": pl.String,
                "dorsal": pl.String,
                "starting": pl.String,
                "captain": pl.String,
                "timePlayed": pl.String,
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "playersubstitionsgamedata_landingzone": {
            "columns": {
                "uuid": pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                "inOutsList": pl.List(
                    pl.Struct(
                        {
                            "type": pl.String,
                            "minuteAbsolut": pl.Int64,
                            "pointDiff": pl.Int64,
                        }
                    )
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "playergamestatsdata_landingzone": {
            "columns": {
                "uuid": pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                "gamePlayed": pl.String,
                "timePlayed": pl.String,
                "data": pl.Struct(
                    {
                        "score": pl.Int64,
                        "shotsOfOneAttempted": pl.Int64,
                        "shotsOfOneSuccessful": pl.Int64,
                        "shotsOfTwoAttempted": pl.Int64,
                        "shotsOfTwoSuccessful": pl.Int64,
                        "shotsOfThreeAttempted": pl.Int64,
                        "shotsOfThreeSuccessful": pl.Int64,
                        "rebounds": pl.Int64,
                        "assists": pl.Int64,
                        "steals": pl.Int64,
                        "personal": pl.Int64,
                        "valoration": pl.Int64,
                    }
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "teamgamestatsdata_landingzone": {
            "columns": {
                "teamIdExtern": pl.String,
                "data": pl.Struct(
                    {
                        "score": pl.Int64,
                        "shotsOfOneAttempted": pl.Int64,
                        "shotsOfOneSuccessful": pl.Int64,
                        "shotsOfTwoAttempted": pl.Int64,
                        "shotsOfTwoSuccessful": pl.Int64,
                        "shotsOfThreeAttempted": pl.Int64,
                        "shotsOfThreeSuccessful": pl.Int64,
                        "rebounds": pl.Int64,
                        "assists": pl.Int64,
                        "steals": pl.Int64,
                        "valoration": pl.Int64,
                        "personal": pl.Int64,
                    }
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*]",
        },
        "playergameshotsdata2ptmade_landingzone": {
            "columns": {
                "uuid": pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                "data": pl.Struct(
                    {
                        "shootingOfTwoSuccessfulPoint": pl.List(
                            pl.Struct(
                                {
                                    "period": pl.Int64,
                                    "min": pl.Int64,
                                    "xnormalize": pl.Decimal(25, 10),
                                    "ynormalize": pl.Decimal(25, 10),
                                }
                            )
                        ),
                    }
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "playergameshotsdata3ptmade_landingzone": {
            "columns": {
                "uuid": pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                "data": pl.Struct(
                    {
                        "shootingOfThreeSuccessfulPoint": pl.List(
                            pl.Struct(
                                {
                                    "period": pl.Int64,
                                    "min": pl.Int64,
                                    "xnormalize": pl.Decimal(25, 10),
                                    "ynormalize": pl.Decimal(25, 10),
                                }
                            )
                        ),
                    }
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "playergameshotsdata2ptmissed_landingzone": {
            "columns": {
                "uuid": pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                "data": pl.Struct(
                    {
                        "shootingOfTwoFailedPoint": pl.List(
                            pl.Struct(
                                {
                                    "period": pl.Int64,
                                    "min": pl.Int64,
                                    "xnormalize": pl.Decimal(25, 10),
                                    "ynormalize": pl.Decimal(25, 10),
                                }
                            )
                        ),
                    }
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "playergameshotsdata3ptmissed_landingzone": {
            "columns": {
                "uuid": pl.String,
                "actorId": pl.String,
                "teamId": pl.String,
                "data": pl.Struct(
                    {
                        "shootingOfThreeFailedPoint": pl.List(
                            pl.Struct(
                                {
                                    "period": pl.Int64,
                                    "min": pl.Int64,
                                    "xnormalize": pl.Decimal(25, 10),
                                    "ynormalize": pl.Decimal(25, 10),
                                }
                            )
                        ),
                    }
                ),
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$.teams[*].players[*]",
        },
        "gameteamscoresdata_landingzone": {
            "columns": {
                "score": pl.List(
                    pl.Struct(
                        {
                            "local": pl.Int64,
                            "visit": pl.Int64,
                            "minuteQuarter": pl.String,
                            "minuteAbsolute": pl.String,
                            "period": pl.String,
                        }
                    )
                )
            },
            "container": "landingzone",
            "location": "scraping/data",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$",
        },
        "gamedatametadata_landingzone": {
            "columns": {
                "year": pl.String,
                "competition_code": pl.String,
                "phase": pl.String,
                "group": pl.String,
                "long_name": pl.String,
                "uuid": pl.String,
            },
            "container": "landingzone",
            "location": "scraping/mapping/query_args.json",
            "file_format": "json",
            "partition_column": "",
            "quality_checks": [],
            "json_data_path": "$",
        },
    }

    if not table_name:
        return schema_dict

    if table_name not in schema_dict.keys():
        raise ValueError(f"Table {table_name} does not exist in the landingzone")

    return schema_dict[table_name]
