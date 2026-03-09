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


def get_enriched_schema(table_name: str = "") -> dict:
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
        "some_table_enriched": {
            "columns": {
                "string_column": pl.String,
                "integer_column": pl.Int32,
                "decimal_column": pl.Decimal(25, 10),
                "date_column": pl.Date,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "some_table",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [],
        },
        "playergameplusminus_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "game_uuid": pl.String,
                "total_plus_minus": pl.Int64,
                "avg_plus_minus": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playergameplusminus",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "player_uuid"]},
            ],
        },
        "playerplusminus_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "total_plus_minus": pl.Int64,
                "avg_plus_minus": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playerplusminus",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["player_uuid"]},
            ],
        },
        "playerstatssummary_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "season": pl.Int64,
                "total_games": pl.UInt32,
                "total_points": pl.Int64,
                "total_minutes": pl.Int64,
                "total_ft_attempted": pl.Int64,
                "total_ft_made": pl.Int64,
                "total_two_made": pl.Int64,
                "total_three_made": pl.Int64,
                "average_points": pl.Float64,
                "average_minutes": pl.Float64,
                "avg_two_made": pl.Float64,
                "avg_three_made": pl.Float64,
                "avg_ft_made": pl.Float64,
                "avg_ft_attempted": pl.Float64,
                "std_points": pl.Float64,
                "min_points": pl.Int64,
                "max_points": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playerstatssummary",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["player_uuid", "season"]},
            ],
        },
        "player2ptsummary_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "season": pl.Int64,
                "twopoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Decimal(25, 10)),
                            pl.Field("ynormalize", pl.Decimal(25, 10)),
                        ]
                    )
                ),
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "player2ptsummary",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["player_uuid", "season"]},
            ],
        },
        "player3ptsummary_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "season": pl.Int64,
                "threepoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Decimal(25, 10)),
                            pl.Field("ynormalize", pl.Decimal(25, 10)),
                        ]
                    )
                ),
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "player3ptsummary",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["player_uuid", "season"]},
            ],
        },
        "playeranalytics_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "player_name": pl.String,
                "player_number": pl.Int16,
                "team_uuid": pl.String,
                "team_name": pl.String,
                "season": pl.Int64,
                "total_games": pl.UInt32,
                "total_points": pl.Int64,
                "total_minutes": pl.Int64,
                "total_ft_attempted": pl.Int64,
                "total_ft_made": pl.Int64,
                "total_two_made": pl.Int64,
                "total_three_made": pl.Int64,
                "average_points": pl.Float64,
                "average_minutes": pl.Float64,
                "avg_two_made": pl.Float64,
                "avg_three_made": pl.Float64,
                "avg_ft_made": pl.Float64,
                "avg_ft_attempted": pl.Float64,
                "std_points": pl.Float64,
                "min_points": pl.Int64,
                "max_points": pl.Int64,
                "total_plus_minus": pl.Int64,
                "avg_plus_minus": pl.Float64,
                "twopoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Decimal(25, 10)),
                            pl.Field("ynormalize", pl.Decimal(25, 10)),
                        ]
                    )
                ),
                "threepoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Decimal(25, 10)),
                            pl.Field("ynormalize", pl.Decimal(25, 10)),
                        ]
                    )
                ),
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playeranalytics",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["player_uuid", "season"]},
            ],
        },
        "teamstatssummary_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "season": pl.Int64,
                "total_games": pl.UInt32,
                "total_points": pl.Int64,
                "total_ft_attempted": pl.Int64,
                "total_ft_made": pl.Int64,
                "total_two_made": pl.Int64,
                "total_three_made": pl.Int64,
                "average_points": pl.Float64,
                "avg_two_made": pl.Float64,
                "avg_three_made": pl.Float64,
                "avg_ft_made": pl.Float64,
                "avg_ft_attempted": pl.Float64,
                "std_points": pl.Float64,
                "min_points": pl.Int64,
                "max_points": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamstatssummary",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["team_uuid", "season"]},
            ],
        },
        "opponentsstatssummary_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "season": pl.Int64,
                "average_points": pl.Float64,
                "avg_two_made": pl.Float64,
                "avg_three_made": pl.Float64,
                "avg_ft_made": pl.Float64,
                "avg_ft_attempted": pl.Float64,
                "std_points": pl.Float64,
                "min_points": pl.Int64,
                "max_points": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "opponentsstatssummary",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["team_uuid", "season"]},
            ],
        },
        "opponentstrendssummary_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "season": pl.Int64,
                "avg_points_diff_pct": pl.Float64,
                "avg_two_made_diff_pct": pl.Float64,
                "avg_three_made_diff_pct": pl.Float64,
                "avg_ftmade_diff_pct": pl.Float64,
                "avg_ftattempted_diff_pct": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "opponentstrendssummary",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["team_uuid", "season"]},
            ],
        },
        "teamanalytics_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "team_name": pl.String,
                "team_short_name": pl.String,
                "season": pl.Int64,
                "total_games": pl.UInt32,
                "total_points": pl.Int64,
                "total_ft_attempted": pl.Int64,
                "total_ft_made": pl.Int64,
                "total_two_made": pl.Int64,
                "total_three_made": pl.Int64,
                "average_points": pl.Float64,
                "avg_two_made": pl.Float64,
                "avg_three_made": pl.Float64,
                "avg_ft_made": pl.Float64,
                "avg_ft_attempted": pl.Float64,
                "std_points": pl.Float64,
                "min_points": pl.Int64,
                "max_points": pl.Int64,
                "average_points_opponent": pl.Float64,
                "avg_two_made_opponent": pl.Float64,
                "avg_three_made_opponent": pl.Float64,
                "avg_ft_made_opponent": pl.Float64,
                "avg_ft_attempted_opponent": pl.Float64,
                "std_points_opponent": pl.Float64,
                "min_points_opponent": pl.Int64,
                "max_points_opponent": pl.Int64,
                "avg_points_diff_pct": pl.Float64,
                "avg_two_made_diff_pct": pl.Float64,
                "avg_three_made_diff_pct": pl.Float64,
                "avg_ftmade_diff_pct": pl.Float64,
                "avg_ftattempted_diff_pct": pl.Float64,
                "twopoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Decimal(25, 10)),
                            pl.Field("ynormalize", pl.Decimal(25, 10)),
                        ]
                    )
                ),
                "threepoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Decimal(25, 10)),
                            pl.Field("ynormalize", pl.Decimal(25, 10)),
                        ]
                    )
                ),
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamanalytics",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["team_uuid", "season"]},
            ],
        },
        "teamgameanalytics_enriched": {
            "columns": {
                "game_uuid": pl.String,
                "team_uuid": pl.String,
                "team_name": pl.String,
                "team_short_name": pl.String,
                "team_uuid_opponent": pl.String,
                "team_name_opponent": pl.String,
                "team_short_name_opponent": pl.String,
                "season": pl.Int64,
                "team_type": pl.String,
                "game_time": pl.Date,
                "points": pl.Int64,
                "ft_attempted": pl.Int64,
                "ft_made": pl.Int64,
                "two_made": pl.Int64,
                "three_made": pl.Int64,
                "assists": pl.Int64,
                "rebounds": pl.Int64,
                "steals": pl.Int64,
                "fouls": pl.Int64,
                "points_opponent": pl.Int64,
                "ft_attempted_opponent": pl.Int64,
                "ft_made_opponent": pl.Int64,
                "two_made_opponent": pl.Int64,
                "three_made_opponent": pl.Int64,
                "assists_opponent": pl.Int64,
                "rebounds_opponent": pl.Int64,
                "steals_opponent": pl.Int64,
                "fouls_opponent": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamgameanalytics",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "team_uuid"]},
            ],
        },
        "playergameanalytics_enriched": {
            "columns": {
                "player_uuid":pl.String,
                "player_name":pl.String,
                "player_number":pl.Int16,
                "team_uuid":pl.String,
                "team_name":pl.String,
                "team_short_name":pl.String,
                "team_uuid_opponent": pl.String,
                "team_name_opponent": pl.String,
                "team_short_name_opponent": pl.String,
                "season":pl.Int64,
                "game_uuid":pl.String,
                "game_time":pl.Date,
                "points":pl.Int64,
                "ft_attempted":pl.Int64,
                "ft_made":pl.Int64,
                "two_made":pl.Int64,
                "three_made":pl.Int64,
                "minutes_played":pl.Int16,
                "total_plus_minus":pl.Int64,
                "avg_plus_minus":pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playergameanalytics",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "player_uuid"]},
            ],
        },
    }

    if not table_name:
        return schema_dict

    if table_name not in schema_dict.keys():
        raise ValueError(f"Table {table_name} does not exist in the raw")

    return schema_dict[table_name]
