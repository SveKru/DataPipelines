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
                "avg_offensive_points_on_court": pl.Float64,
                "avg_defensive_points_on_court": pl.Float64,
                "avg_offensive_points_per_minute": pl.Float64,
                "avg_defensive_points_per_minute": pl.Float64,
                "avg_quarters_started": pl.Float64,
                "avg_quarters_won_when_starting": pl.Float64,
                "avg_quarter_win_rate": pl.Float64,
                "total_clutch_games": pl.Int64,
                "avg_clutch_points": pl.Float64,
                "avg_clutch_shooting_pct": pl.Float64,
                "clutch_wins": pl.Int64,
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
        "playergameimpact_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "game_uuid": pl.String,
                "offensive_points_on_court": pl.Int64,
                "defensive_points_on_court": pl.Int64,
                "offensive_points_per_minute": pl.Float64,
                "defensive_points_per_minute": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playergameimpact",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "player_uuid"]},
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
                "home_games": pl.UInt32,
                "away_games": pl.UInt32,
                "home_avg_points": pl.Float64,
                "away_avg_points": pl.Float64,
                "home_avg_points_allowed": pl.Float64,
                "away_avg_points_allowed": pl.Float64,
                "home_avg_margin": pl.Float64,
                "away_avg_margin": pl.Float64,
                "home_win_rate": pl.Float64,
                "away_win_rate": pl.Float64,
                "home_away_point_diff": pl.Float64,
                "home_court_advantage": pl.Float64,
                "typical_home_game_day": pl.String,
                "typical_home_game_hour": pl.Int64,
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
        "playergamequarterstats_enriched": {
            "columns": {
                "game_uuid": pl.String,
                "player_uuid": pl.String,
                "quarters_started": pl.Int64,
                "quarters_won_when_starting": pl.Int64,
                "quarter_win_rate_when_starting": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playergamequarterstats",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "player_uuid"]},
            ],
        },
        "playergame_shots_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "season": pl.Int64,
                "game_uuid": pl.String,
                "game_date": pl.Date,
                "twopoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Float64),
                            pl.Field("ynormalize", pl.Float64),
                        ]
                    )
                ),
                "threepoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Float64),
                            pl.Field("ynormalize", pl.Float64),
                        ]
                    )
                ),
                "two_pt_made": pl.Int64,
                "two_pt_attempted": pl.Int64,
                "three_pt_made": pl.Int64,
                "three_pt_attempted": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playergame_shots",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "player_uuid"]},
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
                "offensive_points_on_court":pl.Int64,
                "defensive_points_on_court":pl.Int64,
                "offensive_points_per_minute": pl.Float64,
                "defensive_points_per_minute": pl.Float64,
                "quarters_started": pl.Int64,
                "quarters_won_when_starting": pl.Int64,
                "quarter_win_rate_when_starting": pl.Float64,
                "twopoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Float64),
                            pl.Field("ynormalize", pl.Float64),
                        ]
                    )
                ),
                "threepoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Float64),
                            pl.Field("ynormalize", pl.Float64),
                        ]
                    )
                ),
                "is_clutch_game": pl.Boolean,
                "fourth_quarter_points": pl.Int64,
                "fourth_quarter_minutes": pl.Float64,
                "clutch_points": pl.Int64,
                "clutch_minutes": pl.Float64,
                "clutch_ft_made": pl.Int64,
                "clutch_ft_attempted": pl.Int64,
                "clutch_two_made": pl.Int64,
                "clutch_two_attempted": pl.Int64,
                "clutch_three_made": pl.Int64,
                "clutch_three_attempted": pl.Int64,
                "clutch_shooting_pct": pl.Float64,
                "game_result": pl.String,
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
        "teamhomeawaysplits_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "team_name": pl.String,
                "team_short_name": pl.String,
                "season": pl.Int64,
                "home_games": pl.UInt32,
                "away_games": pl.UInt32,
                "home_avg_points": pl.Float64,
                "away_avg_points": pl.Float64,
                "home_avg_points_allowed": pl.Float64,
                "away_avg_points_allowed": pl.Float64,
                "home_avg_margin": pl.Float64,
                "away_avg_margin": pl.Float64,
                "home_win_rate": pl.Float64,
                "away_win_rate": pl.Float64,
                "home_away_point_diff": pl.Float64,
                "home_court_advantage": pl.Float64,
                "typical_home_game_day": pl.String,
                "typical_home_game_hour": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamhomeawaysplits",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["team_uuid", "season"]},
            ],
        },
        "teamquarterperformance_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "team_name": pl.String,
                "team_short_name": pl.String,
                "season": pl.Int64,
                "quarter": pl.Int64,
                "total_games": pl.UInt32,
                "avg_points": pl.Float64,
                "avg_points_allowed": pl.Float64,
                "avg_point_margin": pl.Float64,
                "quarter_win_rate": pl.Float64,
                "max_scoring_run": pl.Int64,
                "avg_largest_run": pl.Float64,
                "comeback_wins": pl.UInt32,
                "lead_blown": pl.UInt32,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamquarterperformance",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["team_uuid", "season", "quarter"]},
            ],
        },
        "fiveplayer_combinations_enriched": {
            "columns": {
                "game_uuid": pl.String,
                "team_uuid": pl.String,
                "team_name": pl.String,
                "season": pl.Int64,
                "game_date": pl.Date,
                "opponent": pl.String,
                "lineup_id": pl.String,
                "player_1_uuid": pl.String,
                "player_2_uuid": pl.String,
                "player_3_uuid": pl.String,
                "player_4_uuid": pl.String,
                "player_5_uuid": pl.String,
                "minutes": pl.Float64,
                "plus_minus": pl.Int64,
                "court_result": pl.String,
                "win_rate": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "fiveplayer_combinations",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "team_uuid", "lineup_id"]},
            ],
        },
        "threeplayer_combinations_enriched": {
            "columns": {
                "game_uuid": pl.String,
                "team_uuid": pl.String,
                "team_name": pl.String,
                "season": pl.Int64,
                "game_date": pl.Date,
                "opponent": pl.String,
                "combo_id": pl.String,
                "player_1_uuid": pl.String,
                "player_2_uuid": pl.String,
                "player_3_uuid": pl.String,
                "minutes": pl.Float64,
                "plus_minus": pl.Int64,
                "court_result": pl.String,
                "win_rate": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "threeplayer_combinations",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "team_uuid", "combo_id"]},
            ],
        },
        "gamecompetitiveness_enriched": {
            "columns": {
                "game_uuid": pl.String,
                "season": pl.Int64,
                "game_date": pl.Date,
                "home_team_uuid": pl.String,
                "away_team_uuid": pl.String,
                "home_final_score": pl.Int64,
                "away_final_score": pl.Int64,
                "final_margin": pl.Int64,
                "largest_lead": pl.Int64,
                "lead_changes": pl.Int64,
                "game_type": pl.String,
                "competitive_until_minute": pl.Int64,
                "comeback_win": pl.Boolean,
                "winning_team_uuid": pl.String,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "gamecompetitiveness",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid"]},
            ],
        },
        "playerclutchperformance_enriched": {
            "columns": {
                "player_uuid": pl.String,
                "game_uuid": pl.String,
                "season": pl.Int64,
                "game_date": pl.Date,
                "is_clutch_game": pl.Boolean,
                "fourth_quarter_points": pl.Int64,
                "fourth_quarter_minutes": pl.Float64,
                "clutch_points": pl.Int64,
                "clutch_minutes": pl.Float64,
                "clutch_ft_made": pl.Int64,
                "clutch_ft_attempted": pl.Int64,
                "clutch_two_made": pl.Int64,
                "clutch_two_attempted": pl.Int64,
                "clutch_three_made": pl.Int64,
                "clutch_three_attempted": pl.Int64,
                "clutch_shooting_pct": pl.Float64,
                "game_result": pl.String,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "playerclutchperformance",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "player_uuid"]},
            ],
        },
        "teamgamequarterperformance_enriched": {
            "columns": {
                "game_uuid": pl.String,
                "team_uuid": pl.String,
                "team_name": pl.String,
                "team_short_name": pl.String,
                "opponent_uuid": pl.String,
                "opponent_name": pl.String,
                "season": pl.Int64,
                "game_date": pl.Date,
                "team_type": pl.String,
                "quarter_1_points": pl.Int64,
                "quarter_2_points": pl.Int64,
                "quarter_3_points": pl.Int64,
                "quarter_4_points": pl.Int64,
                "quarter_1_points_allowed": pl.Int64,
                "quarter_2_points_allowed": pl.Int64,
                "quarter_3_points_allowed": pl.Int64,
                "quarter_4_points_allowed": pl.Int64,
                "quarter_1_margin": pl.Int64,
                "quarter_2_margin": pl.Int64,
                "quarter_3_margin": pl.Int64,
                "quarter_4_margin": pl.Int64,
                "quarters_won": pl.Int64,
                "quarters_lost": pl.Int64,
                "quarters_tied": pl.Int64,
                "largest_lead_q1": pl.Int64,
                "largest_lead_q2": pl.Int64,
                "largest_lead_q3": pl.Int64,
                "largest_lead_q4": pl.Int64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamgamequarterperformance",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "team_uuid"]},
            ],
        },
        "teamgame_shots_enriched": {
            "columns": {
                "team_uuid": pl.String,
                "team_name": pl.String,
                "team_short_name": pl.String,
                "season": pl.Int64,
                "game_uuid": pl.String,
                "game_date": pl.Date,
                "opponent_uuid": pl.String,
                "opponent_name": pl.String,
                "team_type": pl.String,
                "twopoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Float64),
                            pl.Field("ynormalize", pl.Float64),
                        ]
                    )
                ),
                "threepoint_locations": pl.List(
                    pl.Struct(
                        [
                            pl.Field("xnormalize", pl.Float64),
                            pl.Field("ynormalize", pl.Float64),
                        ]
                    )
                ),
                "two_pt_made": pl.Int64,
                "two_pt_attempted": pl.Int64,
                "three_pt_made": pl.Int64,
                "three_pt_attempted": pl.Int64,
                "two_pt_pct": pl.Float64,
                "three_pt_pct": pl.Float64,
                "from_date": pl.Date,
                "to_date": pl.Date,
                "RecordID": pl.String,
            },
            "container": "enriched",
            "location": "teamgame_shots",
            "file_format": "parquet",
            "partition_column": "",
            "quality_checks": [
                {"check": "values are unique", "columns": ["game_uuid", "team_uuid"]},
            ],
        },
    }

    if not table_name:
        return schema_dict

    if table_name not in schema_dict.keys():
        raise ValueError(f"Table {table_name} does not exist in the raw")

    return schema_dict[table_name]
