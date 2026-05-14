from polars_src.custom_dataframes import CustomDF
import polars as pl
from datetime import datetime
from pytz import timezone


def generate_table_enriched(table_name: str) -> bool:
    if table_name == "flight_airport_departures_enriched":
        flights = CustomDF("flights_datamodel")
        airports = CustomDF("airports_datamodel")
        airlines = CustomDF("airlines_datamodel")

        flights = flights.custom_select(
            [
                "Flight_Date",
                "Airline_Code",
                "Origin_Airport",
                "Taxi_Out",
                "Departure_Delay",
            ]
        )
        airports = airports.custom_select(["IATA_Airport_Code", "Airport"])
        airlines = airlines.custom_select(["IATA_Airline_Code", "Airline"])

        flights = flights.custom_join(
            airports,
            custom_how="left",
            custom_left_on=["Origin_Airport"],
            custom_right_on=["IATA_Airport_Code"],
        )
        flights = flights.custom_join(
            airlines,
            custom_how="left",
            custom_left_on=["Airline_Code"],
            custom_right_on=["IATA_Airline_Code"],
        )

        flights = flights.custom_groupby(
            ["Flight_Date", "Airline", "Airport"],
            pl.len().alias("Amount_Flights"),
            pl.mean("Departure_Delay").alias("Average_Delay"),
            pl.mean("Taxi_Out").alias("Average_Taxi"),
            pl.max("Departure_Delay").alias("Max_Delay"),
        )

        flights.data = flights.data.with_columns(
            pl.lit(None).cast(pl.Date).alias("from_date"),
            pl.lit(None).cast(pl.Date).alias("to_date"),
        )

        rename_columns_dict = {"Airline": "Airline_Name", "Airport": "Airport_Name"}

        flights.rename_columns(rename_columns_dict)

        flights = flights.custom_select(
            [
                "Flight_Date",
                "Airline_Name",
                "Airport_Name",
                "Amount_Flights",
                "Average_Delay",
                "Average_Taxi",
                "Max_Delay",
                "from_date",
                "to_date",
            ]
        )

        flight_airport_departures = CustomDF(
            "flight_airport_departures_enriched", initial_df=flights.data
        )
        flight_airport_departures.write_table()

    elif table_name == "playergameplusminus_enriched":
        substitions = CustomDF("playergamesubstitionsgamedata_datamodel")

        substitions.data = substitions.data.with_columns(
            pl.col("minute_absolute")
            .rank("dense")
            .over(["game_uuid", "player_uuid", "type"])
            .alias("minute_absolute_rank")
        )
        substitions_in = substitions.custom_select(
            ["game_uuid", "player_uuid", "minute_absolute_rank", "type", "point_diff"]
        )
        substitions_in.data = substitions_in.data.filter(pl.col("type") == "IN_TYPE")
        substitions_out = substitions.custom_select(
            ["game_uuid", "player_uuid", "minute_absolute_rank", "type", "point_diff"]
        )
        substitions_out.data = substitions_out.data.filter(pl.col("type") == "OUT_TYPE")

        substitions = substitions_in.custom_join(
            substitions_out,
            custom_on=["game_uuid", "player_uuid", "minute_absolute_rank"],
            custom_how="left",
        )

        substitions.data = substitions.data.with_columns(
            (pl.col("point_diff_right") - pl.col("point_diff")).alias("plus_minus")
        )

        substitions = substitions.custom_select(
            ["game_uuid", "player_uuid", "plus_minus"]
        )

        substitions = substitions.custom_groupby(
            ["game_uuid", "player_uuid"], 
            pl.sum("plus_minus").alias("total_plus_minus"),
            pl.mean("plus_minus").alias("avg_plus_minus"),
        )

        substitions = substitions.custom_select(
            ["player_uuid", "game_uuid", "total_plus_minus", "avg_plus_minus"]
        )

        playergameplusminus_enriched = CustomDF(
            "playergameplusminus_enriched", initial_df=substitions.data
        )
        playergameplusminus_enriched.write_table()

    elif table_name == "playerplusminus_enriched":
        player_game_plusminus = CustomDF("playergameplusminus_enriched")

        player_plusminus = player_game_plusminus.custom_groupby(
            ["player_uuid"],
            pl.sum("total_plus_minus").alias("total_plus_minus"),
            pl.mean("total_plus_minus").alias("avg_plus_minus"),
        )

        playerplusminus_enriched = CustomDF(
            "playerplusminus_enriched", initial_df=player_plusminus.data
        )
        playerplusminus_enriched.write_table()

    elif table_name == "playerstatssummary_enriched":
        player_stats = CustomDF("playergamestatsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")

        player_stats = player_stats.custom_join(
            game_data.custom_select(["game_uuid", "season"]).custom_distinct(),
            custom_on=["game_uuid"],
            custom_how="left",
        )
        player_stats.data = player_stats.data.filter(pl.col("did_play") == 1)

        player_stats = player_stats.custom_select(
            [
                pl.col("season"),
                "player_uuid",
                "points",
                "ft_attempted",
                "ft_made",
                "two_made",
                "three_made",
                "minutes_played",
            ]
        )

        player_stats = player_stats.custom_groupby(
            ["season", "player_uuid"],
            pl.len().alias("total_games"),
            pl.sum("points").alias("total_points"),
            pl.sum("minutes_played").alias("total_minutes"),
            pl.sum("ft_attempted").alias("total_ft_attempted"),
            pl.sum("ft_made").alias("total_ft_made"),
            pl.sum("two_made").alias("total_two_made"),
            pl.sum("three_made").alias("total_three_made"),
            pl.mean("points").alias("average_points"),
            pl.mean("minutes_played").alias("average_minutes"),
            pl.mean("two_made").alias("avg_two_made"),
            pl.mean("three_made").alias("avg_three_made"),
            pl.mean("ft_made").alias("avg_ft_made"),
            pl.mean("ft_attempted").alias("avg_ft_attempted"),
            pl.std("points").alias("std_points"),
            pl.min("points").alias("min_points"),
            pl.max("points").alias("max_points"),
        )

        player_stats = player_stats.custom_select(
            [
                "player_uuid",
                "season",
                "total_games",
                "total_points",
                "total_minutes",
                "total_ft_attempted",
                "total_ft_made",
                "total_two_made",
                "total_three_made",
                "average_points",
                "average_minutes",
                "avg_two_made",
                "avg_three_made",
                "avg_ft_made",
                "avg_ft_attempted",
                "std_points",
                "min_points",
                "max_points",
            ]
        )

        playerstatssummary_enriched = CustomDF(
            "playerstatssummary_enriched", initial_df=player_stats.data
        )
        playerstatssummary_enriched.write_table()

    elif table_name == "player2ptsummary_enriched":
        playergameshotsdata_df = CustomDF("playergameshotsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")

        # Deduplicate game_data to avoid cartesian product (each game has 2 rows: home/away)
        game_data.data = game_data.data.unique(subset=["game_uuid"], keep="first")
        game_season = game_data.custom_select(["game_uuid", "season"])

        playergameshotsdata_df = playergameshotsdata_df.custom_join(
            game_season,
            custom_on=["game_uuid"],
            custom_how="left",
        )
        playergameshotsdata_df.data = playergameshotsdata_df.data.filter(
            pl.col("shot_type") == "2PT"
        ).unique()
        playergameshotsdata_df = playergameshotsdata_df.custom_groupby(
            ["player_uuid", "season"],
            pl.struct([pl.col("xnormalize"), pl.col("ynormalize")]).alias(
                "twopoint_locations"
            ),
        )

        playergameshotsdata_df = playergameshotsdata_df.custom_select(
            ["player_uuid", "season", "twopoint_locations"]
        )

        player2ptsummary_enriched = CustomDF(
            "player2ptsummary_enriched", initial_df=playergameshotsdata_df.data
        )
        player2ptsummary_enriched.write_table()

    elif table_name == "player3ptsummary_enriched":
        playergameshotsdata_df = CustomDF("playergameshotsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")

        # Deduplicate game_data to avoid cartesian product (each game has 2 rows: home/away)
        game_data.data = game_data.data.unique(subset=["game_uuid"], keep="first")
        game_season = game_data.custom_select(["game_uuid", "season"])

        playergameshotsdata_df = playergameshotsdata_df.custom_join(
            game_season,
            custom_on=["game_uuid"],
            custom_how="left",
        )
        playergameshotsdata_df.data = playergameshotsdata_df.data.filter(
            pl.col("shot_type") == "3PT"
        ).unique()
        playergameshotsdata_df = playergameshotsdata_df.custom_groupby(
            ["player_uuid", "season"],
            pl.struct([pl.col("xnormalize"), pl.col("ynormalize")]).alias(
                "threepoint_locations"
            ),
        )

        playergameshotsdata_df = playergameshotsdata_df.custom_select(
            ["player_uuid", "season", "threepoint_locations"]
        )

        player3ptsummary_enriched = CustomDF(
            "player3ptsummary_enriched", initial_df=playergameshotsdata_df.data
        )
        player3ptsummary_enriched.write_table()

    elif table_name == "playeranalytics_enriched":
        player_data = CustomDF("playerdata_datamodel")
        team_data = CustomDF("teamdata_datamodel")
        player_stats = CustomDF("playerstatssummary_enriched")
        player_plusminus = CustomDF("playerplusminus_enriched")
        player_2pointers = CustomDF("player2ptsummary_enriched")
        player_3pointers = CustomDF("player3ptsummary_enriched")
        player_impact = CustomDF("playergameimpact_enriched")

        player_data = player_data.custom_select(
            ["player_uuid", "team_uuid", "player_name", "player_number"]
        )

        team_data = team_data.custom_select(
            ["team_uuid", "team_name"]
        ).custom_distinct()

        player_data.data = player_data.data.unique(subset=["player_uuid"], keep="first")
        team_data.data = team_data.data.unique(subset=["team_uuid"], keep="first")

        # Aggregate per-game impact metrics to career averages per player
        player_impact_avg = player_impact.custom_select(
            ["player_uuid", "offensive_points_on_court", "defensive_points_on_court",
             "offensive_points_per_minute", "defensive_points_per_minute"]
        ).custom_groupby(
            ["player_uuid"],
            pl.mean("offensive_points_on_court").alias("avg_offensive_points_on_court"),
            pl.mean("defensive_points_on_court").alias("avg_defensive_points_on_court"),
            pl.mean("offensive_points_per_minute").alias("avg_offensive_points_per_minute"),
            pl.mean("defensive_points_per_minute").alias("avg_defensive_points_per_minute"),
        )

        # Aggregate quarter stats
        player_quarter = CustomDF("playergamequarterstats_enriched")
        game_data_season = CustomDF("gamedata_datamodel")
        game_data_season.data = game_data_season.data.unique(subset=["game_uuid"], keep="first")
        game_data_season = game_data_season.custom_select(["game_uuid", "season"])

        player_quarter = player_quarter.custom_join(
            game_data_season,
            custom_on="game_uuid",
            custom_how="left",
        )

        player_quarter_avg = player_quarter.custom_groupby(
            ["player_uuid", "season"],
            pl.mean("quarters_started").alias("avg_quarters_started"),
            pl.mean("quarters_won_when_starting").alias("avg_quarters_won_when_starting"),
            pl.mean("quarter_win_rate_when_starting").alias("avg_quarter_win_rate"),
        )

        # Aggregate clutch performance
        player_clutch = CustomDF("playerclutchperformance_enriched")
        player_clutch.data = player_clutch.data.with_columns([
            pl.when(pl.col("is_clutch_game") == True).then(1).otherwise(0).alias("is_clutch_int"),
            pl.when(pl.col("game_result") == "Win").then(1).otherwise(0).alias("clutch_win_int"),
            (pl.col("clutch_2pt_made") * 2 + pl.col("clutch_3pt_made") * 3).alias("clutch_points")
        ])
        player_clutch_avg = player_clutch.custom_groupby(
            ["player_uuid", "season"],
            pl.sum("is_clutch_int").cast(pl.Int64).alias("total_clutch_games"),
            pl.mean("clutch_points").alias("avg_clutch_points"),
            pl.mean("clutch_fg_pct").alias("avg_clutch_shooting_pct"),
            pl.sum("clutch_win_int").cast(pl.Int64).alias("clutch_wins"),
        )

        player_data = (
            player_data.custom_join(
                player_stats.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["player_uuid"],
                custom_how="left",
            )
            .custom_join(
                team_data.custom_select(["team_uuid", "team_name"]).custom_distinct(),
                custom_on=["team_uuid"],
                custom_how="left",
            )
            .custom_join(
                player_plusminus.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["player_uuid"],
                custom_how="left",
            )
            .custom_join(
                player_impact_avg,
                custom_on=["player_uuid"],
                custom_how="left",
            )
            .custom_join(
                player_quarter_avg,
                custom_on=["player_uuid", "season"],
                custom_how="left",
            )
            .custom_join(
                player_clutch_avg,
                custom_on=["player_uuid", "season"],
                custom_how="left",
            )
            .custom_join(
                player_2pointers.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["player_uuid", "season"],
                custom_how="left",
            )
            .custom_join(
                player_3pointers.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["player_uuid", "season"],
                custom_how="left",
            )
        )

        player_analytics = player_data.custom_select(
            [
                "player_uuid",
                "player_name",
                "player_number",
                "team_uuid",
                "team_name",
                "season",
                "total_games",
                "total_points",
                "total_minutes",
                "total_ft_attempted",
                "total_ft_made",
                "total_two_made",
                "total_three_made",
                "average_points",
                "average_minutes",
                "avg_two_made",
                "avg_three_made",
                "avg_ft_made",
                "avg_ft_attempted",
                "std_points",
                "min_points",
                "max_points",
                "total_plus_minus",
                "avg_plus_minus",
                "avg_offensive_points_on_court",
                "avg_defensive_points_on_court",
                "avg_offensive_points_per_minute",
                "avg_defensive_points_per_minute",
                "avg_quarters_started",
                "avg_quarters_won_when_starting",
                "avg_quarter_win_rate",
                "total_clutch_games",
                "avg_clutch_points",
                "avg_clutch_shooting_pct",
                "clutch_wins",
                "twopoint_locations",
                "threepoint_locations",
            ]
        )

        player_analytics_enriched = CustomDF(
            "playeranalytics_enriched", initial_df=player_analytics.data
        )
        player_analytics_enriched.write_table()

    elif table_name == "playergameimpact_enriched":
        substitutions = CustomDF("playergamesubstitionsgamedata_datamodel")
        gameteamscoresdata = CustomDF("gameteamscoresdata_datamodel")
        player_data = CustomDF("playerdata_datamodel")

        # Rank IN/OUT events within each player-game-type group to pair them
        substitutions.data = substitutions.data.with_columns(
            pl.col("minute_absolute")
            .rank("dense")
            .over(["game_uuid", "player_uuid", "type"])
            .alias("rank")
        )

        # Split into IN and OUT events as CustomDF instances to preserve lineage
        stints_in = substitutions.custom_select(
            ["game_uuid", "player_uuid", "rank", "type", "minute_absolute"]
        )
        stints_in.data = (
            stints_in.data.filter(pl.col("type") == "IN_TYPE")
            .rename({"minute_absolute": "minute_in"})
            .drop("type")
        )

        stints_out = substitutions.custom_select(
            ["game_uuid", "player_uuid", "rank", "type", "minute_absolute"]
        )
        stints_out.data = (
            stints_out.data.filter(pl.col("type") == "OUT_TYPE")
            .rename({"minute_absolute": "minute_out"})
            .drop("type")
        )

        # Pair IN/OUT events, drop unmatched, then resolve each player's team
        stints = stints_in.custom_join(
            stints_out,
            custom_on=["game_uuid", "player_uuid", "rank"],
            custom_how="left",
        )
        stints.data = stints.data.filter(pl.col("minute_out").is_not_null())

        player_teams = player_data.custom_select(["player_uuid", "team_uuid"])
        player_teams.data = player_teams.data.unique(subset=["player_uuid"], keep="first")
        stints = stints.custom_join(
            player_teams,
            custom_on=["player_uuid"],
            custom_how="left",
        )

        # Prepare cumulative score snapshots sorted by game and minute
        scores = gameteamscoresdata.custom_select(
            ["game_uuid", "minuteAbsolute", "home_score", "away_score",
             "home_team_uuid", "away_team_uuid"]
        )
        scores.data = scores.data.sort(["game_uuid", "minuteAbsolute"])

        # Snapshot cumulative score at sub-in time via backward asof join
        stints.data = stints.data.sort(["game_uuid", "minute_in"])
        stints = stints.custom_join_asof(
            scores,
            custom_left_on_asof="minute_in",
            custom_right_on_asof="minuteAbsolute",
            custom_by=["game_uuid"],
            custom_strategy="backward",
        )
        stints.data = stints.data.rename({
            "home_score": "home_score_at_in",
            "away_score": "away_score_at_in",
        })

        # Snapshot cumulative score at sub-out time via backward asof join
        stints.data = stints.data.sort(["game_uuid", "minute_out"])
        stints = stints.custom_join_asof(
            scores.custom_select(["game_uuid", "minuteAbsolute", "home_score", "away_score"]),
            custom_left_on_asof="minute_out",
            custom_right_on_asof="minuteAbsolute",
            custom_by=["game_uuid"],
            custom_strategy="backward",
        )
        stints.data = stints.data.rename({
            "home_score": "home_score_at_out",
            "away_score": "away_score_at_out",
        })

        # Points scored while the player was on court = score delta over the stint window
        stints.data = stints.data.with_columns(
            pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
            .then(pl.col("home_score_at_out") - pl.col("home_score_at_in"))
            .otherwise(pl.col("away_score_at_out") - pl.col("away_score_at_in"))
            .cast(pl.Int64)
            .alias("offensive_points_on_court"),
            pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
            .then(pl.col("away_score_at_out") - pl.col("away_score_at_in"))
            .otherwise(pl.col("home_score_at_out") - pl.col("home_score_at_in"))
            .cast(pl.Int64)
            .alias("defensive_points_on_court"),
            (pl.col("minute_out") - pl.col("minute_in")).alias("minutes_on_court"),
        )

        # Aggregate per (game_uuid, player_uuid) and compute net impact and per-minute rates
        stints = stints.custom_select(
            ["game_uuid", "player_uuid", "offensive_points_on_court",
             "defensive_points_on_court", "minutes_on_court"]
        ).custom_groupby(
            ["game_uuid", "player_uuid"],
            pl.sum("offensive_points_on_court").alias("offensive_points_on_court"),
            pl.sum("defensive_points_on_court").alias("defensive_points_on_court"),
            pl.sum("minutes_on_court").alias("total_minutes_on_court"),
        )

        stints.data = stints.data.with_columns(
            (pl.col("offensive_points_on_court") / pl.col("total_minutes_on_court"))
            .alias("offensive_points_per_minute"),
            (pl.col("defensive_points_on_court") / pl.col("total_minutes_on_court"))
            .alias("defensive_points_per_minute"),
        )

        CustomDF(
            "playergameimpact_enriched",
            initial_df=stints.custom_select(
                ["player_uuid", "game_uuid", "offensive_points_on_court",
                 "defensive_points_on_court",
                 "offensive_points_per_minute", "defensive_points_per_minute"]
            ).data,
        ).write_table()

    elif table_name == "teamstatssummary_enriched":
        team_stats = CustomDF("teamgamestatsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")

        team_stats = team_stats.custom_join(
            game_data.custom_select(["game_uuid", "season"]).custom_distinct(),
            custom_on=["game_uuid"],
            custom_how="left",
        )

        teamstatssummary = team_stats.custom_select(
            [
                "team_uuid",
                "points",
                "ft_attempted",
                "ft_made",
                "two_made",
                "three_made",
                "season",
            ]
        ).custom_groupby(
            ["team_uuid", "season"],
            pl.len().alias("total_games"),
            pl.sum("points").alias("total_points"),
            pl.sum("ft_attempted").alias("total_ft_attempted"),
            pl.sum("ft_made").alias("total_ft_made"),
            pl.sum("two_made").alias("total_two_made"),
            pl.sum("three_made").alias("total_three_made"),
            pl.mean("points").alias("average_points"),
            pl.mean("two_made").alias("avg_two_made"),
            pl.mean("three_made").alias("avg_three_made"),
            pl.mean("ft_made").alias("avg_ft_made"),
            pl.mean("ft_attempted").alias("avg_ft_attempted"),
            pl.std("points").alias("std_points"),
            pl.min("points").alias("min_points"),
            pl.max("points").alias("max_points"),
        )

        teamstatssummary_enriched = CustomDF(
            "teamstatssummary_enriched", initial_df=teamstatssummary.data
        )
        teamstatssummary_enriched.write_table()

    elif table_name == "opponentsstatssummary_enriched":
        game_data = CustomDF("gamedata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        teamstatssummary = CustomDF("teamstatssummary_enriched")

        game_data = game_data.custom_select(
            [
                "game_uuid",
                "season",
            ]
        )
        game_data = game_data.custom_distinct()

        teamgamestats = teamgamestats.custom_join(
            game_data, custom_on=["game_uuid"], custom_how="inner"
        )

        teamgamestats_base = teamgamestats.custom_select(
            [
                "game_uuid",
                "team_uuid",
                "season",
            ]
        )
        opponents_stats = teamstatssummary.custom_join(
            teamgamestats_base, custom_on=["team_uuid", "season"], custom_how="inner"
        ).custom_join(
            teamgamestats.custom_drop(["from_date", "to_date", "RecordID"]),
            custom_on=["game_uuid", "season"],
            custom_how="inner",
        )

        opponents_stats.data = opponents_stats.data.filter(
            pl.col("team_uuid_right") != pl.col("team_uuid")
        )

        opponents_stats = opponents_stats.custom_groupby(
            ["team_uuid", "season"],
            pl.mean("points").alias("average_points"),
            pl.mean("two_made").alias("avg_two_made"),
            pl.mean("three_made").alias("avg_three_made"),
            pl.mean("ft_made").alias("avg_ft_made"),
            pl.mean("ft_attempted").alias("avg_ft_attempted"),
            pl.std("points").alias("std_points"),
            pl.min("points").alias("min_points"),
            pl.max("points").alias("max_points"),
        )

        opponentsstatssummary_enriched = CustomDF(
            "opponentsstatssummary_enriched", initial_df=opponents_stats.data
        )
        opponentsstatssummary_enriched.write_table()

    elif table_name == "opponentstrendssummary_enriched":
        teamstatssummary = CustomDF("opponentsstatssummary_enriched")
        game_data = CustomDF("gamedata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        teamstatssummary = CustomDF("teamstatssummary_enriched")

        game_data = game_data.custom_select(
            [
                "game_uuid",
                "season",
            ]
        )
        game_data = game_data.custom_distinct()

        teamgamestats = teamgamestats.custom_join(
            game_data, custom_on=["game_uuid"], custom_how="inner"
        )

        teamgamestats_base = teamgamestats.custom_select(
            [
                "game_uuid",
                "team_uuid",
                "season",
            ]
        )
        opponents_stats = teamstatssummary.custom_join(
            teamgamestats_base, custom_on=["team_uuid", "season"], custom_how="inner"
        ).custom_join(
            teamgamestats.custom_drop(["from_date", "to_date", "RecordID"]),
            custom_on=["game_uuid", "season"],
            custom_how="inner",
        )

        opponents_stats = opponents_stats.custom_join(
            teamstatssummary,
            custom_left_on=["team_uuid_right"],
            custom_right_on=["team_uuid"],
            custom_how="inner",
        )

        opponents_stats.data = opponents_stats.data.with_columns(
            (
                (pl.col("points") - pl.col("average_points")) / pl.col("average_points")
            ).alias("points_diff"),
            (
                (pl.col("two_made") - pl.col("avg_two_made")) / pl.col("avg_two_made")
            ).alias("2pt_diff"),
            (
                (pl.col("three_made") - pl.col("avg_three_made"))
                / pl.col("avg_three_made")
            ).alias("3pt_diff"),
            ((pl.col("ft_made") - pl.col("avg_ft_made")) / pl.col("avg_ft_made")).alias(
                "ftmade_diff"
            ),
            (
                (pl.col("ft_attempted") - pl.col("avg_ft_attempted"))
                / pl.col("avg_ft_attempted")
            ).alias("ftattempted_diff"),
        )

        opponents_stats = opponents_stats.custom_groupby(
            ["team_uuid", "season"],
            pl.mean("points_diff").alias("avg_points_diff_pct"),
            pl.mean("2pt_diff").alias("avg_two_made_diff_pct"),
            pl.mean("3pt_diff").alias("avg_three_made_diff_pct"),
            pl.mean("ftmade_diff").alias("avg_ftmade_diff_pct"),
            pl.mean("ftattempted_diff").alias("avg_ftattempted_diff_pct"),
        )

        opponentstrendssummary_enriched = CustomDF(
            "opponentstrendssummary_enriched", initial_df=opponents_stats.data
        )
        opponentstrendssummary_enriched.write_table()

    elif table_name == "teamanalytics_enriched":
        teamstatssummary = CustomDF("teamstatssummary_enriched")
        opponentsstatsummary = CustomDF("opponentsstatssummary_enriched")
        opponentstrendsummary = CustomDF("opponentstrendssummary_enriched")
        teamhomeawaysplits = CustomDF("teamhomeawaysplits_enriched")
        teamdata = CustomDF("teamdata_datamodel")
        player_analytics = CustomDF("playeranalytics_enriched")

        teamshotsummary = player_analytics.custom_groupby(
            ["season", "team_uuid"],
            pl.col("twopoint_locations").explode().alias("twopoint_locations"),
            pl.col("threepoint_locations").explode().alias("threepoint_locations"),
        )
        teamshotsummary.data = teamshotsummary.data.with_columns(
            pl.col("twopoint_locations").list.eval(
                pl.element().filter(
                    (pl.element().struct.field("xnormalize").is_not_null())
                    & (pl.element().struct.field("ynormalize").is_not_null())
                )
            ),
            pl.col("threepoint_locations").list.eval(
                pl.element().filter(
                    (pl.element().struct.field("xnormalize").is_not_null())
                    & (pl.element().struct.field("ynormalize").is_not_null())
                )
            ),
        )

        teamdata = teamdata.custom_select(["team_uuid", "team_name", "team_short_name"])
        teamdata.data = teamdata.data.unique(subset=["team_uuid"], keep="first")

        team_analytics = (
            teamdata.custom_join(
                teamstatssummary.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["team_uuid"],
                custom_how="left",
            )
            .custom_join(
                opponentsstatsummary.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["team_uuid", "season"],
                custom_how="left",
                custom_suffix="_opponent",
            )
            .custom_join(
                opponentstrendsummary.custom_drop(["from_date", "to_date", "RecordID"]),
                custom_on=["team_uuid", "season"],
                custom_how="left",
                custom_suffix="_opponent",
            )
            .custom_join(
                teamhomeawaysplits.custom_drop(["from_date", "to_date", "RecordID", "team_name", "team_short_name"]),
                custom_on=["team_uuid", "season"],
                custom_how="left",
            )
            .custom_join(
                teamshotsummary,
                custom_on=["team_uuid", "season"],
                custom_how="left",
            )
        )

        teamanalytics_enriched = CustomDF(
            "teamanalytics_enriched", initial_df=team_analytics.data
        )
        teamanalytics_enriched.write_table()

    elif table_name == "teamgameanalytics_enriched":
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        gamedata = CustomDF("gamedata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")

        gamedata = gamedata.custom_join(
            gamedata.custom_select(["game_uuid", "team_uuid"]),
            custom_on=["game_uuid"],
            custom_how="inner",
            custom_suffix="_opponent",
        )

        gamedata.data = gamedata.data.filter(
            pl.col("team_uuid") != pl.col("team_uuid_opponent")
        )

        teamgamestats = teamgamestats.custom_join(
            gamedata.custom_select(
                [
                    "game_uuid",
                    "team_uuid",
                    "team_uuid_opponent",
                    "game_time",
                    "season",
                    "team_type",
                ]
            ),
            custom_on=["game_uuid", "team_uuid"],
            custom_how="left",
        )

        teamdata = teamdata.custom_select(["team_uuid", "team_name", "team_short_name"])
        teamdata.data = teamdata.data.unique(subset=["team_uuid"], keep="first")

        team_game_analytics = teamgamestats.custom_join(
            teamdata, custom_on=["team_uuid"], custom_how="left"
        )
        team_game_analytics = team_game_analytics.custom_join(
            teamdata,
            custom_left_on=["team_uuid_opponent"],
            custom_right_on=["team_uuid"],
            custom_how="left",
            custom_suffix="_opponent",
        )

        team_game_analytics = team_game_analytics.custom_join(
            teamgamestats.custom_drop(["from_date", "to_date", "RecordID"]),
            custom_left_on=["game_uuid", "team_uuid_opponent"],
            custom_right_on=["game_uuid", "team_uuid"],
            custom_how="left",
            custom_suffix="_opponent",
        )

        team_game_analytics.data = team_game_analytics.data.with_columns(
            pl.col("game_time").cast(pl.Date())
        )

        team_game_analytics = team_game_analytics.custom_select(
            [
                "game_uuid",
                "team_uuid",
                "team_name",
                "team_short_name",
                "team_uuid_opponent",
                "team_name_opponent",
                "team_short_name_opponent",
                "season",
                "team_type",
                "game_time",
                "points",
                "ft_attempted",
                "ft_made",
                "two_made",
                "three_made",
                "assists",
                "rebounds",
                "steals",
                "fouls",
                "points_opponent",
                "ft_attempted_opponent",
                "ft_made_opponent",
                "two_made_opponent",
                "three_made_opponent",
                "assists_opponent",
                "rebounds_opponent",
                "steals_opponent",
                "fouls_opponent",
            ]
        )

        teamgameanalytics_enriched = CustomDF(
            "teamgameanalytics_enriched", initial_df=team_game_analytics.data
        )
        teamgameanalytics_enriched.write_table()

    elif table_name == "playergamequarterstats_enriched":
        raw_game_data = CustomDF("playergamedata_raw")
        gameteamscores = CustomDF("gameteamscoresdata_datamodel")
        substitutions_q = CustomDF("playergamesubstitionsgamedata_datamodel")
        player_data = CustomDF("playerdata_datamodel")

        print("\n>> Building player game quarter statistics with margin impact...")

        raw_game_data.data = (
            raw_game_data.data
            .select([
                pl.col("playerUuid").alias("player_uuid"),
                pl.col("idMatchIntern").alias("game_uuid"),
                pl.col("starting"),
            ])
            .filter(pl.col("player_uuid").is_not_null())
        )

        # Get player team mappings
        player_teams_q = (
            player_data.data
            .unique(subset=["player_uuid"], keep="first")
            .select(["player_uuid", "team_uuid"])
        )

        # Prepare substitution data with point_diff
        subs_data = substitutions_q.data.select([
            "game_uuid", "player_uuid", "type", "minute_absolute", "point_diff"
        ]).filter(
            pl.col("minute_absolute").is_not_null() &
            pl.col("type").is_not_null()
        )

        # Join subs with player teams to get team perspective
        subs_with_teams = subs_data.join(player_teams_q, on="player_uuid", how="left")

        # Define quarter boundaries
        quarter_boundaries = {
            1: (0, 10),
            2: (10, 20),
            3: (20, 30),
            4: (30, 40)
        }

        all_quarter_stats = []

        for quarter, (start_min, end_min) in quarter_boundaries.items():
            print(f"  Processing quarter {quarter}...")

            # Determine who started this quarter (on court at start_min boundary)
            if quarter == 1:
                quarter_starters = raw_game_data.data.select([
                    pl.lit(quarter).cast(pl.Int64).alias("quarter"),
                    pl.col("game_uuid"),
                    pl.col("player_uuid"),
                    pl.col("starting").alias("starts_quarter"),
                ])
            else:
                # For Q2-Q4, check last substitution AT OR BEFORE quarter start
                last_sub_before_q = (
                    subs_data
                    .filter(pl.col("minute_absolute") <= start_min)
                    .sort("minute_absolute")
                    .group_by(["game_uuid", "player_uuid"], maintain_order=True)
                    .agg(pl.last("type").alias("last_sub_type"))
                )
                quarter_starters = (
                    raw_game_data.data
                    .join(last_sub_before_q, on=["game_uuid", "player_uuid"], how="left")
                    .with_columns(
                        pl.when(pl.col("last_sub_type").is_not_null())
                        .then(pl.col("last_sub_type") == "IN_TYPE")
                        .otherwise(pl.col("starting"))
                        .alias("starts_quarter")
                    )
                    .select([
                        pl.lit(quarter).cast(pl.Int64).alias("quarter"),
                        pl.col("game_uuid"),
                        pl.col("player_uuid"),
                        pl.col("starts_quarter"),
                    ])
                )

            # Get first OUT in this quarter (only for players who STARTED the quarter)
            first_out_in_quarter = (
                subs_with_teams
                .filter(
                    (pl.col("minute_absolute") > start_min) &
                    (pl.col("minute_absolute") <= end_min) &
                    (pl.col("type") == "OUT_TYPE")
                )
                .group_by(["game_uuid", "player_uuid", "team_uuid"])
                .agg([
                    pl.col("minute_absolute").min().alias("first_out_minute"),
                    pl.col("point_diff").first().alias("point_diff_at_out")
                ])
            )

            # Get margin at quarter start from game scores
            # IMPORTANT: Must sort by minuteAbsolute before taking last() to get chronologically last score
            scores_at_quarter_start = (
                gameteamscores.data
                .filter(pl.col("minuteAbsolute") <= start_min)
                .sort("minuteAbsolute")
                .group_by("game_uuid", maintain_order=True)
                .agg([
                    pl.last("home_score").alias("home_score_start"),
                    pl.last("away_score").alias("away_score_start"),
                    pl.last("home_team_uuid").alias("home_team_uuid"),
                    pl.last("away_team_uuid").alias("away_team_uuid")
                ])
                .with_columns([
                    (pl.col("home_score_start") - pl.col("away_score_start")).alias("home_margin_start")
                ])
            )

            # Join starters with their OUT data and quarter start scores
            quarter_stats = (
                quarter_starters
                .join(player_teams_q, on="player_uuid", how="left")
                .join(first_out_in_quarter, on=["game_uuid", "player_uuid", "team_uuid"], how="left")
                .join(scores_at_quarter_start, on="game_uuid", how="left")
            )

            # Calculate margin at quarter start from team perspective
            quarter_stats = quarter_stats.with_columns([
                pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
                .then(pl.col("home_margin_start"))
                .otherwise(-pl.col("home_margin_start"))
                .alias("team_margin_at_quarter_start")
            ])

            # Calculate margin impact and minutes ONLY FOR STARTERS
            quarter_stats = quarter_stats.with_columns([
                # Margin impact: only calculated if player started the quarter
                pl.when(pl.col("starts_quarter") & pl.col("point_diff_at_out").is_not_null())
                .then(pl.col("point_diff_at_out") - pl.col("team_margin_at_quarter_start"))
                .otherwise(None)
                .alias("margin_change_until_out"),

                # Minutes played:
                # - If started and got subbed out: minute_OUT - quarter_start
                # - If started and never subbed out: full quarter (end_min - start_min)
                # - If didn't start: 0
                pl.when(pl.col("starts_quarter") & pl.col("first_out_minute").is_not_null())
                .then((pl.col("first_out_minute") - start_min).cast(pl.Float64))
                .when(pl.col("starts_quarter") & pl.col("first_out_minute").is_null())
                .then(pl.lit(end_min - start_min).cast(pl.Float64))
                .otherwise(0.0)
                .alias("minutes_until_out")
            ])

            # Select relevant columns
            quarter_stats_final = quarter_stats.select([
                "game_uuid",
                "player_uuid",
                "quarter",
                "starts_quarter",
                "first_out_minute",
                "margin_change_until_out",
                "minutes_until_out"
            ])

            all_quarter_stats.append(quarter_stats_final)

        # Combine all quarters
        combined_quarters = pl.concat(all_quarter_stats)

        # Pivot to get per-quarter columns
        final_stats = combined_quarters.pivot(
            values=["starts_quarter", "first_out_minute", "margin_change_until_out", "minutes_until_out"],
            index=["game_uuid", "player_uuid"],
            columns="quarter",
            aggregate_function="first"
        )

        # Rename columns
        final_stats = final_stats.rename({
            "starts_quarter_1": "started_q1",
            "starts_quarter_2": "started_q2",
            "starts_quarter_3": "started_q3",
            "starts_quarter_4": "started_q4",
            "first_out_minute_1": "q1_subbed_out_minute",
            "first_out_minute_2": "q2_subbed_out_minute",
            "first_out_minute_3": "q3_subbed_out_minute",
            "first_out_minute_4": "q4_subbed_out_minute",
            "margin_change_until_out_1": "q1_margin_impact",
            "margin_change_until_out_2": "q2_margin_impact",
            "margin_change_until_out_3": "q3_margin_impact",
            "margin_change_until_out_4": "q4_margin_impact",
            "minutes_until_out_1": "q1_minutes_played",
            "minutes_until_out_2": "q2_minutes_played",
            "minutes_until_out_3": "q3_minutes_played",
            "minutes_until_out_4": "q4_minutes_played"
        })

        # Calculate summary statistics
        final_stats = final_stats.with_columns([
            # Count quarters started
            (pl.col("started_q1").cast(pl.Int64) +
             pl.col("started_q2").cast(pl.Int64) +
             pl.col("started_q3").cast(pl.Int64) +
             pl.col("started_q4").cast(pl.Int64)).alias("total_quarters_started"),

            # Average margin impact when starting
            pl.when(
                (pl.col("started_q1") & pl.col("q1_margin_impact").is_not_null()) |
                (pl.col("started_q2") & pl.col("q2_margin_impact").is_not_null()) |
                (pl.col("started_q3") & pl.col("q3_margin_impact").is_not_null()) |
                (pl.col("started_q4") & pl.col("q4_margin_impact").is_not_null())
            )
            .then(
                pl.concat_list([
                    pl.when(pl.col("started_q1")).then(pl.col("q1_margin_impact")),
                    pl.when(pl.col("started_q2")).then(pl.col("q2_margin_impact")),
                    pl.when(pl.col("started_q3")).then(pl.col("q3_margin_impact")),
                    pl.when(pl.col("started_q4")).then(pl.col("q4_margin_impact"))
                ]).list.mean()
            )
            .otherwise(None)
            .alias("avg_margin_impact_when_starting")
        ])

        print(f">> Found {len(final_stats)} player game quarter stat records")

        playergamequarterstats_enriched = CustomDF(
            "playergamequarterstats_enriched", initial_df=final_stats
        )
        playergamequarterstats_enriched.write_table()

    elif table_name == "playergame_shots_enriched":
        # Load source data
        shots_data = CustomDF("playergameshotsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")

        print("\n>> Building per-game shot location data...")

        # Get game metadata (season, date)
        game_metadata = game_data.custom_select(["game_uuid", "season", "game_time"]).custom_distinct()

        # Join shots with game metadata
        shots_with_metadata = shots_data.custom_join(
            game_metadata,
            custom_on="game_uuid",
            custom_how="left"
        )

        # Group by player and game, collect shot locations by type
        shots_per_game = shots_with_metadata.custom_groupby(
            ["player_uuid", "game_uuid", "season", "game_time"],
            # Collect 2PT locations (shot_type contains '2' or is 'TWO')
            pl.when(
                (pl.col("shot_type").str.contains("2")) |
                (pl.col("shot_type").str.to_uppercase() == "TWO")
            ).then(
                pl.struct([
                    pl.col("xnormalize").cast(pl.Float64).alias("xnormalize"),
                    pl.col("ynormalize").cast(pl.Float64).alias("ynormalize")
                ])
            ).filter(pl.col("shot_type").is_not_null()).alias("twopoint_locations"),

            # Collect 3PT locations (shot_type contains '3' or is 'THREE')
            pl.when(
                (pl.col("shot_type").str.contains("3")) |
                (pl.col("shot_type").str.to_uppercase() == "THREE")
            ).then(
                pl.struct([
                    pl.col("xnormalize").cast(pl.Float64).alias("xnormalize"),
                    pl.col("ynormalize").cast(pl.Float64).alias("ynormalize")
                ])
            ).filter(pl.col("shot_type").is_not_null()).alias("threepoint_locations"),

            # Count made/attempted for validation
            pl.when(
                (pl.col("shot_type").str.contains("2")) |
                (pl.col("shot_type").str.to_uppercase() == "TWO")
            ).then(1).sum().alias("two_pt_attempted"),

            pl.when(
                ((pl.col("shot_type").str.contains("2")) |
                 (pl.col("shot_type").str.to_uppercase() == "TWO")) &
                (pl.col("outcome").str.to_uppercase() == "MADE")
            ).then(1).sum().alias("two_pt_made"),

            pl.when(
                (pl.col("shot_type").str.contains("3")) |
                (pl.col("shot_type").str.to_uppercase() == "THREE")
            ).then(1).sum().alias("three_pt_attempted"),

            pl.when(
                ((pl.col("shot_type").str.contains("3")) |
                 (pl.col("shot_type").str.to_uppercase() == "THREE")) &
                (pl.col("outcome").str.to_uppercase() == "MADE")
            ).then(1).sum().alias("three_pt_made"),
        )

        # Add game_date column and drop game_time
        shots_per_game.data = shots_per_game.data.with_columns([
            pl.col("game_time").cast(pl.Date).alias("game_date")
        ])

        # Fill nulls for counts and cast to Int64
        shots_per_game.data = shots_per_game.data.with_columns([
            pl.col("two_pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("two_pt_made").fill_null(0).cast(pl.Int64),
            pl.col("three_pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("three_pt_made").fill_null(0).cast(pl.Int64),
        ])

        print(f">> Found {len(shots_per_game.data)} player-game shot records")

        # Select final columns in correct order
        shots_final = shots_per_game.custom_select([
            "player_uuid",
            "season",
            "game_uuid",
            "game_date",
            "twopoint_locations",
            "threepoint_locations",
            "two_pt_made",
            "two_pt_attempted",
            "three_pt_made",
            "three_pt_attempted",
        ])

        CustomDF(
            "playergame_shots_enriched",
            initial_df=shots_final.data
        ).write_table()

    elif table_name == "playergameanalytics_enriched":
        player_stats = CustomDF("playergamestatsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")
        player_data = CustomDF("playerdata_datamodel")
        team_data = CustomDF("teamdata_datamodel")
        playergameplusminus_enriched = CustomDF("playergameplusminus_enriched")
        playergameimpact_enriched = CustomDF("playergameimpact_enriched")

        
        game_data = game_data.custom_join(
            game_data.custom_select(["game_uuid", "team_uuid"]),
            custom_on=["game_uuid"],
            custom_how="inner",
            custom_suffix="_opponent",
        )

        game_data.data = game_data.data.filter(
            pl.col("team_uuid") != pl.col("team_uuid_opponent")
        )
        
        player_data = player_data.custom_select(
            ["player_uuid", "team_uuid", "player_name", "player_number"]
        )

        team_data = team_data.custom_select(
            ["team_uuid", "team_name","team_short_name"]
        ).custom_distinct()

        player_data.data = player_data.data.unique(subset=["player_uuid"], keep="first")
        team_data.data = team_data.data.unique(subset=["team_uuid"], keep="first")


        player_stats = player_stats.custom_join(
            game_data.custom_select(["game_uuid", "season","game_time"]).custom_distinct(),
            custom_on=["game_uuid"],
            custom_how="left",
        )
        player_stats = player_stats.custom_join(
            player_data,
            custom_on=["player_uuid"],
            custom_how="left",
        )
        player_stats.data = player_stats.data.filter(pl.col("did_play") == 1)

        player_stats = player_stats.custom_join(
            playergameplusminus_enriched.custom_drop(["from_date", "to_date", "RecordID"]),
            custom_on=["player_uuid", "game_uuid"],
            custom_how="left",
        )

        player_stats = player_stats.custom_join(
            playergameimpact_enriched.custom_drop(["from_date", "to_date", "RecordID"]),
            custom_on=["player_uuid", "game_uuid"],
            custom_how="left",
        )

        player_stats = player_stats.custom_join(
            game_data.custom_select(
                [
                    "game_uuid",
                    "team_uuid",
                    "team_uuid_opponent",
                    "game_time",
                    "season",
                    "team_type",
                ]
            ),
            custom_on=["game_uuid", "team_uuid"],
            custom_how="left",
        ).custom_join(
            team_data.custom_select(["team_uuid", "team_name","team_short_name"]).custom_distinct(),
            custom_on=["team_uuid"],
            custom_how="left",
        ).custom_join(
            team_data.custom_select(["team_uuid", "team_name","team_short_name"]).custom_distinct(),
            custom_left_on=["team_uuid_opponent"],
            custom_right_on=["team_uuid"],
            custom_how="left",
            custom_suffix="_opponent",
        )

        
        player_stats.data = player_stats.data.with_columns(
            pl.col("game_time").cast(pl.Date())
        )

        playergamequarterstats_enriched = CustomDF("playergamequarterstats_enriched")

        player_stats = player_stats.custom_join(
            playergamequarterstats_enriched.custom_drop(["from_date", "to_date", "RecordID"]),
            custom_on=["player_uuid", "game_uuid"],
            custom_how="left",
        )

        # Join shot locations
        playergame_shots_enriched = CustomDF("playergame_shots_enriched")
        player_stats = player_stats.custom_join(
            playergame_shots_enriched.custom_drop(["from_date", "to_date", "RecordID", "season", "game_date", "two_pt_made", "two_pt_attempted", "three_pt_made", "three_pt_attempted"]),
            custom_on=["player_uuid", "game_uuid"],
            custom_how="left",
        )

        # Join clutch performance
        playerclutchperformance_enriched = CustomDF("playerclutchperformance_enriched")
        player_stats = player_stats.custom_join(
            playerclutchperformance_enriched.custom_drop(["from_date", "to_date", "RecordID", "season", "game_date"]),
            custom_on=["player_uuid", "game_uuid"],
            custom_how="left",
        )

        player_stats = player_stats.custom_select(
            [
                "player_uuid",
                "player_name",
                "player_number",
                "team_uuid",
                "team_name",
                "team_short_name",
                "team_uuid_opponent",
                "team_name_opponent",
                "team_short_name_opponent",
                "season",
                "game_uuid",
                "game_time",
                "points",
                "ft_attempted",
                "ft_made",
                "two_made",
                "three_made",
                "minutes_played",
                "total_plus_minus",
                "avg_plus_minus",
                "offensive_points_on_court",
                "defensive_points_on_court",
                "offensive_points_per_minute",
                "defensive_points_per_minute",
                "started_q1",
                "started_q2",
                "started_q3",
                "started_q4",
                "q1_subbed_out_minute",
                "q2_subbed_out_minute",
                "q3_subbed_out_minute",
                "q4_subbed_out_minute",
                "q1_margin_impact",
                "q2_margin_impact",
                "q3_margin_impact",
                "q4_margin_impact",
                "q1_minutes_played",
                "q2_minutes_played",
                "q3_minutes_played",
                "q4_minutes_played",
                "total_quarters_started",
                "avg_margin_impact_when_starting",
                "twopoint_locations",
                "threepoint_locations",
                "is_clutch_game",
                "clutch_minutes_played",
                "clutch_points",
                "clutch_2pt_made",
                "clutch_2pt_attempted",
                "clutch_3pt_made",
                "clutch_3pt_attempted",
                "clutch_fg_pct",
                "game_result",
                "was_on_court_during_clutch",
            ]
        ).custom_distinct()

        playergameanalytics_enriched = CustomDF(
            "playergameanalytics_enriched", initial_df=player_stats.data
        )
        playergameanalytics_enriched.write_table()

    elif table_name == "teamhomeawaysplits_enriched":
        gamedata = CustomDF("gamedata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")

        # Join game data with team stats
        game_stats = gamedata.custom_join(
            teamgamestats,
            custom_on=["game_uuid", "team_uuid"],
            custom_how="inner",
        )

        # Self-join to get opponent stats
        game_stats_opponent = teamgamestats.custom_select(["game_uuid", "points"])
        game_stats = game_stats.custom_join(
            game_stats_opponent,
            custom_on=["game_uuid"],
            custom_how="inner",
            custom_suffix="_opponent",
        )

        # Filter and calculate metrics in one pass
        game_stats.data = game_stats.data.filter(
            pl.col("points") != pl.col("points_opponent")
        ).with_columns([
            (pl.col("points") - pl.col("points_opponent")).alias("margin"),
            (pl.col("points") > pl.col("points_opponent")).cast(pl.Int32).alias("win"),
            pl.col("game_time").dt.weekday().alias("day_of_week"),
            pl.col("game_time").dt.hour().alias("hour_of_day"),
        ])

        # Split by home/away and aggregate
        home_games = game_stats.custom_select([
            "team_uuid", "season", "team_type", "points", "points_opponent",
            "margin", "win", "day_of_week", "hour_of_day"
        ])
        home_games.data = home_games.data.filter(pl.col("team_type") == "home")

        away_games = game_stats.custom_select([
            "team_uuid", "season", "team_type", "points", "points_opponent",
            "margin", "win"
        ])
        away_games.data = away_games.data.filter(pl.col("team_type") == "away")

        # Aggregate home statistics
        home_agg = home_games.custom_groupby(
            ["team_uuid", "season"],
            pl.len().alias("home_games"),
            pl.mean("points").alias("home_avg_points"),
            pl.mean("points_opponent").alias("home_avg_points_allowed"),
            pl.mean("margin").alias("home_avg_margin"),
            pl.mean("win").alias("home_win_rate"),
            pl.col("day_of_week").mode().first().alias("typical_home_game_day_num"),
            pl.col("hour_of_day").mode().first().alias("typical_home_game_hour"),
        )

        # Aggregate away statistics
        away_agg = away_games.custom_groupby(
            ["team_uuid", "season"],
            pl.len().alias("away_games"),
            pl.mean("points").alias("away_avg_points"),
            pl.mean("points_opponent").alias("away_avg_points_allowed"),
            pl.mean("margin").alias("away_avg_margin"),
            pl.mean("win").alias("away_win_rate"),
        )

        # Join home and away stats
        splits = home_agg.custom_join(
            away_agg,
            custom_on=["team_uuid", "season"],
            custom_how="outer",
        )

        # Calculate derived metrics and map day names
        day_mapping = {
            0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday",
            4: "Friday", 5: "Saturday", 6: "Sunday"
        }
        splits.data = splits.data.with_columns([
            (pl.col("home_avg_points") - pl.col("away_avg_points")).alias("home_away_point_diff"),
            (pl.col("home_avg_margin") - pl.col("away_avg_margin")).alias("home_court_advantage"),
            pl.col("typical_home_game_day_num").replace(day_mapping, default=None).alias("typical_home_game_day"),
            pl.col("typical_home_game_hour").cast(pl.Int64).alias("typical_home_game_hour")
        ]).drop("typical_home_game_day_num")

        # Join with team names
        teamdata = teamdata.custom_select(["team_uuid", "team_name", "team_short_name"])
        teamdata.data = teamdata.data.unique(subset=["team_uuid"], keep="first")

        splits = splits.custom_join(
            teamdata,
            custom_on=["team_uuid"],
            custom_how="left",
        )

        # Select final columns
        splits = splits.custom_select([
            "team_uuid", "team_name", "team_short_name", "season",
            "home_games", "away_games",
            "home_avg_points", "away_avg_points",
            "home_avg_points_allowed", "away_avg_points_allowed",
            "home_avg_margin", "away_avg_margin",
            "home_win_rate", "away_win_rate",
            "home_away_point_diff", "home_court_advantage",
            "typical_home_game_day", "typical_home_game_hour",
        ])

        teamhomeawaysplits_enriched = CustomDF(
            "teamhomeawaysplits_enriched", initial_df=splits.data
        )
        teamhomeawaysplits_enriched.write_table()

    elif table_name == "teamquarterperformance_enriched":
        gamedata = CustomDF("gamedata_datamodel")
        gamescores = CustomDF("gameteamscoresdata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")

        # Filter to regulation quarters and sort
        gamescores.data = gamescores.data.filter(
            pl.col("quarter").is_between(1, 4)
        )

        # Join with game data
        game_quarter_scores = gamedata.custom_join(
            gamescores,
            custom_on=["game_uuid"],
            custom_how="inner",
        )

        # Sort and get end-of-quarter scores
        game_quarter_scores.data = game_quarter_scores.data.sort(
            ["game_uuid", "team_uuid", "quarter", "minuteAbsolute"]
        )

        quarter_final = game_quarter_scores.custom_groupby(
            ["game_uuid", "team_uuid", "season", "quarter", "home_team_uuid", "away_team_uuid"],
            pl.last("home_score").alias("home_score_end"),
            pl.last("away_score").alias("away_score_end"),
        )

        # Calculate all metrics in optimized passes
        quarter_final.data = quarter_final.data.sort(
            ["game_uuid", "team_uuid", "quarter"]
        ).with_columns([
            # Start-of-quarter scores
            pl.col("home_score_end").shift(1).over(["game_uuid", "team_uuid"]).fill_null(0).alias("home_score_start"),
            pl.col("away_score_end").shift(1).over(["game_uuid", "team_uuid"]).fill_null(0).alias("away_score_start"),
        ]).with_columns([
            # Quarter points
            (pl.col("home_score_end") - pl.col("home_score_start")).alias("home_quarter_points"),
            (pl.col("away_score_end") - pl.col("away_score_start")).alias("away_quarter_points"),
        ]).with_columns([
            # Team's perspective
            pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
            .then(pl.col("home_quarter_points"))
            .otherwise(pl.col("away_quarter_points"))
            .alias("team_points"),
            pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
            .then(pl.col("away_quarter_points"))
            .otherwise(pl.col("home_quarter_points"))
            .alias("opponent_points"),
            # Game margins
            pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
            .then(pl.col("home_score_start") - pl.col("away_score_start"))
            .otherwise(pl.col("away_score_start") - pl.col("home_score_start"))
            .alias("game_margin_start"),
            pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
            .then(pl.col("home_score_end") - pl.col("away_score_end"))
            .otherwise(pl.col("away_score_end") - pl.col("home_score_end"))
            .alias("game_margin_end"),
        ]).with_columns([
            # Derived metrics
            (pl.col("team_points") - pl.col("opponent_points")).alias("quarter_margin"),
            (pl.col("team_points") > pl.col("opponent_points")).cast(pl.Int32).alias("quarter_win"),
            ((pl.col("game_margin_start") < 0) & (pl.col("game_margin_end") > 0)).cast(pl.Int32).alias("comeback_win"),
            ((pl.col("game_margin_start") > 0) & (pl.col("game_margin_end") < 0)).cast(pl.Int32).alias("lead_blown"),
        ])

        # Select relevant columns and aggregate
        quarter_stats = quarter_final.custom_select([
            "team_uuid", "season", "quarter", "team_points", "opponent_points",
            "quarter_margin", "quarter_win", "comeback_win", "lead_blown"
        ])

        quarter_agg = quarter_stats.custom_groupby(
            ["team_uuid", "season", "quarter"],
            pl.len().alias("total_games"),
            pl.mean("team_points").alias("avg_points"),
            pl.mean("opponent_points").alias("avg_points_allowed"),
            pl.mean("quarter_margin").alias("avg_point_margin"),
            pl.mean("quarter_win").alias("quarter_win_rate"),
            pl.max("team_points").alias("max_scoring_run"),
            pl.mean("team_points").alias("avg_largest_run"),
            pl.sum("comeback_win").cast(pl.UInt32).alias("comeback_wins"),
            pl.sum("lead_blown").cast(pl.UInt32).alias("lead_blown"),
        )

        # Join with team names
        teamdata = teamdata.custom_select(["team_uuid", "team_name", "team_short_name"])
        teamdata.data = teamdata.data.unique(subset=["team_uuid"], keep="first")

        quarter_performance = quarter_agg.custom_join(
            teamdata,
            custom_on=["team_uuid"],
            custom_how="left",
        ).custom_select([
            "team_uuid", "team_name", "team_short_name", "season", "quarter",
            "total_games", "avg_points", "avg_points_allowed", "avg_point_margin",
            "quarter_win_rate", "max_scoring_run", "avg_largest_run",
            "comeback_wins", "lead_blown",
        ])

        CustomDF(
            "teamquarterperformance_enriched",
            initial_df=quarter_performance.data
        ).write_table()

    elif table_name == "threeplayer_combinations_enriched":
        # Load necessary data
        from collections import defaultdict
        from itertools import combinations

        gamedata = CustomDF("gamedata_datamodel")
        subs_df = CustomDF("playergamesubstitionsgamedata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")
        playerdata = CustomDF("playerdata_datamodel")

        print("\n>> Building per-game 3-player combinations from substitution patterns...")

        # Get all teams and seasons
        teams_seasons = gamedata.data.select(["team_uuid", "season"]).unique()

        # Track 3-player combination data PER GAME (not aggregated)
        all_combo_data = []

        for team_season in teams_seasons.iter_rows(named=True):
            team_uuid = team_season["team_uuid"]
            season = team_season["season"]

            # Get games for this team/season
            team_games = gamedata.data.filter(
                (pl.col("team_uuid") == team_uuid) &
                (pl.col("season") == season)
            )
            game_uuids = team_games["game_uuid"].to_list()

            if len(game_uuids) == 0:
                continue

            # Get team player UUIDs
            team_player_uuids = playerdata.data.filter(
                pl.col("team_uuid") == team_uuid
            )["player_uuid"].to_list()

            if len(team_player_uuids) == 0:
                continue

            # Filter substitutions for this team (exclude DNP players with NULL values)
            team_subs = subs_df.data.filter(
                (pl.col("game_uuid").is_in(game_uuids)) &
                (pl.col("player_uuid").is_in(team_player_uuids)) &
                pl.col("minute_absolute").is_not_null() &
                pl.col("type").is_not_null()
            ).sort(["game_uuid", "minute_absolute"])

            # Get game metadata
            game_metadata = {}
            for game_row in team_games.iter_rows(named=True):
                game_uuid = game_row["game_uuid"]
                game_metadata[game_uuid] = {
                    "game_date": game_row.get("game_time"),
                    "opponent": game_row.get("opponent_team_name", "")
                }

            # Process each game to build PER-GAME 3-player combination records
            for game_uuid in game_uuids:
                game_subs = team_subs.filter(pl.col("game_uuid") == game_uuid).sort("minute_absolute")

                if len(game_subs) == 0:
                    continue

                # Get game metadata
                game_date = game_metadata[game_uuid]["game_date"]
                opponent = game_metadata[game_uuid]["opponent"]

                # Track combo stats PER GAME
                game_combo_stats = defaultdict(lambda: {
                    "minutes": 0,
                    "plus_minus": 0,
                })

                on_court = set()
                prev_minute = 0
                prev_point_diff = 0

                for row in game_subs.iter_rows(named=True):
                    player_uuid = row["player_uuid"]
                    minute = row["minute_absolute"]
                    sub_type = row["type"]
                    current_point_diff = row["point_diff"]

                    # For every 5-player lineup, generate all 3-player combinations
                    if len(on_court) == 5 and minute > prev_minute:
                        minutes_played = minute - prev_minute
                        point_diff_change = current_point_diff - prev_point_diff

                        # Generate all 3-player combinations from the 5-player lineup
                        for combo in combinations(sorted(on_court), 3):
                            game_combo_stats[combo]["minutes"] += minutes_played
                            game_combo_stats[combo]["plus_minus"] += point_diff_change

                    if sub_type == "IN_TYPE":
                        on_court.add(player_uuid)
                    elif sub_type == "OUT_TYPE":
                        on_court.discard(player_uuid)

                    prev_minute = minute
                    prev_point_diff = current_point_diff

                # Handle final lineup
                if len(on_court) == 5 and prev_minute < 40:
                    minutes_played = 40 - prev_minute

                    # Get final point diff from last substitution event
                    final_subs = game_subs.tail(1)
                    if len(final_subs) > 0:
                        final_point_diff = final_subs["point_diff"][0]
                        point_diff_change = final_point_diff - prev_point_diff
                    else:
                        point_diff_change = 0

                    for combo in combinations(sorted(on_court), 3):
                        game_combo_stats[combo]["minutes"] += minutes_played
                        game_combo_stats[combo]["plus_minus"] += point_diff_change

                # Create a record for each combo in this game (min 1 minute threshold)
                for combo_key, stats in game_combo_stats.items():
                    minutes = stats["minutes"]

                    # Filter: only include combos with >= 1 minute in this game
                    if minutes < 1:
                        continue

                    plus_minus = stats["plus_minus"]

                    # Determine court result based on plus/minus
                    if plus_minus > 0:
                        court_result = "Won"
                    elif plus_minus < 0:
                        court_result = "Lost"
                    else:
                        court_result = "Draw"

                    # Calculate win_rate based on plus_minus (1.0 if won, 0.0 if lost, 0.5 if draw)
                    if plus_minus > 0:
                        win_rate = 1.0
                    elif plus_minus < 0:
                        win_rate = 0.0
                    else:
                        win_rate = 0.5

                    # Create combo_id
                    combo_list = list(combo_key)
                    combo_id = "_".join(combo_list)

                    all_combo_data.append({
                        "game_uuid": game_uuid,
                        "team_uuid": team_uuid,
                        "season": season,
                        "game_date": game_date,
                        "opponent": opponent,
                        "combo_id": combo_id,
                        "player_1_uuid": combo_list[0],
                        "player_2_uuid": combo_list[1],
                        "player_3_uuid": combo_list[2],
                        "minutes": minutes,
                        "plus_minus": plus_minus,
                        "court_result": court_result,
                        "win_rate": win_rate,
                    })

        print(f">> Found {len(all_combo_data)} per-game 3-player combination records")

        # Convert to DataFrame
        combo_df = pl.DataFrame(all_combo_data)

        # Join with team names
        teamdata_names = teamdata.custom_select(["team_uuid", "team_name"])
        teamdata_names.data = teamdata_names.data.unique(subset=["team_uuid"], keep="first")

        combo_df = combo_df.join(
            teamdata_names.data,
            on="team_uuid",
            how="left"
        )

        # Cast and select final columns
        combo_df = combo_df.with_columns([
            pl.col("game_uuid").cast(pl.String),
            pl.col("team_uuid").cast(pl.String),
            pl.col("team_name").cast(pl.String),
            pl.col("season").cast(pl.Int64),
            pl.col("game_date").cast(pl.Date),
            pl.col("opponent").cast(pl.String),
            pl.col("combo_id").cast(pl.String),
            pl.col("minutes").cast(pl.Float64),
            pl.col("plus_minus").cast(pl.Int64),
            pl.col("court_result").cast(pl.String),
            pl.col("win_rate").cast(pl.Float64),
        ]).select([
            "game_uuid", "team_uuid", "team_name", "season", "game_date", "opponent", "combo_id",
            "player_1_uuid", "player_2_uuid", "player_3_uuid",
            "minutes", "plus_minus", "court_result", "win_rate",
        ])

        CustomDF(
            "threeplayer_combinations_enriched",
            initial_df=combo_df
        ).write_table()

    elif table_name == "fiveplayer_combinations_enriched":
        # Load necessary data
        from collections import defaultdict

        gamedata = CustomDF("gamedata_datamodel")
        subs_df = CustomDF("playergamesubstitionsgamedata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")
        playerdata = CustomDF("playerdata_datamodel")

        print("\n>> Building per-game lineup data from substitution patterns...")

        # Get all teams and seasons
        teams_seasons = gamedata.data.select(["team_uuid", "season"]).unique()

        # Track lineup combinations PER GAME (not aggregated)
        all_lineup_data = []

        for team_season in teams_seasons.iter_rows(named=True):
            team_uuid = team_season["team_uuid"]
            season = team_season["season"]

            # Get games for this team/season
            team_games = gamedata.data.filter(
                (pl.col("team_uuid") == team_uuid) &
                (pl.col("season") == season)
            )
            game_uuids = team_games["game_uuid"].to_list()

            if len(game_uuids) == 0:
                continue

            # Get team player UUIDs
            team_player_uuids = playerdata.data.filter(
                pl.col("team_uuid") == team_uuid
            )["player_uuid"].to_list()

            if len(team_player_uuids) == 0:
                continue

            # Filter substitutions for this team (exclude DNP players with NULL values)
            team_subs = subs_df.data.filter(
                (pl.col("game_uuid").is_in(game_uuids)) &
                (pl.col("player_uuid").is_in(team_player_uuids)) &
                pl.col("minute_absolute").is_not_null() &
                pl.col("type").is_not_null()
            ).sort(["game_uuid", "minute_absolute"])

            # Get game metadata for joining
            game_metadata = {}
            for game_row in team_games.iter_rows(named=True):
                game_uuid = game_row["game_uuid"]
                game_metadata[game_uuid] = {
                    "game_date": game_row.get("game_time"),
                    "opponent": game_row.get("opponent_team_name", "")
                }

            # Process each game to build PER-GAME lineup records
            for game_uuid in game_uuids:
                game_subs = team_subs.filter(pl.col("game_uuid") == game_uuid).sort("minute_absolute")

                if len(game_subs) == 0:
                    continue

                # Get game metadata
                game_date = game_metadata[game_uuid]["game_date"]
                opponent = game_metadata[game_uuid]["opponent"]

                # Track lineup stats PER GAME
                game_lineup_stats = defaultdict(lambda: {
                    "minutes": 0,
                    "plus_minus": 0,
                })

                on_court = set()
                prev_minute = 0
                prev_point_diff = 0

                for row in game_subs.iter_rows(named=True):
                    player_uuid = row["player_uuid"]
                    minute = row["minute_absolute"]
                    sub_type = row["type"]
                    current_point_diff = row["point_diff"]

                    # Record lineup before substitution
                    if len(on_court) == 5 and minute > prev_minute:
                        lineup_key = tuple(sorted(on_court))
                        minutes_played = minute - prev_minute
                        point_diff_change = current_point_diff - prev_point_diff

                        game_lineup_stats[lineup_key]["minutes"] += minutes_played
                        game_lineup_stats[lineup_key]["plus_minus"] += point_diff_change

                    # Apply substitution
                    if sub_type == "IN_TYPE":
                        on_court.add(player_uuid)
                    elif sub_type == "OUT_TYPE":
                        on_court.discard(player_uuid)

                    prev_minute = minute
                    prev_point_diff = current_point_diff

                # Final lineup until end of game (40 minutes)
                if len(on_court) == 5 and prev_minute < 40:
                    lineup_key = tuple(sorted(on_court))
                    minutes_played = 40 - prev_minute

                    # Get final point diff from last substitution event
                    final_subs = game_subs.tail(1)
                    if len(final_subs) > 0:
                        final_point_diff = final_subs["point_diff"][0]
                        point_diff_change = final_point_diff - prev_point_diff
                    else:
                        point_diff_change = 0

                    game_lineup_stats[lineup_key]["minutes"] += minutes_played
                    game_lineup_stats[lineup_key]["plus_minus"] += point_diff_change

                # Create a record for each lineup in this game (min 1 minute threshold)
                for lineup_key, stats in game_lineup_stats.items():
                    minutes = stats["minutes"]

                    # Filter: only include lineups with >= 1 minute in this game
                    if minutes < 1:
                        continue

                    plus_minus = stats["plus_minus"]

                    # Determine court result based on plus/minus
                    if plus_minus > 0:
                        court_result = "Won"
                    elif plus_minus < 0:
                        court_result = "Lost"
                    else:
                        court_result = "Draw"

                    # Calculate win_rate based on plus_minus (1.0 if won, 0.0 if lost, 0.5 if draw)
                    if plus_minus > 0:
                        win_rate = 1.0
                    elif plus_minus < 0:
                        win_rate = 0.0
                    else:
                        win_rate = 0.5

                    # Create lineup_id
                    lineup_list = list(lineup_key)
                    lineup_id = "_".join(lineup_list)

                    all_lineup_data.append({
                        "game_uuid": game_uuid,
                        "team_uuid": team_uuid,
                        "season": season,
                        "game_date": game_date,
                        "opponent": opponent,
                        "lineup_id": lineup_id,
                        "player_1_uuid": lineup_list[0],
                        "player_2_uuid": lineup_list[1],
                        "player_3_uuid": lineup_list[2],
                        "player_4_uuid": lineup_list[3],
                        "player_5_uuid": lineup_list[4],
                        "minutes": minutes,
                        "plus_minus": plus_minus,
                        "court_result": court_result,
                        "win_rate": win_rate,
                    })

        print(f">> Found {len(all_lineup_data)} per-game lineup records")

        # Convert to DataFrame
        lineup_df = pl.DataFrame(all_lineup_data)

        # Join with team names
        teamdata_names = teamdata.custom_select(["team_uuid", "team_name"])
        teamdata_names.data = teamdata_names.data.unique(subset=["team_uuid"], keep="first")

        lineup_df = lineup_df.join(
            teamdata_names.data,
            on="team_uuid",
            how="left"
        )

        # Cast and select final columns
        lineup_df = lineup_df.with_columns([
            pl.col("game_uuid").cast(pl.String),
            pl.col("team_uuid").cast(pl.String),
            pl.col("team_name").cast(pl.String),
            pl.col("season").cast(pl.Int64),
            pl.col("game_date").cast(pl.Date),
            pl.col("opponent").cast(pl.String),
            pl.col("lineup_id").cast(pl.String),
            pl.col("minutes").cast(pl.Float64),
            pl.col("plus_minus").cast(pl.Int64),
            pl.col("court_result").cast(pl.String),
            pl.col("win_rate").cast(pl.Float64),
        ]).select([
            "game_uuid", "team_uuid", "team_name", "season", "game_date", "opponent", "lineup_id",
            "player_1_uuid", "player_2_uuid", "player_3_uuid", "player_4_uuid", "player_5_uuid",
            "minutes", "plus_minus", "court_result", "win_rate",
        ])

        CustomDF(
            "fiveplayer_combinations_enriched",
            initial_df=lineup_df
        ).write_table()

    elif table_name == "gamecompetitiveness_enriched":
        gamedata = CustomDF("gamedata_datamodel")
        gamescores = CustomDF("gameteamscoresdata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")

        # Get final scores for each game
        game_final = teamgamestats.custom_select([
            "game_uuid", "team_uuid", "points"
        ])

        # Join to get home and away team scores
        gamedata_teams = gamedata.custom_select([
            "game_uuid", "team_uuid", "team_type", "season", "game_time"
        ])

        game_scores_home = game_final.custom_join(
            gamedata_teams,
            custom_on=["game_uuid", "team_uuid"],
            custom_how="inner"
        )
        game_scores_home.data = game_scores_home.data.filter(pl.col("team_type") == "home")
        game_scores_home = game_scores_home.custom_select([
            "game_uuid", "season", "game_time", "team_uuid", "points"
        ])
        game_scores_home.data = game_scores_home.data.rename({
            "team_uuid": "home_team_uuid",
            "points": "home_final_score"
        })

        game_scores_away = game_final.custom_join(
            gamedata_teams,
            custom_on=["game_uuid", "team_uuid"],
            custom_how="inner"
        )
        game_scores_away.data = game_scores_away.data.filter(pl.col("team_type") == "away")
        game_scores_away = game_scores_away.custom_select([
            "game_uuid", "team_uuid", "points"
        ])
        game_scores_away.data = game_scores_away.data.rename({
            "team_uuid": "away_team_uuid",
            "points": "away_final_score"
        })

        # Combine home and away
        game_competitive = game_scores_home.custom_join(
            game_scores_away,
            custom_on=["game_uuid"],
            custom_how="inner"
        )

        # Calculate final margin and determine winner
        game_competitive.data = game_competitive.data.with_columns([
            pl.col("game_time").cast(pl.Date).alias("game_date"),
            (pl.col("home_final_score") - pl.col("away_final_score")).abs().alias("final_margin"),
            pl.when(pl.col("home_final_score") > pl.col("away_final_score"))
            .then(pl.col("home_team_uuid"))
            .otherwise(pl.col("away_team_uuid"))
            .alias("winning_team_uuid")
        ])

        # Analyze score progression for lead changes and largest lead
        gamescores_sorted = gamescores.custom_select([
            "game_uuid", "minuteAbsolute", "home_score", "away_score", "home_team_uuid", "away_team_uuid"
        ])
        gamescores_sorted.data = gamescores_sorted.data.sort(["game_uuid", "minuteAbsolute"])

        # Calculate lead at each score change
        gamescores_sorted.data = gamescores_sorted.data.with_columns([
            (pl.col("home_score") - pl.col("away_score")).alias("lead"),
            (pl.col("home_score") - pl.col("away_score")).shift(1).over("game_uuid").alias("prev_lead")
        ])

        # Aggregate per game
        game_flow = gamescores_sorted.custom_groupby(
            ["game_uuid"],
            pl.col("lead").abs().max().alias("largest_lead"),
            ((pl.col("lead") > 0) != (pl.col("prev_lead") > 0)).sum().alias("lead_changes"),
        )

        # Determine when game stopped being competitive (margin <= 5)
        gamescores_sorted.data = gamescores_sorted.data.with_columns(
            (pl.col("lead").abs() <= 5).alias("is_close")
        )

        # Get last minute when game was within 5 points
        competitive_minute = gamescores_sorted.data.group_by("game_uuid").agg([
            pl.when(pl.col("is_close").any())
            .then(pl.col("minuteAbsolute").filter(pl.col("is_close")).max())
            .otherwise(0)
            .alias("competitive_until_minute")
        ])

        game_competitive.data = game_competitive.data.join(
            game_flow.data,
            on="game_uuid",
            how="left"
        ).join(
            competitive_minute,
            on="game_uuid",
            how="left"
        )

        # Classify game type and detect comebacks
        game_competitive.data = game_competitive.data.with_columns([
            pl.when(pl.col("final_margin") <= 5)
            .then(pl.lit("competitive"))
            .when(pl.col("final_margin") <= 15)
            .then(pl.lit("comfortable"))
            .otherwise(pl.lit("blowout"))
            .alias("game_type"),
            pl.lit(False).alias("comeback_win"),  # Simplified - would need Q4 start data
            pl.col("season").cast(pl.Int64),
            pl.col("home_final_score").cast(pl.Int64),
            pl.col("away_final_score").cast(pl.Int64),
            pl.col("final_margin").cast(pl.Int64),
            pl.col("largest_lead").cast(pl.Int64),
            pl.col("lead_changes").cast(pl.Int64),
            pl.col("competitive_until_minute").cast(pl.Int64)
        ])

        # Select final columns
        game_competitive = game_competitive.custom_select([
            "game_uuid", "season", "game_date", "home_team_uuid", "away_team_uuid",
            "home_final_score", "away_final_score", "final_margin", "largest_lead",
            "lead_changes", "game_type", "competitive_until_minute", "comeback_win",
            "winning_team_uuid"
        ])

        CustomDF(
            "gamecompetitiveness_enriched",
            initial_df=game_competitive.data
        ).write_table()

    elif table_name == "playerclutchperformance_enriched":
        from collections import defaultdict

        gamedata = CustomDF("gamedata_datamodel")
        gamescores = CustomDF("gameteamscoresdata_datamodel")
        substitutions = CustomDF("playergamesubstitionsgamedata_datamodel")
        shots = CustomDF("playergameshotsdata_datamodel")
        playerdata = CustomDF("playerdata_datamodel")
        teamgamestats = CustomDF("teamgamestatsdata_datamodel")
        raw_game_data = CustomDF("playergamedata_raw")

        # STEP 1: Identify clutch minutes (score within 5 points in last 5 minutes)
        clutch_minutes = gamescores.custom_select([
            "game_uuid", "minuteAbsolute", "home_score", "away_score", "home_team_uuid", "away_team_uuid"
        ])
        clutch_minutes.data = clutch_minutes.data.filter(
            pl.col("minuteAbsolute") >= 35
        ).with_columns(
            (pl.col("home_score") - pl.col("away_score")).abs().alias("margin")
        ).filter(
            pl.col("margin") <= 5
        )

        clutch_minutes = clutch_minutes.custom_select([
            "game_uuid", "minuteAbsolute", "home_team_uuid", "away_team_uuid"
        ]).custom_distinct()

        clutch_games = clutch_minutes.custom_select(["game_uuid"]).custom_distinct()

        # STEP 2: Reconstruct on-court lineups at each clutch minute

        # Get starting lineups from raw data
        starting_lineups = raw_game_data.data.select([
            pl.col("playerUuid").alias("player_uuid"),
            pl.col("idMatchIntern").alias("game_uuid"),
            pl.col("starting"),
        ]).filter(
            (pl.col("player_uuid").is_not_null()) &
            (pl.col("starting") == True)
        )

        # Get player teams with date ranges
        player_teams = playerdata.custom_select(["player_uuid", "team_uuid", "from_date", "to_date"]).custom_distinct()

        # Filter substitutions to clutch games only
        clutch_subs = substitutions.data.filter(
            pl.col("game_uuid").is_in(clutch_games.data["game_uuid"]) &
            pl.col("minute_absolute").is_not_null() &
            pl.col("type").is_not_null()
        ).sort(["game_uuid", "minute_absolute"])

        # Build on-court player mapping for each clutch minute
        clutch_player_minutes = []

        for game_uuid in clutch_games.data["game_uuid"].to_list():
            game_clutch_minutes = clutch_minutes.data.filter(
                pl.col("game_uuid") == game_uuid
            ).sort("minuteAbsolute")

            game_subs = clutch_subs.filter(pl.col("game_uuid") == game_uuid)
            game_starters = starting_lineups.filter(pl.col("game_uuid") == game_uuid)

            # Initialize with starters
            on_court = set(game_starters["player_uuid"].to_list())

            # For each clutch minute, determine who was on court
            for minute_row in game_clutch_minutes.iter_rows(named=True):
                minute = minute_row["minuteAbsolute"]

                # Apply all substitutions up to this minute
                relevant_subs = game_subs.filter(pl.col("minute_absolute") <= minute)
                on_court_at_minute = set(game_starters["player_uuid"].to_list())

                for sub_row in relevant_subs.iter_rows(named=True):
                    if sub_row["type"] == "IN_TYPE":
                        on_court_at_minute.add(sub_row["player_uuid"])
                    elif sub_row["type"] == "OUT_TYPE":
                        on_court_at_minute.discard(sub_row["player_uuid"])

                # Record each player on court at this clutch minute
                for player_uuid in on_court_at_minute:
                    clutch_player_minutes.append({
                        "game_uuid": game_uuid,
                        "player_uuid": player_uuid,
                        "clutch_minute": minute,
                        "home_team_uuid": minute_row["home_team_uuid"],
                        "away_team_uuid": minute_row["away_team_uuid"],
                    })

        # STEP 3: Join shots with clutch minutes and on-court players
        # Work with raw polars DataFrames for intermediate calculations

        clutch_player_minutes_df = pl.DataFrame(clutch_player_minutes)

        # Convert period/minute in shots to absolute minute (Period 4 minute 1-10 = absolute minute 31-40)
        shots_q4 = shots.custom_select([
            "game_uuid", "player_uuid", "shot_type", "period", "minute", "outcome"
        ])
        shots_q4.data = shots_q4.data.filter(
            (pl.col("period") == 4) &
            pl.col("minute").is_not_null() &
            pl.col("minute") >= 5
        ).with_columns([
            (30 + pl.col("minute")).alias("clutch_minute")
        ])

        # Join shots with clutch player minutes (both are raw polars DataFrames)
        clutch_shots_df = clutch_player_minutes_df.join(
            shots_q4.data,
            on=["game_uuid", "player_uuid", "clutch_minute"],
            how="inner"
        )

        # STEP 4: Aggregate clutch stats per player-game

        # Calculate minutes played in clutch time per player-game
        clutch_minutes_played_df = clutch_player_minutes_df.group_by(
            ["game_uuid", "player_uuid"]
        ).agg([
            pl.col("clutch_minute").n_unique().cast(pl.Float64).alias("clutch_minutes_played"),
        ])

        # Aggregate shot statistics
        clutch_shot_stats_df = clutch_shots_df.group_by(
            ["game_uuid", "player_uuid"]
        ).agg([
            pl.when(pl.col("shot_type").str.contains("2"))
              .then(1).sum().alias("clutch_2pt_attempted"),
            pl.when(
                pl.col("shot_type").str.contains("2") &
                (pl.col("outcome").str.to_uppercase() == "MADE")
            ).then(1).sum().alias("clutch_2pt_made"),
            pl.when(pl.col("shot_type").str.contains("3"))
              .then(1).sum().alias("clutch_3pt_attempted"),
            pl.when(
                pl.col("shot_type").str.contains("3") &
                (pl.col("outcome").str.to_uppercase() == "MADE")
            ).then(1).sum().alias("clutch_3pt_made"),
        ]).with_columns([
            pl.col("clutch_2pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("clutch_2pt_made").fill_null(0).cast(pl.Int64),
            pl.col("clutch_3pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("clutch_3pt_made").fill_null(0).cast(pl.Int64),
        ])

        # Combine minutes and shots
        clutch_combined_df = clutch_minutes_played_df.join(
            clutch_shot_stats_df,
            on=["game_uuid", "player_uuid"],
            how="left"
        ).with_columns([
            pl.col("clutch_2pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("clutch_2pt_made").fill_null(0).cast(pl.Int64),
            pl.col("clutch_3pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("clutch_3pt_made").fill_null(0).cast(pl.Int64),
        ]).with_columns([
            # Calculate clutch points (2PT made * 2 + 3PT made * 3)
            (pl.col("clutch_2pt_made") * 2 + pl.col("clutch_3pt_made") * 3).cast(pl.Int64).alias("clutch_points"),
            # Calculate field goal percentage
            pl.when(
                (pl.col("clutch_2pt_attempted") + pl.col("clutch_3pt_attempted")) > 0
            ).then(
                (pl.col("clutch_2pt_made") + pl.col("clutch_3pt_made")).cast(pl.Float64) /
                (pl.col("clutch_2pt_attempted") + pl.col("clutch_3pt_attempted"))
            ).otherwise(None).alias("clutch_fg_pct")
        ])

        # Get game metadata (ensure unique by game_uuid only)
        game_metadata = gamedata.custom_select([
            "game_uuid", "season", "game_time"
        ])
        game_metadata.data = game_metadata.data.unique(subset=["game_uuid"], keep="first").with_columns(
            pl.col("game_time").cast(pl.Date).alias("game_date")
        )

        # Determine game result (Win/Loss)
        team_results = teamgamestats.custom_join(
            gamedata.custom_select(["game_uuid", "team_uuid"]),
            custom_on=["game_uuid"],
            custom_how="inner",
            custom_suffix="_opponent"
        )
        team_results.data = team_results.data.filter(
            pl.col("team_uuid") != pl.col("team_uuid_opponent")
        )

        team_results = team_results.custom_join(
            teamgamestats.custom_select(["game_uuid", "team_uuid", "points"]),
            custom_left_on=["game_uuid", "team_uuid_opponent"],
            custom_right_on=["game_uuid", "team_uuid"],
            custom_how="left",
            custom_suffix="_opponent"
        )

        team_results.data = team_results.data.with_columns(
            pl.when(pl.col("points") > pl.col("points_opponent"))
            .then(pl.lit("Win"))
            .otherwise(pl.lit("Loss"))
            .alias("game_result")
        )

        team_results = team_results.custom_select(["game_uuid", "team_uuid", "game_result"])

        # Join everything together using raw polars joins for the intermediate result
        # First join game metadata to get game_date
        clutch_final_df = clutch_combined_df.join(
            game_metadata.data,
            on="game_uuid",
            how="left"
        )

        # Then join player_teams filtered by date range
        clutch_final_df = clutch_final_df.join(
            player_teams.data,
            on="player_uuid",
            how="left"
        )

        # Finally join team results
        clutch_final_df = clutch_final_df.join(
            team_results.data,
            on=["game_uuid", "team_uuid"],
            how="left"
        )

        # Add clutch game flag and was_on_court flag
        clutch_final_df = clutch_final_df.with_columns([
            pl.lit(True).alias("is_clutch_game"),
            pl.lit(True).alias("was_on_court_during_clutch"),
        ])

        # Select final columns
        clutch_final_df = clutch_final_df.select([
            "player_uuid",
            "game_uuid",
            "season",
            "game_date",
            "is_clutch_game",
            "clutch_minutes_played",
            "clutch_points",
            "clutch_2pt_made",
            "clutch_2pt_attempted",
            "clutch_3pt_made",
            "clutch_3pt_attempted",
            "clutch_fg_pct",
            "game_result",
            "was_on_court_during_clutch",
        ]).unique(subset=["game_uuid", "player_uuid"], keep="first")

        CustomDF(
            "playerclutchperformance_enriched",
            initial_df=clutch_final_df
        ).write_table()

    elif table_name == "teamgamequarterperformance_enriched":
        gamedata = CustomDF("gamedata_datamodel")
        gamescores = CustomDF("gameteamscoresdata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")
        substitutions = CustomDF("playergamesubstitionsgamedata_datamodel")
        playerdata = CustomDF("playerdata_datamodel")

        print("\n>> Building team game quarter performance data...")

        # Filter to regulation quarters
        gamescores.data = gamescores.data.filter(
            pl.col("quarter").is_between(1, 4)
        ).sort(["game_uuid", "quarter", "minuteAbsolute"])

        # Get end-of-quarter scores
        quarter_scores = gamescores.custom_groupby(
            ["game_uuid", "quarter", "home_team_uuid", "away_team_uuid"],
            pl.last("home_score").alias("home_score_end"),
            pl.last("away_score").alias("away_score_end"),
        )

        # Calculate start-of-quarter scores (end of previous quarter)
        quarter_scores.data = quarter_scores.data.sort(["game_uuid", "quarter"]).with_columns([
            pl.col("home_score_end").shift(1).over("game_uuid").fill_null(0).alias("home_score_start"),
            pl.col("away_score_end").shift(1).over("game_uuid").fill_null(0).alias("away_score_start"),
        ])

        # Calculate quarter points
        quarter_scores.data = quarter_scores.data.with_columns([
            (pl.col("home_score_end") - pl.col("home_score_start")).alias("home_quarter_points"),
            (pl.col("away_score_end") - pl.col("away_score_start")).alias("away_quarter_points"),
        ])

        # Create separate records for home and away teams
        home_quarters = quarter_scores.data.with_columns([
            pl.col("home_team_uuid").alias("team_uuid"),
            pl.col("away_team_uuid").alias("opponent_uuid"),
            pl.lit("home").alias("team_type"),
            pl.col("home_quarter_points").alias("team_quarter_points"),
            pl.col("away_quarter_points").alias("opponent_quarter_points"),
        ])

        away_quarters = quarter_scores.data.with_columns([
            pl.col("away_team_uuid").alias("team_uuid"),
            pl.col("home_team_uuid").alias("opponent_uuid"),
            pl.lit("away").alias("team_type"),
            pl.col("away_quarter_points").alias("team_quarter_points"),
            pl.col("home_quarter_points").alias("opponent_quarter_points"),
        ])

        # Combine home and away
        all_quarters = pl.concat([home_quarters, away_quarters])

        # Calculate margins and determine quarter winners
        all_quarters = all_quarters.with_columns([
            (pl.col("team_quarter_points") - pl.col("opponent_quarter_points")).alias("quarter_margin"),
            pl.when(pl.col("team_quarter_points") > pl.col("opponent_quarter_points"))
            .then(1)
            .when(pl.col("team_quarter_points") < pl.col("opponent_quarter_points"))
            .then(0)
            .otherwise(None)
            .alias("quarter_won")
        ])

        # Pivot quarters to columns
        quarter_pivoted = all_quarters.pivot(
            values=["team_quarter_points", "opponent_quarter_points", "quarter_margin"],
            index=["game_uuid", "team_uuid", "opponent_uuid", "team_type"],
            columns="quarter",
            aggregate_function="first"
        )

        # Rename columns to match schema
        quarter_pivoted = quarter_pivoted.rename({
            "team_quarter_points_1": "quarter_1_points",
            "team_quarter_points_2": "quarter_2_points",
            "team_quarter_points_3": "quarter_3_points",
            "team_quarter_points_4": "quarter_4_points",
            "opponent_quarter_points_1": "quarter_1_points_allowed",
            "opponent_quarter_points_2": "quarter_2_points_allowed",
            "opponent_quarter_points_3": "quarter_3_points_allowed",
            "opponent_quarter_points_4": "quarter_4_points_allowed",
            "quarter_margin_1": "quarter_1_margin",
            "quarter_margin_2": "quarter_2_margin",
            "quarter_margin_3": "quarter_3_margin",
            "quarter_margin_4": "quarter_4_margin",
        })

        # Calculate quarters won/lost/tied
        quarter_pivoted = quarter_pivoted.with_columns([
            ((pl.col("quarter_1_margin") > 0).cast(pl.Int64) +
             (pl.col("quarter_2_margin") > 0).cast(pl.Int64) +
             (pl.col("quarter_3_margin") > 0).cast(pl.Int64) +
             (pl.col("quarter_4_margin") > 0).cast(pl.Int64)).alias("quarters_won"),
            ((pl.col("quarter_1_margin") < 0).cast(pl.Int64) +
             (pl.col("quarter_2_margin") < 0).cast(pl.Int64) +
             (pl.col("quarter_3_margin") < 0).cast(pl.Int64) +
             (pl.col("quarter_4_margin") < 0).cast(pl.Int64)).alias("quarters_lost"),
            ((pl.col("quarter_1_margin") == 0).cast(pl.Int64) +
             (pl.col("quarter_2_margin") == 0).cast(pl.Int64) +
             (pl.col("quarter_3_margin") == 0).cast(pl.Int64) +
             (pl.col("quarter_4_margin") == 0).cast(pl.Int64)).alias("quarters_tied"),
        ])

        # Calculate largest lead per quarter (simplified - using quarter margin as proxy)
        quarter_pivoted = quarter_pivoted.with_columns([
            pl.when(pl.col("quarter_1_margin") > 0)
            .then(pl.col("quarter_1_margin"))
            .otherwise(0)
            .alias("largest_lead_q1"),
            pl.when(pl.col("quarter_2_margin") > 0)
            .then(pl.col("quarter_2_margin"))
            .otherwise(0)
            .alias("largest_lead_q2"),
            pl.when(pl.col("quarter_3_margin") > 0)
            .then(pl.col("quarter_3_margin"))
            .otherwise(0)
            .alias("largest_lead_q3"),
            pl.when(pl.col("quarter_4_margin") > 0)
            .then(pl.col("quarter_4_margin"))
            .otherwise(0)
            .alias("largest_lead_q4"),
        ])

        # Join with game metadata
        game_metadata = gamedata.custom_select([
            "game_uuid", "team_uuid", "season", "game_time"
        ])
        game_metadata.data = game_metadata.data.with_columns(
            pl.col("game_time").cast(pl.Date).alias("game_date")
        )

        quarter_final = quarter_pivoted.join(
            game_metadata.data,
            on=["game_uuid", "team_uuid"],
            how="left"
        )

        # Join with team names
        teamdata_names = teamdata.custom_select(["team_uuid", "team_name", "team_short_name"])
        teamdata_names.data = teamdata_names.data.unique(subset=["team_uuid"], keep="first")

        quarter_final = quarter_final.join(
            teamdata_names.data,
            on="team_uuid",
            how="left"
        )

        # Join opponent names
        quarter_final = quarter_final.join(
            teamdata_names.data.rename({"team_uuid": "opponent_uuid", "team_name": "opponent_name"}),
            on="opponent_uuid",
            how="left"
        ).drop("team_short_name_right")

        # Calculate first substitution timing per quarter per team
        print(">> Calculating first substitution timing per quarter...")

        # Get player-team mappings
        player_teams = playerdata.custom_select(["player_uuid", "team_uuid"]).custom_distinct()

        # Join substitutions with player teams
        subs_with_teams = substitutions.data.join(
            player_teams.data,
            on="player_uuid",
            how="left"
        ).filter(
            pl.col("team_uuid").is_not_null() &
            pl.col("minute_absolute").is_not_null() &
            pl.col("type").is_not_null()
        )

        # Join with game scores to get quarter info
        subs_with_scores = subs_with_teams.join(
            gamescores.data.select(["game_uuid", "quarter", "minuteAbsolute", "home_team_uuid", "away_team_uuid"]),
            left_on=["game_uuid", "minute_absolute"],
            right_on=["game_uuid", "minuteAbsolute"],
            how="left"
        ).filter(pl.col("quarter").is_not_null())

        # Calculate first OUT substitution per team per quarter
        first_sub_per_quarter = subs_with_scores.filter(
            pl.col("type") == "OUT_TYPE"
        ).group_by(["game_uuid", "team_uuid", "quarter"]).agg([
            pl.col("minute_absolute").min().alias("first_sub_minute"),
            pl.col("point_diff").first().alias("point_diff_at_first_sub")
        ])

        # Determine team perspective (positive diff = winning, negative = losing)
        first_sub_per_quarter = first_sub_per_quarter.with_columns([
            pl.when(pl.col("point_diff_at_first_sub") > 0)
            .then(pl.lit(True))
            .when(pl.col("point_diff_at_first_sub") < 0)
            .then(pl.lit(False))
            .otherwise(None)
            .alias("was_winning_at_first_sub")
        ])

        # Pivot by quarter
        sub_timing_pivoted = first_sub_per_quarter.pivot(
            values=["first_sub_minute", "was_winning_at_first_sub"],
            index=["game_uuid", "team_uuid"],
            columns="quarter",
            aggregate_function="first"
        )

        # Rename columns
        sub_timing_pivoted = sub_timing_pivoted.rename({
            "first_sub_minute_1": "q1_first_sub_minute",
            "first_sub_minute_2": "q2_first_sub_minute",
            "first_sub_minute_3": "q3_first_sub_minute",
            "first_sub_minute_4": "q4_first_sub_minute",
            "was_winning_at_first_sub_1": "q1_winning_at_first_sub",
            "was_winning_at_first_sub_2": "q2_winning_at_first_sub",
            "was_winning_at_first_sub_3": "q3_winning_at_first_sub",
            "was_winning_at_first_sub_4": "q4_winning_at_first_sub"
        })

        # Join substitution timing data with quarter performance
        quarter_final = quarter_final.join(
            sub_timing_pivoted,
            on=["game_uuid", "team_uuid"],
            how="left"
        )

        # Select final columns
        quarter_final = quarter_final.select([
            "game_uuid", "team_uuid", "team_name", "team_short_name", "opponent_uuid",
            "opponent_name", "season", "game_date", "team_type",
            "quarter_1_points", "quarter_2_points", "quarter_3_points", "quarter_4_points",
            "quarter_1_points_allowed", "quarter_2_points_allowed",
            "quarter_3_points_allowed", "quarter_4_points_allowed",
            "quarter_1_margin", "quarter_2_margin", "quarter_3_margin", "quarter_4_margin",
            "quarters_won", "quarters_lost", "quarters_tied",
            "largest_lead_q1", "largest_lead_q2", "largest_lead_q3", "largest_lead_q4",
            "q1_first_sub_minute", "q1_winning_at_first_sub",
            "q2_first_sub_minute", "q2_winning_at_first_sub",
            "q3_first_sub_minute", "q3_winning_at_first_sub",
            "q4_first_sub_minute", "q4_winning_at_first_sub"
        ])

        print(f">> Found {len(quarter_final)} team game quarter performance records")

        CustomDF(
            "teamgamequarterperformance_enriched",
            initial_df=quarter_final
        ).write_table()

    elif table_name == "teamgame_shots_enriched":
        playergameshotsdata = CustomDF("playergameshotsdata_datamodel")
        gamedata = CustomDF("gamedata_datamodel")
        playerdata = CustomDF("playerdata_datamodel")
        teamdata = CustomDF("teamdata_datamodel")

        print("\n>> Building team game shot locations...")

        # Get player teams
        player_teams = playerdata.custom_select(["player_uuid", "team_uuid"])
        player_teams.data = player_teams.data.unique(subset=["player_uuid"], keep="first")

        # Join shots with player teams
        team_shots = playergameshotsdata.custom_join(
            player_teams,
            custom_on=["player_uuid"],
            custom_how="left"
        )

        # Get game metadata
        game_metadata = gamedata.custom_select([
            "game_uuid", "team_uuid", "season", "game_time", "team_type"
        ])
        game_metadata.data = game_metadata.data.with_columns(
            pl.col("game_time").cast(pl.Date).alias("game_date")
        )

        # Join with game data to get season and opponent
        team_shots = team_shots.custom_join(
            game_metadata,
            custom_on=["game_uuid", "team_uuid"],
            custom_how="left"
        )

        # Get opponent info
        game_opponents = gamedata.custom_select(["game_uuid", "team_uuid", "team_type"])
        game_opponents_self = game_opponents.custom_join(
            game_opponents,
            custom_on=["game_uuid"],
            custom_how="inner",
            custom_suffix="_opponent"
        )
        game_opponents_self.data = game_opponents_self.data.filter(
            pl.col("team_uuid") != pl.col("team_uuid_opponent")
        ).select(["game_uuid", "team_uuid", "team_uuid_opponent"])

        team_shots.data = team_shots.data.join(
            game_opponents_self.data,
            on=["game_uuid", "team_uuid"],
            how="left"
        )

        # Group by team and game, collecting shot locations
        team_game_shots = team_shots.custom_groupby(
            ["team_uuid", "game_uuid", "season", "game_date", "team_uuid_opponent", "team_type"],
            # Collect 2PT locations
            pl.when(
                (pl.col("shot_type").str.contains("2")) |
                (pl.col("shot_type").str.to_uppercase() == "TWO")
            ).then(
                pl.struct([
                    pl.col("xnormalize").cast(pl.Float64).alias("xnormalize"),
                    pl.col("ynormalize").cast(pl.Float64).alias("ynormalize")
                ])
            ).filter(pl.col("shot_type").is_not_null()).alias("twopoint_locations"),

            # Collect 3PT locations
            pl.when(
                (pl.col("shot_type").str.contains("3")) |
                (pl.col("shot_type").str.to_uppercase() == "THREE")
            ).then(
                pl.struct([
                    pl.col("xnormalize").cast(pl.Float64).alias("xnormalize"),
                    pl.col("ynormalize").cast(pl.Float64).alias("ynormalize")
                ])
            ).filter(pl.col("shot_type").is_not_null()).alias("threepoint_locations"),

            # Count made/attempted
            pl.when(
                (pl.col("shot_type").str.contains("2")) |
                (pl.col("shot_type").str.to_uppercase() == "TWO")
            ).then(1).sum().alias("two_pt_attempted"),

            pl.when(
                ((pl.col("shot_type").str.contains("2")) |
                 (pl.col("shot_type").str.to_uppercase() == "TWO")) &
                (pl.col("outcome").str.to_uppercase() == "MADE")
            ).then(1).sum().alias("two_pt_made"),

            pl.when(
                (pl.col("shot_type").str.contains("3")) |
                (pl.col("shot_type").str.to_uppercase() == "THREE")
            ).then(1).sum().alias("three_pt_attempted"),

            pl.when(
                ((pl.col("shot_type").str.contains("3")) |
                 (pl.col("shot_type").str.to_uppercase() == "THREE")) &
                (pl.col("outcome").str.to_uppercase() == "MADE")
            ).then(1).sum().alias("three_pt_made"),
        )

        # Calculate shooting percentages
        team_game_shots.data = team_game_shots.data.with_columns([
            pl.col("two_pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("two_pt_made").fill_null(0).cast(pl.Int64),
            pl.col("three_pt_attempted").fill_null(0).cast(pl.Int64),
            pl.col("three_pt_made").fill_null(0).cast(pl.Int64),
        ]).with_columns([
            pl.when(pl.col("two_pt_attempted") > 0)
            .then(pl.col("two_pt_made").cast(pl.Float64) / pl.col("two_pt_attempted"))
            .otherwise(None)
            .alias("two_pt_pct"),
            pl.when(pl.col("three_pt_attempted") > 0)
            .then(pl.col("three_pt_made").cast(pl.Float64) / pl.col("three_pt_attempted"))
            .otherwise(None)
            .alias("three_pt_pct"),
        ])

        # Join with team names
        teamdata_names = teamdata.custom_select(["team_uuid", "team_name", "team_short_name"])
        teamdata_names.data = teamdata_names.data.unique(subset=["team_uuid"], keep="first")

        team_game_shots.data = team_game_shots.data.join(
            teamdata_names.data,
            on="team_uuid",
            how="left"
        )

        # Join opponent names
        team_game_shots.data = team_game_shots.data.join(
            teamdata_names.data.rename({"team_uuid": "team_uuid_opponent", "team_name": "opponent_name"}),
            on="team_uuid_opponent",
            how="left"
        ).drop("team_short_name_right")

        # Select final columns
        team_game_shots = team_game_shots.custom_select([
            "team_uuid", "team_name", "team_short_name", "season", "game_uuid",
            "game_date", "team_uuid_opponent", "opponent_name", "team_type",
            "twopoint_locations", "threepoint_locations",
            "two_pt_made", "two_pt_attempted", "three_pt_made", "three_pt_attempted",
            "two_pt_pct", "three_pt_pct"
        ])

        # Rename opponent_uuid column
        team_game_shots.data = team_game_shots.data.rename({"team_uuid_opponent": "opponent_uuid"})

        print(f">> Found {len(team_game_shots.data)} team game shot records")

        CustomDF(
            "teamgame_shots_enriched",
            initial_df=team_game_shots.data
        ).write_table()

    else:
        raise ValueError(
            f"The table: {table_name} is not specified in the processing functions"
        )

    return True
