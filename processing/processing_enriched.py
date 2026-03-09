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

        playergameshotsdata_df = playergameshotsdata_df.custom_join(
            game_data.custom_select(["game_uuid", "season"]),
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

        playergameshotsdata_df = playergameshotsdata_df.custom_join(
            game_data.custom_select(["game_uuid", "season"]),
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

        player_data = player_data.custom_select(
            ["player_uuid", "team_uuid", "player_name", "player_number"]
        )

        team_data = team_data.custom_select(
            ["team_uuid", "team_name"]
        ).custom_distinct()

        player_data.data = player_data.data.unique(subset=["player_uuid"], keep="first")
        team_data.data = team_data.data.unique(subset=["team_uuid"], keep="first")

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
                "twopoint_locations",
                "threepoint_locations",
            ]
        )

        player_analytics_enriched = CustomDF(
            "playeranalytics_enriched", initial_df=player_analytics.data
        )
        player_analytics_enriched.write_table()

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

    elif table_name == "playergameanalytics_enriched":
        player_stats = CustomDF("playergamestatsdata_datamodel")
        game_data = CustomDF("gamedata_datamodel")
        player_data = CustomDF("playerdata_datamodel")
        team_data = CustomDF("teamdata_datamodel")
        playergameplusminus_enriched = CustomDF("playergameplusminus_enriched")

        
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
            ]
        ).custom_distinct()

        playergameanalytics_enriched = CustomDF(
            "playergameanalytics_enriched", initial_df=player_stats.data
        )
        playergameanalytics_enriched.write_table()

    else:
        raise ValueError(
            f"The table: {table_name} is not specified in the processing functions"
        )

    return True
