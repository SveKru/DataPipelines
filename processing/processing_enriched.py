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

    elif table_name == "playergamequarterstats_enriched":
        raw_game_data = CustomDF("playergamedata_raw")
        gameteamscores = CustomDF("gameteamscoresdata_datamodel")
        substitutions_q = CustomDF("playergamesubstitionsgamedata_datamodel")
        player_data = CustomDF("playerdata_datamodel")

        raw_game_data.data = (
            raw_game_data.data
            .select([
                pl.col("playerUuid").alias("player_uuid"),
                pl.col("idMatchIntern").alias("game_uuid"),
                pl.col("starting"),
            ])
            .filter(pl.col("player_uuid").is_not_null())
        )

        # Last cumulative score per (game_uuid, quarter) for regulation quarters
        quarter_scores = (
            gameteamscores.data
            .filter(pl.col("quarter").is_between(1, 4))
            .sort(["game_uuid", "quarter", "minuteAbsolute"])
            .group_by(["game_uuid", "quarter", "home_team_uuid", "away_team_uuid"], maintain_order=True)
            .agg(
                pl.last("home_score").alias("home_score_end"),
                pl.last("away_score").alias("away_score_end"),
            )
            .sort(["game_uuid", "quarter"])
            .with_columns([
                pl.col("home_score_end").shift(1).over("game_uuid").fill_null(0).alias("home_score_start"),
                pl.col("away_score_end").shift(1).over("game_uuid").fill_null(0).alias("away_score_start"),
            ])
            .with_columns(
                (pl.col("home_score_end") - pl.col("home_score_start") >
                 pl.col("away_score_end") - pl.col("away_score_start")).alias("home_won_quarter")
            )
            .select(["game_uuid", "quarter", "home_team_uuid", "home_won_quarter"])
        )

        subs_data = substitutions_q.data.select(["game_uuid", "player_uuid", "type", "minute_absolute"])

        # Q1: use the game-level starting flag from raw data
        q1_starts = raw_game_data.data.select([
            pl.lit(1).cast(pl.Int64).alias("quarter"),
            pl.col("game_uuid"),
            pl.col("player_uuid"),
            pl.col("starting").alias("starts_quarter"),
        ])

        quarter_starts_frames = [q1_starts]

        # Q2-Q4: last sub-event at or before each quarter's start boundary determines on-court status;
        # players with no events before the boundary inherit the game-level starting flag.
        for q, boundary in [(2, 10), (3, 20), (4, 30)]:
            last_sub_type = (
                subs_data
                .filter(pl.col("minute_absolute") <= boundary)
                .sort("minute_absolute")
                .group_by(["game_uuid", "player_uuid"], maintain_order=True)
                .agg(pl.last("type").alias("last_sub_type"))
            )
            qn_starts = (
                raw_game_data.data
                .join(last_sub_type, on=["game_uuid", "player_uuid"], how="left")
                .with_columns(
                    pl.when(pl.col("last_sub_type").is_not_null())
                    .then(pl.col("last_sub_type") == "IN_TYPE")
                    .otherwise(pl.col("starting"))
                    .alias("starts_quarter")
                )
                .select([
                    pl.lit(q).cast(pl.Int64).alias("quarter"),
                    pl.col("game_uuid"),
                    pl.col("player_uuid"),
                    pl.col("starts_quarter"),
                ])
            )
            quarter_starts_frames.append(qn_starts)

        # Keep only rows where the player starts the quarter
        player_quarter_starts = (
            pl.concat(quarter_starts_frames)
            .filter(pl.col("starts_quarter") == True)
        )

        # Resolve team_uuid for each player
        player_teams_q = (
            player_data.data
            .unique(subset=["player_uuid"], keep="first")
            .select(["player_uuid", "team_uuid"])
        )
        player_quarter_starts = player_quarter_starts.join(
            player_teams_q, on="player_uuid", how="left"
        )

        # Determine whether each quarter was won by the starting player's team
        player_quarter_starts = (
            player_quarter_starts
            .join(quarter_scores, on=["game_uuid", "quarter"], how="left")
            .with_columns(
                pl.when(pl.col("team_uuid") == pl.col("home_team_uuid"))
                .then(pl.col("home_won_quarter"))
                .otherwise(~pl.col("home_won_quarter"))
                .alias("won_quarter")
            )
        )

        # Aggregate per (game_uuid, player_uuid)
        player_quarter_agg = (
            player_quarter_starts
            .group_by(["game_uuid", "player_uuid"])
            .agg([
                pl.len().alias("quarters_started"),
                pl.col("won_quarter").sum().cast(pl.Int64).alias("quarters_won_when_starting"),
            ])
            .with_columns(
                pl.when(pl.col("quarters_started") > 0)
                .then(pl.col("quarters_won_when_starting").cast(pl.Float64) / pl.col("quarters_started"))
                .otherwise(None)
                .alias("quarter_win_rate_when_starting")
            )
            .with_columns(pl.col("quarters_started").cast(pl.Int64))
        )

        playergamequarterstats_enriched = CustomDF(
            "playergamequarterstats_enriched", initial_df=player_quarter_agg
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
                "quarters_started",
                "quarters_won_when_starting",
                "quarter_win_rate_when_starting",
                "twopoint_locations",
                "threepoint_locations",
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

    else:
        raise ValueError(
            f"The table: {table_name} is not specified in the processing functions"
        )

    return True
