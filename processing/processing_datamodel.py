from polars_src.custom_dataframes import CustomDF
import polars as pl
from datetime import datetime
from pytz import timezone


def generate_table_datamodel(table_name: str) -> bool:
    if table_name == "flights_datamodel":
        flights_raw = CustomDF("flights_raw")

        timezone_raw = CustomDF("timezone_raw").custom_select(
            ["Airport", "TimeZoneOffset"]
        )

        flights_raw = flights_raw.custom_join(
            timezone_raw,
            custom_left_on=pl.col("ORIGIN_AIRPORT"),
            custom_right_on=pl.col("Airport"),
            custom_how="left",
        )

        flights_raw.data = flights_raw.data.rename(
            {"TimeZoneOffset": "DepartureTimeZoneOffset"}
        )

        flights_raw = flights_raw.custom_join(
            timezone_raw,
            custom_left_on=pl.col("DESTINATION_AIRPORT"),
            custom_right_on=pl.col("Airport"),
            custom_how="left",
        )

        flights_raw.data = flights_raw.data.rename(
            {"TimeZoneOffset": "DestinationTimeZoneOffset"}
        )

        flights_raw.data = flights_raw.data.with_columns(
            (pl.col("DestinationTimeZoneOffset") - pl.col("DepartureTimeZoneOffset"))
            .alias("TimeZoneOffset")
            .cast(pl.Int64)
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.date(pl.col("YEAR"), pl.col("MONTH"), pl.col("DAY")).alias("Flight_Date")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col("YEAR"),
                pl.col("MONTH"),
                pl.col("DAY"),
                pl.col("SCHEDULED_DEPARTURE").str.slice(0, 2).cast(pl.Int64),
                pl.col("SCHEDULED_DEPARTURE").str.slice(2, 2).cast(pl.Int64),
            ).alias("Scheduled_Departure")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col("YEAR"),
                pl.col("MONTH"),
                pl.col("DAY"),
                pl.col("SCHEDULED_DEPARTURE").str.slice(0, 2).cast(pl.Int64),
                pl.col("SCHEDULED_DEPARTURE").str.slice(2, 2).cast(pl.Int64),
            )
            .dt.offset_by(pl.concat_str([pl.col("DEPARTURE_DELAY"), pl.lit("m")]))
            .alias("Departure_Time")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col("YEAR"),
                pl.col("MONTH"),
                pl.col("DAY"),
                pl.col("SCHEDULED_DEPARTURE").str.slice(0, 2).cast(pl.Int64),
                pl.col("SCHEDULED_DEPARTURE").str.slice(2, 2).cast(pl.Int64),
            )
            .dt.offset_by(pl.concat_str([pl.col("DEPARTURE_DELAY"), pl.lit("m")]))
            .dt.offset_by(pl.concat_str([pl.col("TAXI_OUT"), pl.lit("m")]))
            .alias("Wheels_Off")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col("YEAR"),
                pl.col("MONTH"),
                pl.col("DAY"),
                pl.col("SCHEDULED_DEPARTURE").str.slice(0, 2).cast(pl.Int64),
                pl.col("SCHEDULED_DEPARTURE").str.slice(2, 2).cast(pl.Int64),
            )
            .dt.offset_by(pl.concat_str([pl.col("DEPARTURE_DELAY"), pl.lit("m")]))
            .dt.offset_by(pl.concat_str([pl.col("TAXI_OUT"), pl.lit("m")]))
            .dt.offset_by(pl.concat_str([pl.col("AIR_TIME"), pl.lit("m")]))
            .dt.offset_by(
                pl.concat_str(
                    [pl.coalesce(pl.col("TimeZoneOffset"), pl.lit("0")), pl.lit("m")]
                )
            )
            .alias("Wheels_On")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col("YEAR"),
                pl.col("MONTH"),
                pl.col("DAY"),
                pl.col("SCHEDULED_DEPARTURE").str.slice(0, 2).cast(pl.Int64),
                pl.col("SCHEDULED_DEPARTURE").str.slice(2, 2).cast(pl.Int64),
            )
            .dt.offset_by(pl.concat_str([pl.col("DEPARTURE_DELAY"), pl.lit("m")]))
            .dt.offset_by(pl.concat_str([pl.col("TAXI_OUT"), pl.lit("m")]))
            .dt.offset_by(pl.concat_str([pl.col("AIR_TIME"), pl.lit("m")]))
            .dt.offset_by(pl.concat_str([pl.col("TAXI_IN"), pl.lit("m")]))
            .dt.offset_by(
                pl.concat_str(
                    [pl.coalesce(pl.col("TimeZoneOffset"), pl.lit("0")), pl.lit("m")]
                )
            )
            .alias("Arrival_Time")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.datetime(
                pl.col("YEAR"),
                pl.col("MONTH"),
                pl.col("DAY"),
                pl.col("SCHEDULED_DEPARTURE").str.slice(0, 2).cast(pl.Int64),
                pl.col("SCHEDULED_DEPARTURE").str.slice(2, 2).cast(pl.Int64),
            )
            .dt.offset_by(pl.concat_str([pl.col("SCHEDULED_TIME"), pl.lit("m")]))
            .dt.offset_by(
                pl.concat_str(
                    [pl.coalesce(pl.col("TimeZoneOffset"), pl.lit("0")), pl.lit("m")]
                )
            )
            .alias("Scheduled_Arrival")
        )

        flights_raw.data = flights_raw.data.with_columns(
            pl.col("DIVERTED").cast(pl.Boolean).alias("Diverted"),
            pl.col("CANCELLED").cast(pl.Boolean).alias("Cancelled"),
        )

        rename_columns_dict = {
            "DAY_OF_WEEK": "Weekday",
            "AIRLINE": "Airline_Code",
            "FLIGHT_NUMBER": "Flight_Number",
            "TAIL_NUMBER": "Tail_Number",
            "ORIGIN_AIRPORT": "Origin_Airport",
            "DESTINATION_AIRPORT": "Destination_Airport",
            "DEPARTURE_DELAY": "Departure_Delay",
            "TAXI_OUT": "Taxi_Out",
            "SCHEDULED_TIME": "Scheduled_Time",
            "ELAPSED_TIME": "Elapsed_Time",
            "AIR_TIME": "Air_Time",
            "DISTANCE": "Distance",
            "TAXI_IN": "Taxi_In",
            "ARRIVAL_DELAY": "Arrival_Delay",
            "CANCELLATION_REASON": "Cancellation_Reason",
            "AIR_SYSTEM_DELAY": "Air_System_Delay",
            "SECURITY_DELAY": "Security_Delay",
            "AIRLINE_DELAY": "Airline_Delay",
            "LATE_AIRCRAFT_DELAY": "Late_Aircraft_Delay",
            "WEATHER_DELAY": "Weather_Delay",
        }

        flights_raw.rename_columns(rename_columns_dict)

        flights_raw = flights_raw.custom_select(
            [
                "Flight_Date",
                "Weekday",
                "Airline_Code",
                "Flight_Number",
                "Tail_Number",
                "Origin_Airport",
                "Destination_Airport",
                "Scheduled_Departure",
                "Departure_Time",
                "Departure_Delay",
                "Taxi_Out",
                "Wheels_Off",
                "Scheduled_Time",
                "Elapsed_Time",
                "Air_Time",
                "Distance",
                "Wheels_On",
                "Taxi_In",
                "Scheduled_Arrival",
                "Arrival_Time",
                "Arrival_Delay",
                "Diverted",
                "Cancelled",
                "Cancellation_Reason",
                "Air_System_Delay",
                "Security_Delay",
                "Airline_Delay",
                "Late_Aircraft_Delay",
                "Weather_Delay",
                "from_date",
                "to_date",
            ]
        )

        flights_datamodel = CustomDF("flights_datamodel", initial_df=flights_raw.data)
        flights_datamodel.write_table()

    elif table_name == "airlines_datamodel":
        airlines_raw = CustomDF("airlines_raw")

        airlines_rename_dict = {
            "IATA_CODE": "IATA_Airline_Code",
            "AIRLINE": "Airline",
        }

        airlines_raw.rename_columns(airlines_rename_dict)

        airlines_datamodel = CustomDF(
            "airlines_datamodel", initial_df=airlines_raw.data
        )
        airlines_datamodel.write_table()

    elif table_name == "airports_datamodel":
        airports_raw = CustomDF("airports_raw")

        airport_rename_dict = {
            "IATA_CODE": "IATA_Airport_Code",
            "AIRPORT": "Airport",
            "CITY": "City",
            "STATE": "State",
            "COUNTRY": "Country",
            "LATITUDE": "Latitude",
            "LONGITUDE": "Longitude",
        }

        airports_raw.rename_columns(airport_rename_dict)

        airports_datamodel = CustomDF(
            "airports_datamodel", initial_df=airports_raw.data
        )
        airports_datamodel.write_table()

    elif table_name == "gamedata_datamodel":
        gamedata_raw = CustomDF("gamedata_raw")
        gamedatametadata_raw = CustomDF("gamedatametadata_raw")
        teamdata_raw = CustomDF("teamdata_raw")

        gamedata = gamedata_raw.custom_select(
            ["time", "localId", "visitId", "idMatchIntern"]
        )
        gamedata.data = gamedata.data.with_columns(pl.col("time").alias("game_time"))
        gamedata = gamedata.custom_join(
            gamedatametadata_raw.custom_select(
                ["uuid", "competition_code", "group", "long_name", "year"]
            ),
            custom_left_on=[pl.col("idMatchIntern")],
            custom_right_on=[pl.col("uuid")],
            custom_how="left",
        )
        gamedata.data = gamedata.data.with_columns(
            pl.col("year").alias("season"),
            pl.col("long_name").alias("competition_name"),
            pl.col("idMatchIntern").alias("game_uuid"),
        )
        gamedata_home = gamedata.custom_join(
            teamdata_raw.custom_select(["teamIdExtern", "teamIdIntern", "name"]),
            custom_left_on=[pl.col("localId")],
            custom_right_on=[pl.col("teamIdIntern")],
            custom_how="left",
        )
        gamedata_home.data = gamedata_home.data.with_columns(
            pl.lit("home").alias("team_type"),
            pl.col("teamIdExtern").alias("team_uuid"),
        )
        gamedata_home = gamedata_home.custom_select(
            [
                "game_uuid",
                "game_time",
                "competition_code",
                "group",
                "season",
                "competition_name",
                "team_type",
                "team_uuid",
            ]
        )

        gamedata_away = gamedata.custom_join(
            teamdata_raw.custom_select(["teamIdExtern", "teamIdIntern", "name"]),
            custom_left_on=[pl.col("visitId")],
            custom_right_on=[pl.col("teamIdIntern")],
            custom_how="left",
        )
        gamedata_away.data = gamedata_away.data.with_columns(
            pl.lit("away").alias("team_type"),
            pl.col("teamIdExtern").alias("team_uuid"),
        )
        gamedata_away = gamedata_away.custom_select(
            [
                "game_uuid",
                "game_time",
                "competition_code",
                "group",
                "season",
                "competition_name",
                "team_type",
                "team_uuid",
            ]
        )

        gamedata_total = gamedata_home.custom_union(gamedata_away)

        gamedata_datamodel = CustomDF(
            "gamedata_datamodel", initial_df=gamedata_total.data
        )
        gamedata_datamodel.write_table()

    elif table_name == "gameteamscoresdata_datamodel":
        gameteamscoresdata_raw = CustomDF("gameteamscoresdata_raw")
        gamedata_raw = CustomDF("gamedata_raw")
        teamdata_raw = CustomDF("teamdata_raw")

        homeaway_teams = gamedata_raw.custom_select(
            ["idMatchIntern", "localId", "visitId"]
        ).custom_join(
            teamdata_raw.custom_select(["teamIdExtern", "teamIdIntern", "name"]),
            custom_left_on=[pl.col("localId")],
            custom_right_on=[pl.col("teamIdIntern")],
            custom_how="left",
        )
        homeaway_teams.data = homeaway_teams.data.with_columns(
            pl.col("teamIdExtern").alias("home_team_uuid"),
        ).drop("teamIdExtern")

        homeaway_teams = homeaway_teams.custom_join(
            teamdata_raw.custom_select(["teamIdExtern", "teamIdIntern", "name"]),
            custom_left_on=[pl.col("visitId")],
            custom_right_on=[pl.col("teamIdIntern")],
            custom_how="left",
        )
        homeaway_teams.data = homeaway_teams.data.with_columns(
            pl.col("teamIdExtern").alias("away_team_uuid"),
        ).drop("teamIdExtern")

        gamescores = gameteamscoresdata_raw.custom_join(
            homeaway_teams.custom_select(
                ["idMatchIntern", "home_team_uuid", "away_team_uuid"]
            ),
            custom_left_on=[pl.col("idMatchIntern")],
            custom_right_on=[pl.col("idMatchIntern")],
            custom_how="left",
        )

        gamescores.data = gamescores.data.with_columns(
            pl.col("idMatchIntern").alias("game_uuid"),
            pl.col("local").alias("home_score"),
            pl.col("visit").alias("away_score"),
            pl.col("period").alias("quarter"),
        )

        gamescores = gamescores.custom_select(
            [
                "game_uuid",
                "home_team_uuid",
                "away_team_uuid",
                "home_score",
                "away_score",
                "minuteQuarter",
                "minuteAbsolute",
                "quarter",
            ]
        )

        gamescores.convert_data_types(
            ["home_score", "away_score", "minuteQuarter", "minuteAbsolute", "quarter"],
            pl.Int64,
        )

        gameteamscoresdata_datamodel = CustomDF(
            "gameteamscoresdata_datamodel", initial_df=gamescores.data
        )
        gameteamscoresdata_datamodel.write_table()

    elif table_name == "playerdata_datamodel":
        playerdata_raw = CustomDF("playergamedata_raw")

        playerdata_raw.data = playerdata_raw.data.with_columns(
            pl.col("playerUuid").alias("player_uuid"),
            pl.col("teamId").alias("team_uuid"),
            pl.col("name").alias("player_name"),
            pl.col("dorsal").alias("player_number"),
        )
        playerdata_raw = playerdata_raw.custom_select(
            ["player_uuid", "team_uuid", "player_name", "player_number"]
        )
        playerdata_raw.data = playerdata_raw.data.filter(
            pl.col("player_uuid").is_not_null()
        )
        playerdata_raw = playerdata_raw.custom_distinct()

        playerdata_datamodel = CustomDF(
            "playerdata_datamodel", initial_df=playerdata_raw.data
        )
        playerdata_datamodel.write_table()

    elif table_name == "playergamestatsdata_datamodel":
        playergamestatsdata_raw = CustomDF("playergamestatsdata_raw")

        playergamestatsdata_raw.data = playergamestatsdata_raw.data.with_columns(
            pl.col("idMatchIntern").alias("game_uuid"),
            pl.col("score").alias("points"),
            pl.col("shotsOfOneAttempted").alias("ft_attempted"),
            pl.col("shotsOfOneSuccessful").alias("ft_made"),
            pl.col("shotsOfTwoAttempted").alias("two_attempted"),
            pl.col("shotsOfTwoSuccessful").alias("two_made"),
            pl.col("shotsOfThreeAttempted").alias("three_attempted"),
            pl.col("shotsOfThreeSuccessful").alias("three_made"),
            pl.col("personal").alias("fouls"),
            pl.col("timePlayed").alias("minutes_played"),
            pl.col("gamePlayed").alias("did_play"),
            pl.col("playerUuid").alias("player_uuid"),
        )

        playergamestatsdata_raw = playergamestatsdata_raw.custom_select(
            [
                "game_uuid",
                "player_uuid",
                "did_play",
                "minutes_played",
                "points",
                "ft_attempted",
                "ft_made",
                "two_attempted",
                "two_made",
                "three_attempted",
                "three_made",
                "rebounds",
                "assists",
                "steals",
                "fouls",
            ]
        )

        playergamestatsdata_datamodel = CustomDF(
            "playergamestatsdata_datamodel", initial_df=playergamestatsdata_raw.data
        )
        playergamestatsdata_datamodel.write_table()

    elif table_name == "playergameshotsdata_datamodel":
        twoptmade_raw = CustomDF("playergameshotsdata2ptmade_raw")
        twoptmissed_raw = CustomDF("playergameshotsdata2ptmissed_raw")
        threeptmade_raw = CustomDF("playergameshotsdata3ptmade_raw")
        threeptmissed_raw = CustomDF("playergameshotsdata3ptmissed_raw")

        shots_data = (
            twoptmade_raw.custom_union(twoptmissed_raw)
            .custom_union(threeptmade_raw)
            .custom_union(threeptmissed_raw)
        )

        playergameshotsdata_datamodel = CustomDF(
            "playergameshotsdata_datamodel", initial_df=shots_data.data
        )
        playergameshotsdata_datamodel.write_table()

    elif table_name == "teamgamestatsdata_datamodel":
        teamgamestatsdata_raw = CustomDF("teamgamestatsdata_raw")

        teamgamestatsdata_raw.data = teamgamestatsdata_raw.data.with_columns(
            pl.col("idMatchIntern").alias("game_uuid"),
            pl.col("score").alias("points"),
            pl.col("shotsOfOneAttempted").alias("ft_attempted"),
            pl.col("shotsOfOneSuccessful").alias("ft_made"),
            pl.col("shotsOfTwoAttempted").alias("two_attempted"),
            pl.col("shotsOfTwoSuccessful").alias("two_made"),
            pl.col("shotsOfThreeAttempted").alias("three_attempted"),
            pl.col("shotsOfThreeSuccessful").alias("three_made"),
            pl.col("personal").alias("fouls"),
        )

        teamgamestatsdata_raw = teamgamestatsdata_raw.custom_select(
            [
                "game_uuid",
                "team_uuid",
                "points",
                "ft_attempted",
                "ft_made",
                "two_attempted",
                "two_made",
                "three_attempted",
                "three_made",
                "rebounds",
                "assists",
                "steals",
                "fouls",
            ]
        )

        teamgamestatsdata_datamodel = CustomDF(
            "teamgamestatsdata_datamodel", initial_df=teamgamestatsdata_raw.data
        )
        teamgamestatsdata_datamodel.write_table()

    elif table_name == "playergamesubstitionsgamedata_datamodel":
        playersubstitionsgamedata_raw = CustomDF("playersubstitionsgamedata_raw")

        playersubstitionsgamedata_raw.data = (
            playersubstitionsgamedata_raw.data.with_columns(
                pl.col("idMatchIntern").alias("game_uuid"),
                pl.col("playerUuid").alias("player_uuid"),
                pl.col("minuteAbsolut").alias("minute_absolute"),
                pl.col("pointDiff").alias("point_diff"),
            )
        )

        playersubstitionsgamedata_raw = playersubstitionsgamedata_raw.custom_select(
            [
                "game_uuid",
                "player_uuid",
                "type",
                "minute_absolute",
                "point_diff",
            ]
        )

        playersubstitionsgamedata_raw.convert_data_types(
            ["minute_absolute", "point_diff"], pl.Int64
        )

        playersubstitionsgamedata_datamodel = CustomDF(
            "playergamesubstitionsgamedata_datamodel",
            initial_df=playersubstitionsgamedata_raw.data,
        )
        playersubstitionsgamedata_datamodel.write_table()

    elif table_name == "teamdata_datamodel":
        teamdata_raw = CustomDF("teamdata_raw")

        teamdata_raw.data = teamdata_raw.data.with_columns(
            pl.col("teamIdExtern").alias("team_uuid"),
            pl.col("name").alias("team_name"),
            pl.col("shortName").alias("team_short_name"),
        )

        teamdata_raw = teamdata_raw.custom_select(
            [
                "team_uuid",
                "team_name",
                "team_short_name",
            ]
        )

        teamdata_raw = teamdata_raw.custom_distinct()

        teamdata_datamodel = CustomDF(
            "teamdata_datamodel", initial_df=teamdata_raw.data
        )
        teamdata_datamodel.write_table()

    else:
        raise ValueError(
            f"The table: {table_name} is not specified in the processing functions"
        )

    return True
