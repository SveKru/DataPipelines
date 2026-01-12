from polars_src.custom_dataframes import CustomDF
import polars as pl
import datetime, pytz


def generate_table_raw(table_name: str) -> bool:
    if table_name == "flights_raw":
        flights_landingzone = CustomDF("flights_landingzone")

        flights_raw = CustomDF("flights_raw", initial_df=flights_landingzone.data)
        flights_raw.write_table()

    elif table_name == "cancellation_codes_raw":
        cancellation_codes_landingzone = CustomDF("cancellation_codes_landingzone")

        cancellation_codes_raw = CustomDF(
            "cancellation_codes_raw", initial_df=cancellation_codes_landingzone.data
        )
        cancellation_codes_raw.write_table()

    elif table_name == "airlines_raw":
        airlines_landingzone = CustomDF("airlines_landingzone")

        airlines_raw = CustomDF("airlines_raw", initial_df=airlines_landingzone.data)
        airlines_raw.write_table()

    elif table_name == "airports_raw":
        airports_landingzone = CustomDF("airports_landingzone")

        airports_raw = CustomDF("airports_raw", initial_df=airports_landingzone.data)
        airports_raw.write_table()

    elif table_name == "timezone_raw":
        timezone_landingzone = CustomDF("timezone_landingzone")
        timezone_landingzone.data = timezone_landingzone.data.hstack(
            timezone_landingzone.data.map_rows(
                lambda t: datetime.datetime.now(pytz.timezone(t[1])).strftime("%z")
            )
        ).rename({"map": "TimeZoneOffset"})
        timezone_landingzone.data = timezone_landingzone.data.with_columns(
            (
                pl.when(pl.col("TimeZoneOffset").str.slice(0, 1) == "+")
                .then(1)
                .otherwise(-1)
                * (
                    (pl.col("TimeZoneOffset").str.slice(1, 2).cast(pl.Int64) * 60)
                    + (pl.col("TimeZoneOffset").str.slice(3, 2).cast(pl.Int64))
                )
            )
            .alias("TimeZoneOffset")
            .cast(pl.Int64)
        )
        timezone_landingzone = timezone_landingzone.custom_select(
            ["Airport", "TimeZone", "TimeZoneOffset"]
        )
        timezone_raw = CustomDF("timezone_raw", initial_df=timezone_landingzone.data)
        timezone_raw.write_table()

    elif table_name == "gamedata_raw":
        gamedata_landingzone = CustomDF("gamedata_landingzone")

        gamedata_landingzone.data = gamedata_landingzone.data.with_columns(
            pl.col("file_name").str.replace(".json", "").alias("idMatchIntern")
        )
        gamedata_landingzone.data = gamedata_landingzone.data.with_columns(
            pl.col("time")
            .str.strptime(pl.Datetime, "%b %d, %Y %I:%M:%S %p", exact=False)
            .alias("time")
        )
        gamedata_landingzone.data = gamedata_landingzone.data.drop("file_name")

        gamedata_raw = CustomDF("gamedata_raw", initial_df=gamedata_landingzone.data)
        gamedata_raw.write_table()

    elif table_name == "teamdata_raw":
        teamdata_landingzone = CustomDF("teamdata_landingzone")
        teamdata_landingzone = teamdata_landingzone.custom_select(
            ["teamIdIntern", "teamIdExtern", "name", "shortName"]
        )
        teamdata_landingzone.data = teamdata_landingzone.data.unique()
        teamdata_raw = CustomDF("teamdata_raw", initial_df=teamdata_landingzone.data)
        teamdata_raw.write_table()

    elif table_name == "playergamedata_raw":
        playergamedata_landingzone = CustomDF("playergamedata_landingzone")

        for column in ["starting", "captain"]:
            playergamedata_landingzone.data = (
                playergamedata_landingzone.data.with_columns(
                    pl.col(column)
                    .str.replace("true", "1")
                    .str.replace("false", "0")
                    .cast(pl.Int16)
                    .cast(pl.Boolean)
                    .alias(column)
                )
            )
        playergamedata_landingzone.convert_data_types(
            ["dorsal", "timePlayed"], pl.Int16
        )

        playergamedata_landingzone.data = playergamedata_landingzone.data.with_columns(
            pl.col("file_name").str.replace(".json", "").alias("idMatchIntern")
        )
        playergamedata_landingzone.data = playergamedata_landingzone.data.with_columns(
            pl.col("uuid").alias("playerUuid")
        )
        playergamedata_landingzone = playergamedata_landingzone.custom_select(
            [
                "actorId",
                "playerUuid",
                "teamId",
                "name",
                "playerInternTeam",
                "dorsal",
                "starting",
                "captain",
                "timePlayed",
                "idMatchIntern",
            ]
        )
        playergamedata_raw = CustomDF(
            "playergamedata_raw", initial_df=playergamedata_landingzone.data
        )
        playergamedata_raw.write_table()

    elif table_name == "playersubstitionsgamedata_raw":
        playersubstitionsgamedata_landingzone = CustomDF(
            "playersubstitionsgamedata_landingzone"
        )

        playersubstitionsgamedata_landingzone.data = (
            playersubstitionsgamedata_landingzone.data.explode("inOutsList").unnest(
                "inOutsList"
            )
        )
        playersubstitionsgamedata_landingzone.data = (
            playersubstitionsgamedata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("idMatchIntern")
            )
        )
        playersubstitionsgamedata_landingzone.data = (
            playersubstitionsgamedata_landingzone.data.with_columns(
                pl.col("uuid").alias("playerUuid")
            ).unique()
        )
        playersubstitionsgamedata_landingzone = (
            playersubstitionsgamedata_landingzone.custom_select(
                [
                    "playerUuid",
                    "actorId",
                    "teamId",
                    "type",
                    "minuteAbsolut",
                    "pointDiff",
                    "idMatchIntern",
                ]
            )
        )

        playersubstitionsgamedata_raw = CustomDF(
            "playersubstitionsgamedata_raw",
            initial_df=playersubstitionsgamedata_landingzone.data,
        )
        playersubstitionsgamedata_raw.write_table()

    elif table_name == "playergamestatsdata_raw":
        playergamestatsdata_landingzone = CustomDF("playergamestatsdata_landingzone")

        playergamestatsdata_landingzone.data = (
            playergamestatsdata_landingzone.data.unnest("data")
        )
        playergamestatsdata_landingzone.data = (
            playergamestatsdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("idMatchIntern")
            )
        )
        playergamestatsdata_landingzone.data = (
            playergamestatsdata_landingzone.data.with_columns(
                pl.col("uuid").alias("playerUuid")
            )
        )
        playergamestatsdata_landingzone = playergamestatsdata_landingzone.custom_select(
            [
                "playerUuid",
                "actorId",
                "teamId",
                "score",
                "shotsOfOneAttempted",
                "shotsOfOneSuccessful",
                "shotsOfTwoAttempted",
                "shotsOfTwoSuccessful",
                "shotsOfThreeAttempted",
                "shotsOfThreeSuccessful",
                "gamePlayed",
                "timePlayed",
                "rebounds",
                "assists",
                "steals",
                "personal",
                "valoration",
                "idMatchIntern",
            ]
        )
        playergamestatsdata_landingzone.convert_data_types(
            ["gamePlayed", "timePlayed"], pl.Int16
        )

        playergamestatsdata_raw = CustomDF(
            "playergamestatsdata_raw", initial_df=playergamestatsdata_landingzone.data
        )
        playergamestatsdata_raw.write_table()

    elif table_name == "teamgamestatsdata_raw":
        teamgamestatsdata_landingzone = CustomDF("teamgamestatsdata_landingzone")

        teamgamestatsdata_landingzone.data = teamgamestatsdata_landingzone.data.unnest(
            "data"
        )
        teamgamestatsdata_landingzone.data = (
            teamgamestatsdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("idMatchIntern")
            )
        )
        teamgamestatsdata_landingzone.data = (
            teamgamestatsdata_landingzone.data.with_columns(
                pl.col("teamIdExtern").alias("team_uuid")
            )
        )
        teamgamestatsdata_landingzone = teamgamestatsdata_landingzone.custom_select(
            [
                "team_uuid",
                "score",
                "shotsOfOneAttempted",
                "shotsOfOneSuccessful",
                "shotsOfTwoAttempted",
                "shotsOfTwoSuccessful",
                "shotsOfThreeAttempted",
                "shotsOfThreeSuccessful",
                "rebounds",
                "assists",
                "steals",
                "personal",
                "valoration",
                "idMatchIntern",
            ]
        )

        teamgamestatsdata_raw = CustomDF(
            "teamgamestatsdata_raw", initial_df=teamgamestatsdata_landingzone.data
        )
        teamgamestatsdata_raw.write_table()

    elif table_name == "playergameshotsdata2ptmade_raw":
        playergameshotsdata_landingzone = CustomDF(
            "playergameshotsdata2ptmade_landingzone"
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("game_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("uuid").alias("player_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.unnest("data")
        )
        twopoints_made = playergameshotsdata_landingzone.custom_select(
            ["player_uuid", "game_uuid", "shootingOfTwoSuccessfulPoint"]
        )
        twopoints_made.data = twopoints_made.data.explode(
            "shootingOfTwoSuccessfulPoint"
        ).unnest("shootingOfTwoSuccessfulPoint")
        twopoints_made.data = twopoints_made.data.with_columns(
            pl.lit("2PT").alias("shot_type")
        )
        twopoints_made.data = twopoints_made.data.with_columns(
            pl.lit("made").alias("outcome")
        )

        twopoints_made.data = twopoints_made.data.with_columns(
            pl.col("min").cast(pl.Int64).alias("minute")
        ).unique()
        twopoints_made = twopoints_made.custom_select(
            [
                "game_uuid",
                "player_uuid",
                "shot_type",
                "period",
                "minute",
                "xnormalize",
                "ynormalize",
                "outcome",
            ]
        )
        twopoints_made.data = twopoints_made.data.filter(
            pl.col("xnormalize").is_not_null() & pl.col("ynormalize").is_not_null()
        )

        playergameshotsdata_raw = CustomDF(
            "playergameshotsdata2ptmade_raw", initial_df=twopoints_made.data
        )
        playergameshotsdata_raw.write_table()
    elif table_name == "playergameshotsdata3ptmade_raw":
        playergameshotsdata_landingzone = CustomDF(
            "playergameshotsdata3ptmade_landingzone"
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("game_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("uuid").alias("player_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.unnest("data")
        )

        threepoints_made = playergameshotsdata_landingzone.custom_select(
            ["player_uuid", "game_uuid", "shootingOfThreeSuccessfulPoint"]
        )
        threepoints_made.data = threepoints_made.data.explode(
            "shootingOfThreeSuccessfulPoint"
        ).unnest("shootingOfThreeSuccessfulPoint")
        threepoints_made.data = threepoints_made.data.with_columns(
            pl.lit("3PT").alias("shot_type")
        )
        threepoints_made.data = threepoints_made.data.with_columns(
            pl.lit("made").alias("outcome")
        )
        threepoints_made.data = threepoints_made.data.with_columns(
            pl.col("min").cast(pl.Int64).alias("minute")
        ).unique()
        threepoints_made = threepoints_made.custom_select(
            [
                "game_uuid",
                "player_uuid",
                "shot_type",
                "period",
                "minute",
                "xnormalize",
                "ynormalize",
                "outcome",
            ]
        )
        threepoints_made.data = threepoints_made.data.filter(
            pl.col("xnormalize").is_not_null() & pl.col("ynormalize").is_not_null()
        )
        playergameshotsdata_raw = CustomDF(
            "playergameshotsdata3ptmade_raw", initial_df=threepoints_made.data
        )
        playergameshotsdata_raw.write_table()

    elif table_name == "playergameshotsdata2ptmissed_raw":
        playergameshotsdata_landingzone = CustomDF(
            "playergameshotsdata2ptmissed_landingzone"
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("game_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("uuid").alias("player_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.unnest("data")
        )
        twopoints_failed = playergameshotsdata_landingzone.custom_select(
            ["player_uuid", "game_uuid", "shootingOfTwoFailedPoint"]
        )
        twopoints_failed.data = twopoints_failed.data.explode(
            "shootingOfTwoFailedPoint"
        ).unnest("shootingOfTwoFailedPoint")
        twopoints_failed.data = twopoints_failed.data.with_columns(
            pl.lit("2PT").alias("shot_type")
        )
        twopoints_failed.data = twopoints_failed.data.with_columns(
            pl.lit("missed").alias("outcome")
        )
        twopoints_failed.data = twopoints_failed.data.with_columns(
            pl.col("min").cast(pl.Int64).alias("minute")
        ).unique()
        twopoints_failed = twopoints_failed.custom_select(
            [
                "game_uuid",
                "player_uuid",
                "shot_type",
                "period",
                "minute",
                "xnormalize",
                "ynormalize",
                "outcome",
            ]
        )
        twopoints_failed.data = twopoints_failed.data.filter(
            pl.col("xnormalize").is_not_null() & pl.col("ynormalize").is_not_null()
        )

        playergameshotsdata_raw = CustomDF(
            "playergameshotsdata2ptmissed_raw", initial_df=twopoints_failed.data
        )
        playergameshotsdata_raw.write_table()

    elif table_name == "playergameshotsdata3ptmissed_raw":
        playergameshotsdata_landingzone = CustomDF(
            "playergameshotsdata3ptmissed_landingzone"
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("game_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.with_columns(
                pl.col("uuid").alias("player_uuid")
            )
        )
        playergameshotsdata_landingzone.data = (
            playergameshotsdata_landingzone.data.unnest("data")
        )
        threepoints_failed = playergameshotsdata_landingzone.custom_select(
            ["player_uuid", "game_uuid", "shootingOfThreeFailedPoint"]
        )
        threepoints_failed.data = threepoints_failed.data.explode(
            "shootingOfThreeFailedPoint"
        ).unnest("shootingOfThreeFailedPoint")
        threepoints_failed.data = threepoints_failed.data.with_columns(
            pl.lit("3PT").alias("shot_type")
        )
        threepoints_failed.data = threepoints_failed.data.with_columns(
            pl.lit("missed").alias("outcome")
        )
        threepoints_failed.data = threepoints_failed.data.with_columns(
            pl.col("min").cast(pl.Int64).alias("minute")
        ).unique()
        threepoints_failed = threepoints_failed.custom_select(
            [
                "game_uuid",
                "player_uuid",
                "shot_type",
                "period",
                "minute",
                "xnormalize",
                "ynormalize",
                "outcome",
            ]
        )
        threepoints_failed.data = threepoints_failed.data.filter(
            pl.col("xnormalize").is_not_null() & pl.col("ynormalize").is_not_null()
        )

        playergameshotsdata_raw = CustomDF(
            "playergameshotsdata3ptmissed_raw", initial_df=threepoints_failed.data
        )
        playergameshotsdata_raw.write_table()

    elif table_name == "gameteamscoresdata_raw":
        gameteamscoresdata_landingzone = CustomDF("gameteamscoresdata_landingzone")

        gameteamscoresdata_landingzone.data = (
            gameteamscoresdata_landingzone.data.explode("score").unnest("score")
        )
        gameteamscoresdata_landingzone.data = (
            gameteamscoresdata_landingzone.data.with_columns(
                pl.col("file_name").str.replace(".json", "").alias("idMatchIntern")
            )
        )
        gameteamscoresdata_landingzone = gameteamscoresdata_landingzone.custom_select(
            [
                "idMatchIntern",
                "local",
                "visit",
                "minuteQuarter",
                "minuteAbsolute",
                "period",
            ]
        )

        gameteamscoresdata_raw = CustomDF(
            "gameteamscoresdata_raw", initial_df=gameteamscoresdata_landingzone.data
        )
        gameteamscoresdata_raw.write_table()

    elif table_name == "gamedatametadata_raw":
        gamedatametadata_landingzone = CustomDF("gamedatametadata_landingzone")

        gamedatametadata_landingzone.convert_data_types(["year"], pl.Int64)

        gamedatametadata_raw = CustomDF(
            "gamedatametadata_raw",
            initial_df=gamedatametadata_landingzone.data.unique(),
        )
        gamedatametadata_raw.write_table()

    else:
        raise ValueError(
            f"The table: {table_name} is not specified in the processing functions"
        )

    return True
