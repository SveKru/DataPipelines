import os
import json
from dotenv import load_dotenv
from connectors.scrape_data import data_scraper
from processing.processing_raw import generate_table_raw
from processing.processing_datamodel import generate_table_datamodel
from processing.processing_enriched import generate_table_enriched

# Load all data from .env file
load_dotenv()

# Scrape all of the data
base_url = os.getenv("BASE_URL", "")
api_url = os.getenv("API_URL", "")
writing_path = os.getenv("WRITING_PATH", "")
query_args_list = json.loads(os.getenv("QUERY_ARGS_LIST", "[]"))
# data_scraper(
#     writing_path=writing_path,
#     query_args_list=query_args_list,
#     base_url=base_url,
#     api_url=api_url
# )

# generate_table_raw('gamedata_raw')
# generate_table_raw('teamdata_raw')
# generate_table_raw('playergamedata_raw')
# generate_table_raw('playersubstitionsgamedata_raw')
# generate_table_raw('playergamestatsdata_raw')
# generate_table_raw('teamgamestatsdata_raw')
# generate_table_raw('playergameshotsdata2ptmade_raw')
# generate_table_raw('playergameshotsdata2ptmissed_raw')
# generate_table_raw('playergameshotsdata3ptmade_raw')
# generate_table_raw('playergameshotsdata3ptmissed_raw')
# generate_table_raw('gameteamscoresdata_raw')
# generate_table_raw('gamedatametadata_raw')

# generate_table_datamodel('gamedata_datamodel')
# generate_table_datamodel('gameteamscoresdata_datamodel')
# generate_table_datamodel('playerdata_datamodel')
# generate_table_datamodel('playergamestatsdata_datamodel')
# generate_table_datamodel('playergameshotsdata_datamodel')
# generate_table_datamodel('teamgamestatsdata_datamodel')
# generate_table_datamodel('playergamesubstitionsgamedata_datamodel')
# generate_table_datamodel('teamdata_datamodel')

# generate_table_enriched('playergameplusminus_enriched')
# generate_table_enriched('playerplusminus_enriched')
# generate_table_enriched('playerstatssummary_enriched')
# # This does not work yet
# generate_table_enriched('player2ptsummary_enriched')
# generate_table_enriched('player3ptsummary_enriched')
# generate_table_enriched('player3ptsummary_enriched')
# generate_table_enriched('playeranalytics_enriched')
# generate_table_enriched('teamstatssummary_enriched')
# generate_table_enriched('opponentsstatssummary_enriched')
# generate_table_enriched('opponentstrendssummary_enriched')
# generate_table_enriched('teamanalytics_enriched')
# generate_table_enriched("teamgameanalytics_enriched")
generate_table_enriched("playergameimpact_enriched")
generate_table_enriched("playergameanalytics_enriched")

# Export the edges of the datasets
