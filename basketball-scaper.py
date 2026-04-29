import os
import json
from dotenv import load_dotenv
from connectors.scrape_data import data_scraper
from processing.processing_raw import generate_table_raw
from processing.processing_datamodel import generate_table_datamodel
from processing.processing_enriched import generate_table_enriched

# Load all data from .env file
load_dotenv()

# Load session cookies and user agent from environment
raw_cookie_string = os.getenv("RAW_COOKIE_STRING", "")
user_agent = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")

# Scrape all of the data
base_url = os.getenv("BASE_URL", "")
api_url = os.getenv("API_URL", "")
writing_path = os.getenv("WRITING_PATH", "")
query_args_list = json.loads(os.getenv("QUERY_ARGS_LIST", "[]"))

# Prioritize raw cookie string if provided, else fallback to browser/token
cookies = None
if raw_cookie_string:
    # Strip surrounding quotes if present (e.g. if .env value was quoted)
    raw_cookie_string = raw_cookie_string.strip('"').strip("'")
    # Split on "; " or ";" to handle both formats copied from browsers
    parts = [x.strip() for x in raw_cookie_string.replace("; ", ";").split(";") if "=" in x]
    cookies = dict(x.split("=", 1) for x in parts)
    print(f"Loaded {len(cookies)} cookies from RAW_COOKIE_STRING: {list(cookies.keys())}")
elif os.getenv("GRECAPTCHA_TOKEN"):
    cookies = {"_grecaptcha": os.getenv("GRECAPTCHA_TOKEN")}

# data_scraper(
#     writing_path=writing_path,
#     query_args_list=query_args_list,
#     base_url=base_url,
#     api_url=api_url,
#     cookies=cookies,
#     user_agent=user_agent
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

# LAYER 1: Atomic per-game tables
generate_table_enriched('gamecompetitiveness_enriched')
generate_table_enriched('playergameplusminus_enriched')
generate_table_enriched('playergameimpact_enriched')
generate_table_enriched('playergamequarterstats_enriched')
generate_table_enriched('playergame_shots_enriched')
generate_table_enriched('playerclutchperformance_enriched')
generate_table_enriched('teamgameanalytics_enriched')
generate_table_enriched('teamgamequarterperformance_enriched')
generate_table_enriched('teamgame_shots_enriched')
generate_table_enriched('fiveplayer_combinations_enriched')
generate_table_enriched('threeplayer_combinations_enriched')

# LAYER 2: Wide per-game tables
generate_table_enriched('playergameanalytics_enriched')

# LAYER 3: Aggregated tables
generate_table_enriched('playerstatssummary_enriched')
generate_table_enriched('player2ptsummary_enriched')
generate_table_enriched('player3ptsummary_enriched')
generate_table_enriched('playerplusminus_enriched')
generate_table_enriched('playeranalytics_enriched')
generate_table_enriched('teamstatssummary_enriched')
generate_table_enriched('opponentsstatssummary_enriched')
generate_table_enriched('opponentstrendssummary_enriched')
generate_table_enriched('teamhomeawaysplits_enriched')
generate_table_enriched('teamquarterperformance_enriched')
generate_table_enriched('teamanalytics_enriched')
