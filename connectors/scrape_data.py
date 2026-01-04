import os
import requests
from bs4 import BeautifulSoup
import json


def save_html_to_file(html_content, file_path):
    """Save the raw HTML content to a file."""
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(html_content)


def load_html_from_file(file_path):
    """Load the raw HTML content from a file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def save_json_to_file(json_data, file_path):
    """Save the JSON data to a file."""
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(json_data, file, indent=4)


def json_file_exists(uuid,writing_path):
    """Check if the JSON file for the given UUID already exists."""
    data_dir = os.path.join(writing_path, "data")
    file_path = os.path.join(data_dir, f"{uuid}.json")
    return os.path.exists(file_path)


def scrape_webpage(url):
    """Scrape the initial webpage to find all appropriate links and their UUIDs."""
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all links containing the UUID (adjust the selector as needed)
        links = soup.find_all("a", href=True)
        uuids = []
        print(links)
        for link in links:
            if link["href"].startswith(
                # This is the URL pattern for the links containing UUIDs from the past seasons
                # r"/competicions-anteriors/resultat/estadistiques"
                'https://www.basquetcatala.cat/estadistiques'
            ) and 'video' not in link['href']:
                href = link["href"]
                print(f"Found link: {href}")

                # Extract the UUID from the link
                uuid = href.split("/")[-1]
                print(f"Extracted UUID: {uuid}")
                uuids.append(uuid)

        return uuids
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while scraping the webpage: {e}")
        return []


def fetch_game_data(api_url, uuid, writing_path):
    """Fetch game data from the API using the UUID."""
    try:
        # Construct the API URL
        full_api_url = f"{api_url}/{uuid}?currentSeason=true"
        print(f"Fetching data from API: {full_api_url}")

        # Send a GET request to the API
        response = requests.get(full_api_url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

        # Parse the JSON response
        json_data = response.json()

        # Save the JSON data to a file
        data_dir = f"{writing_path}/data"
        os.makedirs(data_dir, exist_ok=True)
        file_path = os.path.join(data_dir, f"{uuid}.json")
        save_json_to_file(json_data, file_path)

        print(f"JSON data saved to file: {file_path}")
        return json_data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching game data: {e}")
        return None

def data_scraper(
        writing_path: str,
        query_args_list: list[dict],
        base_url: str,
        api_url: str
):
    for arg in query_args_list:
        #  This is the URL for all of the past competitions, which are archived
        # initial_url = f"https://www.basquetcatala.cat/competicions-anteriors/resultat/{year}/{competition_code}/{phase}/{group}"
        initial_url = f"{base_url}/{arg['url']}"
        # Replace with the base API URL

        # Step 1: Scrape the initial webpage to find all UUIDs
        uuids = scrape_webpage(initial_url)

        print(uuids)

        if uuids:
            # Step 2: Use each UUID to fetch game data from the API
            for uuid in uuids:
                if json_file_exists(uuid, writing_path):
                    print(f"JSON file for UUID {uuid} already exists. Skipping...")
                    continue

                # save the initial arguments to a json dict
                initial_args = {
                    "year": arg['year'],
                    "competition_code": arg['competition_code'],
                    "phase": arg['phase'],
                    "group": arg['group'],
                    "long_name": arg['long_name'],
                    "uuid": uuid,
                }
                # write the arg to file in append mode
                os.makedirs(f"{writing_path}/mapping", exist_ok=True)
                with open(f"{writing_path}/mapping/query_args.json", "a", encoding="utf-8") as file:
                    json.dump(initial_args, file)
                    file.write("\n")

                game_data = fetch_game_data(api_url, uuid, writing_path)

                if game_data:
                    print(f"Game data for UUID {uuid} fetched successfully.")
