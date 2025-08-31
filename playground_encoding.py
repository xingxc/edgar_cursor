#%%

import requests
import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup

def save_url_content(url):
    """
    Downloads content from a URL and saves it to a local file,
    using Beautiful Soup to format the content prettily.
    """
    try:
        # Get the file name from the URL path
        parsed_url = urlparse(url)
        file_name = os.path.basename(parsed_url.path)
        
        if not file_name:
            print("Could not determine a file name from the URL. Saving as 'downloaded_page.html'.")
            file_name = 'downloaded_page.html'

        # --- Part 1: Download the content using requests ---
        # Fetch the raw content from the URL
        print(f"Fetching content from: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # --- Part 2: Parse and format with Beautiful Soup (optional but good practice) ---
        # Parse the raw HTML into a structured tree
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Format the HTML nicely before writing
        pretty_html = soup.prettify()

        # --- Part 3: Write the content to a local file ---
        print(f"Writing content to file: {file_name}")
        with open(file_name, 'w', encoding='utf-8') as f:
            f.write(pretty_html)

        print(f"Successfully saved content to {file_name}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching the URL: {e}")
    except IOError as e:
        print(f"Error writing to file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# The URL to download
url_to_save = 'https://www.sec.gov/Archives/edgar/data/0000019617/000001961725000615/jpm-20250630.htm'

# Run the function
save_url_content(url_to_save)