import os
import re
import csv
import serpapi
import dotenv
from tqdm import tqdm
import time
from rich import print
import warnings
from tqdm import TqdmWarning
warnings.filterwarnings("ignore", category=TqdmWarning)

# Load environment variables
dotenv.load_dotenv()

def parse_address_and_get_details(result):
  # Retrieve address from the result 
  full_address = result.get('address', '')
  # Regular expression to match specific pattern in the address
  match = re.match(r'(.+),\s+(\d{5})\s+(.+),\s+(.+)', full_address)

  if match is None:
    # If the address does not match the pattern, skip this result
    return None

  # Parsing individual address components using regular expressions
  street_name_and_no = match.group(1)
  zip_code = match.group(2)
  city = match.group(3)
  country = match.group(4)
  
  # If the country is not Germany, then skip this result
  if country not in ['Deutschland', 'Germany']:
    return None
  
  # Retrieve other details 
  title = result.get('title', '')
  website = result.get('website', '')
  phone = result.get('phone', '')
  
  # If website is missing, skip this result
  if website == '':
    return None

  # Return the parsed address and details as a list
  return [title, street_name_and_no, zip_code, city, country, website, phone]

def extract_place_information(keyword, postcode, save_path):
  # Create a SerpApi client for Google Maps web scraping
  client = serpapi.Client(api_key=os.getenv("SERP_APIKEY"))

  # Specify parameters for the search
  params = {
    "engine": "google_maps",
    "type": "search",
    "google_domain": "google.de",
    "q": f"{keyword} near {postcode} in Deutschland",
    "hl": "de",
    "ll": "@52.5200,13.4050,15z",
    "gl": "DE"
  }

  # To store result data
  data = []
  
  # Maintain a delay between each batch of requests
  wait_time = 3600 / 3000 * 20
  total_count = 20
  
  # A progress bar to track the number of requests
  with tqdm(total=total_count/20, bar_format='{desc}{percentage:3.0f}% {bar} {n:.0f}/' + f'{total_count}' + ' - [{elapsed}]') as pbar:
    for i in range(0, total_count, 20):
      pbar.set_description("Requesting to SERP API")
      params["start"] = i
      results = client.search(params)
      places = results['local_results']

      for place in places:
        parsed_result = parse_address_and_get_details(place)
        if parsed_result:
          data.append(parsed_result)
          pbar.bar_format='{desc}{percentage:3.0f}% {bar} ' + f'[{i+20}|{len(data)}]' + '/' + f'{total_count}' + ' - [{elapsed}]'

      # wait before sending the next request 
      sleep_time = wait_time
      while sleep_time > 0:
        pbar.set_description(f"Sleeping [{tqdm.format_interval(wait_time - sleep_time)}/{tqdm.format_interval(sleep_time)}]")
        time.sleep(min(sleep_time, 1))  
        sleep_time -= 1
        pbar.update(1 / wait_time)
    pbar.set_description('Done')

  with open(save_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['title', 'street_name_and_no', 'zip_code', 'city', 'country', 'website', 'phone'])
    writer.writerows(data)
    
  print(f"[green]Done Google Maps data extraction. Extracted [bold yellow]{len(data)}[/bold yellow] results. Saved to [bold yellow]{save_path}[/bold yellow].")

if __name__ == "__main__":
  # example usage of the function
  extract_place_information("Kosmetikstudio", "01945", save_path='output/googlemaps.csv')
