import pandas as pd
import requests
from rich import print
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def add_url_scheme_if_none(url):
  # Check if URL scheme (http/https) is present and add it if not
  parsed_url = urlparse(url)
  if not parsed_url.scheme:
    return "http://" + url
  return url

def get_impressum_url(url):
  # Define potential paths for impressum page
  impressum_paths = ['/impressum', '/impressum.html', '/legal']

  # Iterate over potential paths and try to make a GET request
  for path in impressum_paths:
    impressum_url = urljoin(url, path)
    try:
      # If the request is successful, return the URL
      response = requests.get(impressum_url, timeout=5)
      if response.status_code == 200:
        return impressum_url
    except requests.exceptions.RequestException:
      pass
  return None  # Return None if no impressum page is found

def add_impressum_urls(csv_input_file, csv_output_file):
  # Read input CSV
  df = pd.read_csv(csv_input_file)
  
  # Add URL scheme to all website URLs if there's none
  df['website'] = df['website'].apply(add_url_scheme_if_none)
  
  # Convert the 'website' column to a list
  urls = df['website'].tolist()

  impressum_urls = []
  
  # Use ThreadPoolExecutor for concurrency
  with ThreadPoolExecutor(max_workers=20) as executor:
    # Create a new future for each URL
    futures = {executor.submit(get_impressum_url, url): url for url in urls}
    
    # Iterate over the futures as they complete. 'as_completed' returns an iterator that yields futures as they complete.
    for future in tqdm(as_completed(futures), total=len(futures), desc="Adding Impressum URLs: ", bar_format='{desc}{percentage:3.0f}% {bar} {n_fmt}/{total_fmt} - [{elapsed}]'):
        # Get the result from the future and append to impressum_urls
        result = future.result()
        impressum_urls.append(result)
  
  # Add impressum URLs to the DataFrame
  df['Impressum'] = impressum_urls
  
  # Drop rows where Impressum is NaN
  df = df.dropna(subset=['Impressum'])
  
  # Write the DataFrame to a CSV file
  df.to_csv(csv_output_file, index=False)
  
  print(f"[green]Added [bold yellow]{len(df)}[/bold yellow] Impressum URLs to [bold yellow]{csv_output_file}[/bold yellow].")

if __name__ == "__main__":
  add_impressum_urls('output/googlemaps.csv', 'output/physio_impressum.csv')