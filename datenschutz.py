import pandas as pd
import requests
from bs4 import BeautifulSoup
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

def get_datenschutz_url(url):
  try:
    response = requests.get(url, timeout=5)
    if response.status_code == 200:
      soup = BeautifulSoup(response.text, 'html.parser')
      
      # Check if base tag exists and use that as the base for making URLs absolute
      base = soup.find('base')
      if base:
        base_url = base['href']
      else:
        base_url = url

      for link in soup.find_all('a'):
        href = link.get('href')
        if href is not None:  # Make sure href is not None before using .lower() on it
          if 'datenschutz' in href.lower():
            return urljoin(base_url, href).strip()
  except:
    pass

  return None

def add_datenschutz_urls(csv_input_file, csv_output_file):
  # Read input CSV
  df = pd.read_csv(csv_input_file, dtype={'zip_code': str})
  
  # Add URL scheme to all website URLs if there's none
  df['website'] = df['website'].apply(add_url_scheme_if_none)
  
  # Convert the 'website' column to a list
  urls = df['website'].tolist()

  datenschutz_urls = []
  
  # Use ThreadPoolExecutor for concurrency
  with ThreadPoolExecutor(max_workers=1) as executor:
    # Create a new future for each URL
    futures = {executor.submit(get_datenschutz_url, url): url for url in urls}
    
    # Iterate over the futures as they complete. 'as_completed' returns an iterator that yields futures as they complete.
    for future in tqdm(as_completed(futures), total=len(futures), desc="Adding Datenschutz URLs: ", bar_format='{desc}{percentage:3.0f}% {bar} {n_fmt}/{total_fmt} - [{elapsed}]'):
        # Get the result from the future and append to datenschutz_urls
        result = future.result()
        datenschutz_urls.append(result)
  
  # Add datenschutz URLs to the DataFrame
  df['datenschutz'] = datenschutz_urls
  
  # Write the DataFrame to a CSV file
  df.to_csv(csv_output_file, index=False)
  
  print(f"[green]Added [bold yellow]{len(df)}[/bold yellow] Datenschutz URLs to [bold yellow]{csv_output_file}[/bold yellow].")

if __name__ == "__main__":
  add_datenschutz_urls('output/physio_impressum_result.csv', 'output/physio_datenschutz.csv')