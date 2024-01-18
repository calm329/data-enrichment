import os
import requests
import pandas as pd
import json
import dotenv
from time import sleep
from tqdm import tqdm
from rich import print

dotenv.load_dotenv()

class SnovAPI:
  def __init__(self, client_id, client_secret):
    self.client_id = client_id
    self.client_secret = client_secret
    self.token = self.__get_access_token()

  def __get_access_token(self):
    params = {
      'grant_type': 'client_credentials',
      'client_id': self.client_id,
      'client_secret': self.client_secret
    }
    res = requests.post('https://api.snov.io/v1/oauth/access_token', data=params)
    response = json.loads(res.text)
    
    if 'error' in response:
      raise Exception("Failed to obtain access token.")
    return response['access_token']

  def get_email_finder(self, domain, first_name, last_name):
    params = {
      'access_token': self.token,
      'domain': domain,
      'firstName': first_name,
      'lastName': last_name
    }
    res = requests.post('https://api.snov.io/v1/get-emails-from-names', data=params)
    
    if res.status_code == 200:
      response_data = res.json()
      
      if response_data.get("success") and response_data.get("data", {}).get("emails"):
        for email_info in response_data["data"]["emails"]:
          if email_info.get("emailStatus") == "valid":
            return email_info["email"]
    return None

def enrich_data_with_email_finder(csv_input_file, csv_output_file):
  # Initialize the SNOV API with client id and secret
  snov = SnovAPI(os.getenv('SNOV_CLIENT_ID'), os.getenv('SNOV_CLIENT_SECRET'))

  # Load the dataframe from CSV
  df = pd.read_csv(csv_input_file, dtype={'zip_code': str})

  # Initializing a column to store private emails from the enriched data
  df['private_email'] = None

  # Calculate the wait time to respect the rate limit (400 per hour)
  wait_time = 60 / 60
  
  count = 0

  # Begin data enrichment using SNOV API
  with tqdm(total=df.shape[0], bar_format='{desc}{percentage:3.0f}% {bar} {n:.0f}/{total_fmt} - [{elapsed}]',) as pbar:
    for index, row in df.iterrows():
      pbar.set_description("Enriching data")
      website = row.get('website')
      first_name = row.get('FirstName')
      last_name = row.get('LastName')

      if all([website, first_name, last_name]):
        domain = website.split("//")[-1].split("/")[0]
        
        email = snov.get_email_finder(domain, first_name, last_name)

        if email:
          df.loc[index, 'private_email'] = email
          count += 1

      sleep_time = wait_time

      while sleep_time > 0:
        pbar.set_description(f"Sleeping [{tqdm.format_interval(wait_time - sleep_time)}/{tqdm.format_interval(sleep_time)}]") 
        sleep(min(sleep_time, 1))
        sleep_time -= 1
        pbar.update(1 / wait_time)

  # Save the enriched dataframe to a CSV file
  chunk_size = eval(os.getenv('CHUNK_SIZE'))
  chunks = [x for x in range(0, df.shape[0], chunk_size)]

  # Get base file name
  base = os.path.basename(csv_output_file)
  # Remove .csv extension
  filename = os.path.splitext(base)[0]

  for i in range(len(chunks) - 1):
      df.iloc[chunks[i]:chunks[i + 1]].to_csv(f'output/{filename}_{i}.csv', index=False)

  # Edge case for the final chunk
  df.iloc[chunks[-1]:].to_csv(f'output/{filename}_{len(chunks)}.csv', index=False)

  print(f"[green]Done enriching data. Enriched [bold yellow]{count}[/bold yellow] rows. Saved to [bold yellow]your_output_file.csv[/bold yellow].")


# Run the main function
if __name__ == "__main__":
  enrich_data_with_email_finder('output/physio_impressum_result.csv', 'output/physio_enriched.csv')