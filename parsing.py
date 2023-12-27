import os
import json
from bs4 import BeautifulSoup
import pandas as pd
from rich import print
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()  # load environment variables

def fetch_and_process(url):
  """
  This function takes a URL as input and fetches the HTML. It then extracts the contact 
  details from that HTML using GPT-3.5-turbo.
  """
  
  try:
    # Send a GET request to the URL
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    text = soup.find('body').get_text(strip=True)

    # Use GPT-3.5-turbo to extract contact information from the text
    client = OpenAI()
    completion = client.chat.completions.create(
      model="gpt-3.5-turbo",
      messages=[
          {
            "role": "user",
            "content": "Find a contact person with firstname, lastname and email in the following text and output them in JSON format. Ignore titles like Dr., etc. The response must be like this - {firstname: '', lastname: '', email: ''}:\n\n" + text
          }
      ]
    )

    # Extract contact information from the GPT-3.5-turbo completion
    contact_info = completion.choices[0].message.content
    contact_info_json = json.loads(contact_info)
    return contact_info_json.get('firstname', None), contact_info_json.get('lastname', None), contact_info_json.get('email', None)

  except Exception as e:
    pass
  return None, None, None

def extract_contact_from_websites(input_file, output_file):
  """
  This function reads a CSV file containing URLs, fetches and processes each URL 
  to extract contact information, filters out the URLs that do not provide any 
  contact information and writes the remaining data to another CSV file.
  """

  # Load the data containing the URLs
  df = pd.read_csv(input_file)
  urls = df['Impressum'].dropna().tolist()
  
  results = []
  # Fetch and process all URLs in parallel
  with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_and_process, url): url for url in urls}
    for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting contact information: ", bar_format='{desc}{percentage:3.0f}% {bar} {n_fmt}/{total_fmt} - [{elapsed}]'):
      result = future.result()
      results.append(result)

  # Prepare dataframe with valid Impressum URLs
  df_valid = df[df['Impressum'].notna()].copy()

  # Update dataframe with results
  df_valid[['FirstName', 'LastName', "Email"]] = pd.DataFrame(results, index=df_valid.index)

  # Filter out entries where 'FirstName', 'LastName', and 'Email' are all None
  df_valid = df_valid.dropna(subset=['FirstName', 'LastName', 'Email'], how='all')

  # Save the results to a CSV file
  df_valid.to_csv(output_file, index=False)
  
  print(f"[green]Extracted [bold yellow]{len(df_valid)}[/bold yellow] contact information. Saved to [bold yellow]{output_file}[/bold yellow].")

if __name__ == "__main__":
  extract_contact_from_websites("output/physio_impressum.csv", "output/physio_impressum_result.csv")