import os
import json
from bs4 import BeautifulSoup
import pandas as pd
from PyPDF4 import PdfFileReader
from io import BytesIO
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
    
    if response.status_code == 200:    
      if url.endswith('.pdf'):
        # Creating a BytesIO object
        file = BytesIO(response.content)

        # Creating a PdfFileReader object
        pdf_reader = PdfFileReader(file)

        # Getting the number of pages in the PDF
        num_pages = len(pdf_reader.pages)

        content = ""

        # Creating a pdf reader object
        for page in range(num_pages):
            pdf_page = pdf_reader.pages[page]
            content += pdf_page.extractText() + "\n"
      else:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        content = soup.find('body').get_text(strip=True)

      # Use GPT-3.5-turbo to extract contact information from the text
      client = OpenAI()
      completion = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {
              "role": "user",
              "content": "Find a contact person with firstname, lastname and email in the following text and output them in JSON format. Ignore titles like Dr., etc. The response must be like this - {firstname: '', lastname: '', email: ''}:\n\n" + content
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
  urls = df['impressum'].dropna().tolist()
  
  results = []
  # Fetch and process all URLs in parallel
  with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_and_process, url): url for url in urls}
    for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting contact information: ", bar_format='{desc}{percentage:3.0f}% {bar} {n_fmt}/{total_fmt} - [{elapsed}]'):
      result = future.result()
      results.append(result)

  # Prepare dataframe with valid Impressum URLs
  df_valid = df[df['impressum'].notna()].copy()

  # Update dataframe with results
  df_valid[['firstName', 'lastName', "email"]] = pd.DataFrame(results, index=df_valid.index)

  # Filter out entries where 'firstName', 'lastName', and 'email' are all None
  df_valid = df_valid.dropna(subset=['firstName', 'lastName', 'email'], how='all')

  # Save the results to a CSV file
  df_valid.to_csv(output_file, index=False)
  
  print(f"[green]Extracted [bold yellow]{len(df_valid)}[/bold yellow] contact information. Saved to [bold yellow]{output_file}[/bold yellow].")

if __name__ == "__main__":
  extract_contact_from_websites("output/physio_impressum.csv", "output/physio_impressum_result.csv")