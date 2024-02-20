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

def correct_email(email):
  # replace (at) and [at] with @
  return email.replace("(at)", "@").replace("[at]", "@")
  
def fetch_and_process(url, keywords):
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
        paragraphs = soup.find_all('p')  # find all p tags

        content = ""

        # loop over each p tag and extract the text
        for paragraph in paragraphs:
            content += "\n" + paragraph.get_text(strip=True)

      result = []

      for keyword in keywords:
        if keyword.lower() in content.lower():
          result.append('Y')
        else:
          result.append('N')

      return result      
  except Exception as e:
    pass
  return ['N'] * len(keywords)

def search_in_privacy(input_file, output_file, keywords):
  """
  This function reads a CSV file containing URLs, fetches and processes each URL 
  to extract contact information, filters out the URLs that do not provide any 
  contact information and writes the remaining data to another CSV file.
  """

  # Load the data containing the URLs
  df = pd.read_csv(input_file, dtype={'zip_code': str})
  urls = df['datenschutz'].dropna().tolist()
  
  results = []
  # Fetch and process all URLs in parallel
  with ThreadPoolExecutor(max_workers=1) as executor:
    futures = {executor.submit(fetch_and_process, url, keywords): url for url in urls}
    for future in tqdm(as_completed(futures), total=len(futures), desc="Finding keywords in privacy: ", bar_format='{desc}{percentage:3.0f}% {bar} {n_fmt}/{total_fmt} - [{elapsed}]'):
      result = future.result()
      results.append(result)

  # Prepare dataframe with valid Impressum URLs
  df_valid = df[df['datenschutz'].notna()].copy()

  df_valid['zip_code'] = df_valid['zip_code'].astype(str)

  # Update dataframe with results
  df_valid[keywords] = pd.DataFrame(results, index=df_valid.index)

  # Save the results to a CSV file
  df_valid.to_csv(output_file, index=False)
  
  print(f"[green]Found [bold yellow]{results.count('Y')}[/bold yellow] results with keywords [bold yellow]{', '.join(keywords)}[/bold yellow]. Saved to [bold yellow]{output_file}[/bold yellow].")

if __name__ == "__main__":
  search_in_privacy("output/physio_datenschutz.csv", "output/physio_datenschutz_result.csv", ["Analyse", "Tools"])