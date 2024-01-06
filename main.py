import typer
import questionary
from serp_place_info_extractor import extract_place_information
from impressum import add_impressum_urls
from parsing import extract_contact_from_websites
from snov_io_email_finder import enrich_data_with_email_finder
import pandas as pd

app = typer.Typer()

@app.command("start")
def sample_func():
  keyword = questionary.text(
    'What are you looking for?',
  ).ask()
  
  option = questionary.select(
    'Please select an option below to continue',
    choices=['Basic Scraping', 'Postal Code Scraping']
  ).ask()
  
  if option == 'Basic Scraping':
    df = pd.read_csv('data/basic.csv', dtype={'postal_code': str})
    for row in df.itertuples():
      extract_place_information(keyword, f"@{row.latitude},{row.longitude},15z", 'output/googlemaps.csv', clear=False)
      
    df = pd.read_csv('output/googlemaps.csv')
    df.drop_duplicates(subset='website', keep='first', inplace=True)
    df.to_csv('output/googlemaps.csv', index=False)
    
    add_impressum_urls('output/googlemaps.csv', 'output/physio_impressum.csv')
    extract_contact_from_websites('output/physio_impressum.csv', 'output/physio_impressum_result.csv')
    enrich_data_with_email_finder('output/physio_impressum_result.csv', 'output/physio_enriched.csv')
  elif option == 'Postal Code Scraping':
    postal_code = questionary.text(
      'What is your postal code?',
    ).ask()
    
    df = pd.read_csv('data/all.csv', dtype={'postal_code': str})
    # Print the rows where postal_code starts with '01'
    df_filtered = df[df['postal_code'].astype(str).str.startswith(postal_code)]
    for row in df_filtered.itertuples():
      extract_place_information(keyword, f"@{row.latitude},{row.longitude},15z", 'output/googlemaps.csv', clear=False)
    
    df = pd.read_csv('output/googlemaps.csv')
    df.drop_duplicates(subset='website', keep='first', inplace=True)
    df.to_csv('output/googlemaps.csv', index=False)
    
    add_impressum_urls('output/googlemaps.csv', 'output/physio_impressum.csv')
    extract_contact_from_websites('output/physio_impressum.csv', 'output/physio_impressum_result.csv')
    enrich_data_with_email_finder('output/physio_impressum_result.csv', 'output/physio_enriched.csv')

if __name__ == "__main__":
  app()