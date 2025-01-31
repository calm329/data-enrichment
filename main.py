import typer
import questionary
from serp_place_info_extractor import extract_place_information
from impressum import add_impressum_urls
from parsing_impressum import extract_contact_from_websites
from datenschutz import add_datenschutz_urls
from search_keyword_in_privacy import search_in_privacy
from snov_io_email_finder import enrich_data_with_email_finder
import pandas as pd
import os

app = typer.Typer()

@app.command("start")
def start():
  keyword = questionary.text(
    'What are you looking for?',
  ).ask()

  prefix = keyword.lower().replace(' ', '_')

  search = questionary.confirm('Do you want to search for specific Keywords on the privacy pagae?').ask() 

  if search:
    privacy_keywords = questionary.text(
      'What Keywords are you Looking for? (Comma seperated)',
    ).ask()

    privacy_keywords = privacy_keywords.split(',')
    privacy_keywords = [k.strip() for k in privacy_keywords]
  
  option = questionary.select(
    'Please select an option below to continue',
    choices=['Basic Scraping', 'Postal Code Scraping']
  ).ask()
  
  if not os.path.exists('output'):  # check if directory already exists
    os.makedirs('output')
  
  if option == 'Basic Scraping':
    df = pd.read_csv('data/basic.csv', dtype={'postal_code': str})
    for row in df.itertuples():
      extract_place_information(keyword, f"@{row.latitude},{row.longitude},15z", f'output/{prefix}_googlemaps.csv', clear=False)
      
    df = pd.read_csv(f'output/{prefix}_googlemaps.csv')
    df.drop_duplicates(subset='domain', keep='first', inplace=True)
    df.to_csv(f'output/{prefix}_googlemaps.csv', index=False)

    if search:
      add_datenschutz_urls(f'output/{prefix}_googlemaps.csv', f'output/{prefix}_datenschutz.csv')
      search_in_privacy(f'output/{prefix}_datenschutz.csv', f'output/{prefix}_datenschutz_result.csv', privacy_keywords)
      add_impressum_urls(f'output/{prefix}_datenschutz_result.csv', f'output/{prefix}_impressum.csv')
    else:
      add_impressum_urls(f'output/{prefix}_googlemaps.csv', f'output/{prefix}_impressum.csv')
    extract_contact_from_websites(f'output/{prefix}_impressum.csv', f'output/{prefix}_impressum_result.csv')
    enrich_data_with_email_finder(f'output/{prefix}_impressum_result.csv', f'output/{prefix}_enriched.csv')
  elif option == 'Postal Code Scraping':
    postal_code = questionary.text(
      'What is your postal code?',
    ).ask()
    
    df = pd.read_csv('data/all.csv', dtype={'postal_code': str})
    # Print the rows where postal_code starts with '01'
    df_filtered = df[df['postal_code'].astype(str).str.startswith(postal_code)]
    for row in df_filtered.itertuples():
      extract_place_information(keyword, f"@{row.latitude},{row.longitude},15z", f'output/{prefix}_googlemaps.csv', clear=False)
    
    df = pd.read_csv(f'output/{prefix}_googlemaps.csv')
    df.drop_duplicates(subset='domain', keep='first', inplace=True)
    df.to_csv(f'output/{prefix}_googlemaps.csv', index=False)
    
    if search:
      add_datenschutz_urls(f'output/{prefix}_googlemaps.csv', f'output/{prefix}_datenschutz.csv')
      search_in_privacy(f'output/{prefix}_datenschutz.csv', f'output/{prefix}_datenschutz_result.csv', privacy_keywords)
      add_impressum_urls(f'output/{prefix}_datenschutz_result.csv', f'output/{prefix}_impressum.csv')
    else:
      add_impressum_urls(f'output/{prefix}_googlemaps.csv', f'output/{prefix}_impressum.csv')
    extract_contact_from_websites(f'output/{prefix}_impressum.csv', f'output/{prefix}_impressum_result.csv')
    enrich_data_with_email_finder(f'output/{prefix}_impressum_result.csv', f'output/{prefix}_enriched.csv')

if __name__ == "__main__":
  app()