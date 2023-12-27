import typer
import questionary
from serp_place_info_extractor import extract_place_information
from impressum import add_impressum_urls
from parsing import extract_contact_from_websites
from snov_io_email_finder import enrich_data_with_email_finder

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
    extract_place_information(keyword, 'Dresden', 'output/googlemaps.csv')
    add_impressum_urls('output/googlemaps.csv', 'output/physio_impressum.csv')
    extract_contact_from_websites('output/physio_impressum.csv', 'output/physio_impressum_result.csv')
    enrich_data_with_email_finder('output/physio_impressum_result.csv', 'output/physio_enriched.csv')
  elif option == 'Postal Code Scraping':
    postal_code = questionary.text(
      'What is your postal code?',
    ).ask()

if __name__ == "__main__":
  app()