import pandas as pd
import requests

# Define a function to get lat/long coordinates
def convert_postcode_to_latlong(postal_code, api_key):
  base_url = "https://api.opencagedata.com/geocode/v1/json"
  params = {'q': str(postal_code) + ", DE", 'key': api_key}
  response = requests.get(base_url, params=params)

  if response.status_code == 200:
    data = response.json()
    lat = data['results'][0]['geometry']['lat']
    lng = data['results'][0]['geometry']['lng']

    return lat, lng
  else:
    return None

# Replace with your OpenCage API Key
key = 'cd298485ee44452f91ce0629dfedd708'

print(convert_postcode_to_latlong('92260', key))