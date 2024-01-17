import pandas as pd
import requests
import tempfile
import time

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
  
def get_geolocation(postcode, country):
  base_url = "https://nominatim.openstreetmap.org/search"
  parameters = {
    "postalcode": postcode,
    "country": country,
    "format": "json"
  }
  response = requests.get(base_url, params=parameters)

  if response.status_code == 200:
    json_response = response.json()
    if json_response:
      return json_response[0]['lat'], json_response[0]['lon']
    else:
      return "No data found for this postcode"
  else:
    return "API request failed"

# Replace with your OpenCage API Key
key = 'cd298485ee44452f91ce0629dfedd708'

df = pd.read_csv('data/basic-new.csv', dtype={'PLZ': str}, delimiter=';')

i = 0

for row in df.itertuples():
  lat, lng = get_geolocation(row.PLZ, "Germany")
  print(row.Index, lat, lng)
  df.loc[row.Index, 'latitude'] = lat
  df.loc[row.Index, 'longitude'] = lng
  
  i += 1
  if (i % 30 == 0):
    df.to_csv('data/basic-new (1).csv', index=False)
    i = 0
  
  time.sleep(1)

df.to_csv('data/basic-new (1).csv', index=False)