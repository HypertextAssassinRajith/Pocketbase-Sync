
import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv


load_dotenv()

POCKETBASE_URL = os.getenv("POCKETBASE_URL")
COLLECTION_NAME = "customergps"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
FILE_PATH = "customers.xlsx"


df = pd.read_excel(FILE_PATH)
df = df.where(pd.notna(df), None)
records = df[df['CUSTOMER_NAME'] != 'None'].to_dict(orient="records")

def get_coordinates(address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        'address': address,
        'key': GOOGLE_MAPS_API_KEY
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'OK':
                location = data['results'][0]['geometry']['location']
                return location['lat'], location['lng']
            else:
                print(f"Geocoding Error: {data['status']}")
                return None, None
        else:
            print(f"HTTP Error: {response.status_code}")
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Request Exception: {e}")
        return None, None

def upload_to_pocketbase(records):
    headers = {"Content-Type": "application/json"}

    for record in records:
        customer_name = record.get("CUSTOMER_NAME")
        full_address = record.get("CUSTOMER_FULL_ADDRESS")
        code = record.get("CUSTOMER_CODE")

        if customer_name and customer_name != 'None':
            if full_address:
                latitude, longitude = get_coordinates(full_address)
                record['latitude'] = latitude
                record['longitude'] = longitude

            if code:
                record['id'] = str(code)

            try:
                response = requests.post(
                    f"{POCKETBASE_URL}/api/collections/{COLLECTION_NAME}/records",
                    json=record,
                    headers=headers
                )

                if response.status_code == 200:
                    print(f"Uploaded: {record}")
                else:
                    print(f"Error: {response.text}")

            except json.JSONDecodeError as e:
                print(f"JSON Error: {e}")
            except requests.exceptions.RequestException as e:
                print(f"Request Error: {e}")

upload_to_pocketbase(records)
