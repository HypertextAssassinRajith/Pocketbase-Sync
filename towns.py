import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

POCKETBASE_URL = os.getenv("POCKETBASE_URL")
COLLECTION_NAME = "Towns"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

def get_town_data():
    url = f"{POCKETBASE_URL}/api/collections/{COLLECTION_NAME}/records?page=1&perPage=1000"
    payload = {}
    try:
        response = requests.request("GET",url, data=payload)

        if response.status_code == 200:
            data = response.json()
            print("Retrieved:")
            # print(json.dumps(data, indent=2))
            items = data.get('items', [])
            sorted_items = sorted(items, key=lambda x: x.get('name', '').upper())
            print("Sorted towns by name:")
            for town in sorted_items:
                print(f"{town.get('name', '')} (id: {town.get('id', '')})")
            return data
        else:
            print(f"Error: {response.text}")

    except json.JSONDecodeError as e:
        print(f"JSON Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")

def get_districts_data():
    url = f"{POCKETBASE_URL}/api/collections/Districts/records?page=1&perPage=1000"
    payload = {}
    try:
        response = requests.request("GET",url, data=payload)

        if response.status_code == 200:
            data = response.json()
            print("Retrieved:")
            # print(json.dumps(data, indent=2))
            items = data.get('items', [])
            sorted_items = sorted(items, key=lambda x: x.get('name', '').upper())
            print("Sorted districts by name:")
            for district in sorted_items:
                print(f"{district.get('name', '')} (id: {district.get('id', '')})")
            return data
        else:
            print(f"Error: {response.text}")

    except json.JSONDecodeError as e:
        print(f"JSON Error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")

def get_district_id_by_name(districts, district_name):
    """Find the district record id by name (case-insensitive, partial match allowed)."""
    for d in districts:
        if district_name.lower() in d.get('name', '').lower():
            return d.get('id')
    return None

def get_district_from_gmaps(town_name):
    """Use Google Maps Geocoding API to get the district name for a town."""
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": town_name + ", Sri Lanka",
        "key": GOOGLE_MAPS_API_KEY
    }
    try:
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            data = resp.json()
            if data['status'] == 'OK':
                for comp in data['results'][0]['address_components']:
                    if 'administrative_area_level_2' in comp['types']:
                        return comp['long_name']
            else:
                print(f"Geocoding error for {town_name}: {data['status']}")
        else:
            print(f"HTTP error from Google Maps: {resp.status_code}")
    except Exception as e:
        print(f"Error calling Google Maps for {town_name}: {e}")
    return None

def update_town_district(town_id, district_id):
    """Update the town record in Pocketbase with the district id."""
    url = f"{POCKETBASE_URL}/api/collections/{COLLECTION_NAME}/records/{town_id}"
    payload = {"district": district_id}
    headers = {"Content-Type": "application/json"}
    try:
        resp = requests.patch(url, data=json.dumps(payload), headers=headers)
        if resp.status_code == 200:
            print(f"Updated town {town_id} with district {district_id}")
        else:
            print(f"Failed to update town {town_id}: {resp.text}")
    except Exception as e:
        print(f"Error updating town {town_id}: {e}")

def fix_town_districts():
    # Get all towns and districts
    towns_data = get_town_data()
    districts_data = get_districts_data()
    if not towns_data or not districts_data:
        print("Failed to fetch towns or districts.")
        return
    towns = towns_data.get('items', [])
    districts = districts_data.get('items', [])

    for town in towns:
        if not town.get('district'):
            town_name = town.get('name', '')
            print(f"Fixing district for town: {town_name}")
            district_name = get_district_from_gmaps(town_name)
            if district_name:
                district_id = get_district_id_by_name(districts, district_name)
                if district_id:
                    update_town_district(town['id'], district_id)
                else:
                    print(f"District '{district_name}' not found in Pocketbase for town '{town_name}'")
            else:
                print(f"Could not determine district for town '{town_name}'")

get_town_data()
get_districts_data()
fix_town_districts()