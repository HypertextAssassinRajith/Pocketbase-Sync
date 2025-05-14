import asyncio
import csv
from pocketbase import PocketBase
from dotenv import load_dotenv
import os


load_dotenv()

POCKETBASE_URL = os.getenv("POCKETBASE_URL")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

pb = PocketBase(POCKETBASE_URL)

def load_districts(filepath):
    district_map = {}
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            district_map[row['id']] = row['district'].strip()
    return district_map

def load_cities(filepath):
    cities = []
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cities.append({
                'id': row['id'],
                'city': row['city'].strip(),
                'district_id': row['district_ID']
            })
    return cities


async def update_towns_with_csv():

    district_csv = load_districts('districts.csv')
    cities_csv = load_cities('cities of SL.csv')

    pb_districts =  pb.collection('Districts').get_full_list()
    pb_district_name_to_id = {d.name.strip().lower(): d.id for d in pb_districts}

    pb_towns =  pb.collection('Towns').get_full_list()
    pb_town_name_to_id = {t.name.strip().lower(): t.id for t in pb_towns}

    updated = 0

    for city in cities_csv:
        city_name = city['city'].lower()
        district_name = district_csv.get(city['district_id'], '').lower()
        district_id = pb_district_name_to_id.get(district_name)
        town_id = pb_town_name_to_id.get(city_name)

        if district_id and town_id:
            pb.collection('Towns').update(town_id, {
                'district': district_id
            })
            updated += 1
            print(f"✅ Updated {city['city']} → {district_name}")
        else:
            print(f"⛔ Skipped: {city['city']} (district or town not found)")

    print(f"\n✅ Total Updated: {updated} towns.")

asyncio.run(update_towns_with_csv())
