import pandas as pd
import requests
import json
from pocketbase import PocketBase
from dotenv import load_dotenv
import os

file_path = "customers.xlsx"
address = []
unique_addresses = set()
COLLECTION_NAME = "Towns"
POCKETBASE_URL = os.getenv("POCKETBASE_URL")

data_frame = pd.read_excel(file_path)

data_frame = data_frame.where(pd.notna(data_frame), None)

data = data_frame[data_frame['CUSTOMER_NAME'] != 'None'].to_dict(orient="records")

for record in data:
    customer_address = record.get("CUSTOMER_ADDRESS")
    if customer_address and customer_address not in unique_addresses:
        address.append(customer_address)
        unique_addresses.add(customer_address)



def set_address():
    for address in unique_addresses:
        data = {
            "name": address,
            "district": None,
        }

        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(f"{POCKETBASE_URL}/api/collections/{COLLECTION_NAME}/records",json=data,headers=headers)
            
            if response.status_code == 200:
                print(f"Uploaded: {data}")
            else:
                print(f"Error: {response.text}")

        except json.JSONDecodeError as e:
            print(f"JSON Error: {e}")

# print(address)
with open("address.txt", "a") as f:
  f.write(str(unique_addresses))


set_address()

