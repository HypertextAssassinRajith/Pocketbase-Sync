import pandas as pd
import requests
import json


POCKETBASE_URL = "http://13.251.18.154:8090"
COLLECTION_NAME = "customers"

FILE_PATH = "customers.xlsx" 
df = pd.read_excel(FILE_PATH)

df = df.where(pd.notna(df), None)

records = df[df['CUSTOMER_NAME'] != 'None'].to_dict(orient="records")

def upload_to_pocketbase(records):
    import requests
    import json

    headers = {"Content-Type": "application/json"}

    for record in records:
        customer_name = record.get("CUSTOMER_NAME")
        if customer_name and customer_name != 'None':
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