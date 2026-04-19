"""Initialize Apache Polaris Catalog (Iceberg REST)"""
import os
import time
import requests
import urllib.parse

POLARIS_URL = os.getenv("POLARIS_URL", "http://localhost:8181")
POLARIS_CREDENTIALS = os.getenv("POLARIS_CREDENTIALS", "polaris:polaris_secret")

def setup_polaris():
    print(f"Connecting to Polaris at {POLARIS_URL}...")
    
    # Minimal wait for Polaris to be healthy
    for _ in range(10):
        try:
            res = requests.get(f"{POLARIS_URL}/api/management/v1/health")
            if res.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(2)
        
    print("Polaris is reachable. Setting up Iceberg catalog...")
    
    # Retrieve OAuth token
    client_id, client_secret = POLARIS_CREDENTIALS.split(":")
    token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
    data = {
        "grant_type": "client_credentials", 
        "client_id": client_id, 
        "client_secret": client_secret,
        "scope": "PRINCIPAL_ROLE:ALL"
    }
    
    try:
        token_res = requests.post(token_url, data=data)
        token_res.raise_for_status()
        access_token = token_res.json().get("access_token")
    except Exception as e:
        print(f"Failed to get auth token: {e}")
        return
        
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Create Catalog mapping to local bucket or absolute path constraint for POC
    catalog_payload = {
        "catalog": {
            "name": "kur_polaris_catalog",
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {
                "default-base-location": "s3://kur-warehouse/"
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": ["s3://kur-warehouse/"]
            }
        }
    }
    
    cat_url = f"{POLARIS_URL}/api/management/v1/catalogs"
    print("Creating catalog 'kur_polaris_catalog'...")
    try:
        res = requests.post(cat_url, json=catalog_payload, headers=headers)
        if res.status_code in [200, 201]:
            print("Catalog created successfully.")
        elif res.status_code == 409:
            print("Catalog already exists.")
        else:
            print(f"Error creating catalog: {res.text}")
    except Exception as e:
        print(f"Error requesting catalog creation: {e}")

if __name__ == "__main__":
    setup_polaris()
