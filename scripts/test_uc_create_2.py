import requests

base = "http://localhost:8080/api/2.1/unity-catalog"
payload = {
    "name": "test_regions2",
    "catalog_name": "kur_catalog",
    "schema_name": "public",
    "table_type": "EXTERNAL",
    "data_source_format": "DELTA",
    "storage_location": "/tmp/test_regions2",
    "columns": [
        {
            "name": "id",
            "type_name": "INT",
            "type_text": "int",
            "position": 0,
            "comment": "Mã vùng"
        }
    ]
}

resp = requests.post(f"{base}/tables", json=payload)
print(resp.status_code)
print(resp.text)
