import requests
import json

base = "http://localhost:8080/api/2.1/unity-catalog"
CATALOG = "kur_catalog"
SCHEMA = "public"

payload = {
    "name": "test_regions",
    "catalog_name": CATALOG,
    "schema_name": SCHEMA,
    "table_type": "EXTERNAL",
    "data_source_format": "DELTA",
    "storage_location": "/tmp/test_regions",
    "columns": [
        {
            "name": "id",
            "type_name": "INT",
            "type_text": "int",
            "type_json": '{"name":"id","type":"integer","nullable":true,"metadata":{}}',
            "comment": "Mã vùng"
        }
    ]
}

resp = requests.post(f"{base}/tables", json=payload)
print(resp.status_code)
print(resp.text)
