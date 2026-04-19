import os
from typing import Optional

import requests

from app.core.config import load_settings


def ensure_polaris_catalog(settings: Optional[dict] = None, timeout_s: float = 4.0) -> tuple[bool, str]:
    s = settings or load_settings()
    polaris_url = (s.get("polaris_url") or "").rstrip("/")
    catalog_name = (s.get("polaris_catalog") or "").strip()
    credentials = (s.get("polaris_credentials") or "").strip()

    if not polaris_url or not catalog_name or not credentials or ":" not in credentials:
        return False, "Missing Polaris settings"

    client_id, client_secret = credentials.split(":", 1)
    try:
        health = requests.get(f"{polaris_url}/api/management/v1/health", timeout=timeout_s)
        if health.status_code != 200:
            return False, f"Polaris health={health.status_code}"

        token_res = requests.post(
            f"{polaris_url}/api/catalog/v1/oauth/tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=timeout_s,
        )
        token_res.raise_for_status()
        access_token = token_res.json().get("access_token", "")
        if not access_token:
            return False, "Polaris token missing"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "catalog": {
                "name": catalog_name,
                "type": "INTERNAL",
                "readOnly": False,
                "properties": {
                    "default-base-location": os.getenv("POLARIS_DEFAULT_BASE_LOCATION", "s3://kur-warehouse/")
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": [os.getenv("POLARIS_ALLOWED_LOCATION", "s3://kur-warehouse/")],
                },
            }
        }
        create_res = requests.post(
            f"{polaris_url}/api/management/v1/catalogs",
            json=payload,
            headers=headers,
            timeout=timeout_s,
        )
        if create_res.status_code in (200, 201, 409):
            return True, "Catalog ready"
        return False, f"Catalog create failed: {create_res.status_code}"
    except Exception as exc:
        return False, str(exc)[:180]
