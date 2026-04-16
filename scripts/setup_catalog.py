"""Register tables and metadata in Unity Catalog OSS"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()
UC_URL = os.getenv("UC_SERVER_URL", "http://localhost:8080")
CATALOG = os.getenv("UC_CATALOG", "kur_catalog")
SCHEMA = os.getenv("UC_SCHEMA", "public")


def setup():
    base = f"{UC_URL}/api/2.1/unity-catalog"

    # 1. Create catalog
    print(f"📦 Creating catalog: {CATALOG}")
    resp = requests.post(f"{base}/catalogs", json={
        "name": CATALOG,
        "comment": "Kur — Internal Text-to-SQL Catalog"
    })
    print(f"   {'✅ Created' if resp.ok else '⚠️ ' + resp.text[:100]}")

    # 2. Create schema
    print(f"📂 Creating schema: {CATALOG}.{SCHEMA}")
    resp = requests.post(f"{base}/schemas", json={
        "name": SCHEMA,
        "catalog_name": CATALOG,
        "comment": "Public business tables"
    })
    print(f"   {'✅ Created' if resp.ok else '⚠️ ' + resp.text[:100]}")

    # 3. Register tables with descriptions
    tables = [
        {
            "name": "regions",
            "comment": "Khu vực / vùng miền bán hàng (Hà Nội, HCM, Đà Nẵng, etc.)",
            "columns": [
                {"name": "id", "type_name": "INT", "comment": "Primary key"},
                {"name": "name", "type_name": "STRING", "comment": "Tên khu vực (synonym: vùng miền, area)"},
                {"name": "code", "type_name": "STRING", "comment": "Mã viết tắt (HN, HCM, DN)"},
            ]
        },
        {
            "name": "customers",
            "comment": "Khách hàng — tier: standard, premium, vip",
            "columns": [
                {"name": "id", "type_name": "INT", "comment": "Primary key"},
                {"name": "name", "type_name": "STRING", "comment": "Tên khách hàng"},
                {"name": "email", "type_name": "STRING", "comment": "Email"},
                {"name": "region_id", "type_name": "INT", "comment": "FK → regions.id"},
                {"name": "tier", "type_name": "STRING", "comment": "Hạng: standard, premium, vip"},
                {"name": "created_at", "type_name": "TIMESTAMP", "comment": "Ngày tạo tài khoản"},
            ]
        },
        {
            "name": "products",
            "comment": "Sản phẩm / dịch vụ — category: electronics, clothing, food, services",
            "columns": [
                {"name": "id", "type_name": "INT", "comment": "Primary key"},
                {"name": "name", "type_name": "STRING", "comment": "Tên sản phẩm"},
                {"name": "category", "type_name": "STRING", "comment": "Danh mục: electronics, clothing, food, services"},
                {"name": "unit_price", "type_name": "DECIMAL", "comment": "Đơn giá (VND)"},
                {"name": "is_active", "type_name": "BOOLEAN", "comment": "Còn bán không"},
            ]
        },
        {
            "name": "orders",
            "comment": "Đơn hàng — bảng fact chính. Doanh thu = amount - discount",
            "columns": [
                {"name": "id", "type_name": "INT", "comment": "Primary key"},
                {"name": "customer_id", "type_name": "INT", "comment": "FK → customers.id"},
                {"name": "product_id", "type_name": "INT", "comment": "FK → products.id"},
                {"name": "quantity", "type_name": "INT", "comment": "Số lượng"},
                {"name": "amount", "type_name": "DECIMAL", "comment": "Tổng tiền trước giảm giá (VND)"},
                {"name": "discount", "type_name": "DECIMAL", "comment": "Số tiền giảm giá (VND)"},
                {"name": "status", "type_name": "STRING", "comment": "Trạng thái: pending, completed, cancelled, refunded"},
                {"name": "created_at", "type_name": "TIMESTAMP", "comment": "Ngày đặt hàng"},
            ]
        },
    ]

    for tbl in tables:
        print(f"📊 Registering: {CATALOG}.{SCHEMA}.{tbl['name']}")
        # Enhance columns explicitly for UC OSS
        for idx, c in enumerate(tbl["columns"]):
            c["position"] = idx
            c["type_text"] = c["type_name"].lower()
            if c["type_name"] == "DOUBLE":
                c["type_text"] = "double"
            
            t_json = c["type_text"]
            if t_json == "int":
                t_json = "integer"
                
            c["type_json"] = f'{{"name":"{c["name"]}","type":"{t_json}","nullable":true,"metadata":{{}}}}'

        resp = requests.post(f"{base}/tables", json={
            "name": tbl["name"],
            "catalog_name": CATALOG,
            "schema_name": SCHEMA,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": f"/tmp/kur_uc_data/{tbl['name']}",
            "comment": tbl["comment"],
            "columns": tbl["columns"],
        })
        print(f"   {'✅ Registered' if resp.ok else '⚠️ ' + resp.text[:100]}")

    print("\n🎉 Catalog setup complete!")
    print(f"   UC UI: http://localhost:3000")
    print(f"   API: {UC_URL}/api/2.1/unity-catalog/tables?catalog_name={CATALOG}&schema_name={SCHEMA}")


if __name__ == "__main__":
    setup()
