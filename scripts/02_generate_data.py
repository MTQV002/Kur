"""
Kur V2 — Generate 15-Table Star Schema Data (Optimized)
Uses pandas DataFrames + DuckDB bulk insert for speed.

Usage:
    pip install duckdb numpy pandas
    python scripts/02_generate_data.py
"""
import os
import random
import time
import numpy as np
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# ──── Config ────
DB_PATH = os.getenv("DUCKDB_PATH", "data/kur.db")
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

END_DATE = datetime(2026, 4, 14)
START_DATE = END_DATE - timedelta(days=730)

# ──── Vietnamese data ────
TINH_THANH = [
    "Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu",
    "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
    "Bình Thuận", "Cà Mau", "Cao Bằng", "Đắk Lắk", "Đắk Nông",
    "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang",
    "Hà Nam", "Hà Tĩnh", "Hải Dương", "Hậu Giang", "Hòa Bình",
    "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu",
    "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định",
    "Nghệ An", "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên",
    "Quảng Bình", "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị",
    "Sóc Trăng", "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên",
    "Thanh Hóa", "Thừa Thiên Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang",
    "Vĩnh Long", "Vĩnh Phúc", "Yên Bái",
]

CATEGORIES = [
    "Điện tử", "Thời trang", "Thực phẩm", "Gia dụng", "Sách",
    "Mỹ phẩm", "Thể thao", "Đồ chơi", "SaaS", "Dịch vụ",
]

HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
      "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý", "Trịnh", "Đinh", "Mai", "Tạ"]
TEN_DEM = ["Văn", "Thị", "Hoàng", "Minh", "Quốc", "Thanh", "Ngọc", "Đức", "Xuân", "Thu"]
TEN = ["An", "Bình", "Cường", "Dung", "Em", "Phương", "Huy", "Hoa", "Hùng", "Lan",
       "Mạnh", "Ngọc", "Phong", "Quỳnh", "Sơn", "Thu", "Tuấn", "Uyên", "Vinh", "Xuân"]

PAYMENT_METHODS = [
    (1, "COD", "Thanh toán khi nhận hàng"),
    (2, "VNPay", "Ví điện tử VNPay"),
    (3, "Momo", "Ví điện tử Momo"),
    (4, "ZaloPay", "Ví điện tử ZaloPay"),
    (5, "Bank Transfer", "Chuyển khoản ngân hàng"),
    (6, "Credit Card", "Thẻ tín dụng Visa/Mastercard"),
    (7, "Installment", "Trả góp 0%"),
    (8, "ShopeePay", "Ví ShopeePay"),
]

ORDER_STATUSES = np.array(["completed"] * 70 + ["pending"] * 10 + ["cancelled"] * 10 + ["refunded"] * 10)
TICKET_TYPES = ["Đổi trả", "Khiếu nại", "Hỏi thông tin", "Hỗ trợ kỹ thuật", "Thanh toán", "Vận chuyển"]
TICKET_STATUS = ["open", "in_progress", "resolved", "closed"]
PRIORITY = ["low", "medium", "high", "critical"]
CAMPAIGN_CHANNELS = ["Facebook Ads", "Google Ads", "TikTok Ads", "Email", "SMS", "Zalo OA", "KOL", "SEO"]


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"   ⏱️ {elapsed:.1f}s")
        return result
    return wrapper


def create_schema(db):
    """Create all 15 tables"""
    print("🏗️  Creating DuckDB schema...")
    db.execute("""
    CREATE OR REPLACE TABLE dim_regions (
        id INTEGER PRIMARY KEY, name VARCHAR, vung VARCHAR, population INTEGER
    );
    CREATE OR REPLACE TABLE dim_customers (
        id INTEGER PRIMARY KEY, name VARCHAR, email VARCHAR, phone VARCHAR,
        region_id INTEGER, tier VARCHAR, created_at TIMESTAMP
    );
    CREATE OR REPLACE TABLE dim_products (
        id INTEGER PRIMARY KEY, name VARCHAR, sku VARCHAR, category VARCHAR,
        unit_price BIGINT, cost_price BIGINT, is_active BOOLEAN, avg_rating DOUBLE
    );
    CREATE OR REPLACE TABLE dim_employees (
        id INTEGER PRIMARY KEY, name VARCHAR, department VARCHAR, position VARCHAR,
        region_id INTEGER, hire_date DATE, is_active BOOLEAN
    );
    CREATE OR REPLACE TABLE dim_stores (
        id INTEGER PRIMARY KEY, name VARCHAR, region_id INTEGER, store_type VARCHAR,
        capacity INTEGER, open_date DATE, is_active BOOLEAN
    );
    CREATE OR REPLACE TABLE dim_suppliers (
        id INTEGER PRIMARY KEY, name VARCHAR, country VARCHAR, contact_email VARCHAR,
        lead_time_days INTEGER, quality_score DOUBLE
    );
    CREATE OR REPLACE TABLE dim_campaigns (
        id INTEGER PRIMARY KEY, name VARCHAR, channel VARCHAR,
        start_date DATE, end_date DATE, budget BIGINT, status VARCHAR
    );
    CREATE OR REPLACE TABLE dim_payment_methods (
        id INTEGER PRIMARY KEY, name VARCHAR, description VARCHAR
    );
    CREATE OR REPLACE TABLE fact_orders (
        id INTEGER PRIMARY KEY, customer_id INTEGER, store_id INTEGER,
        employee_id INTEGER, payment_method_id INTEGER, status VARCHAR,
        amount BIGINT, discount BIGINT, shipping_fee BIGINT, created_at TIMESTAMP
    );
    CREATE OR REPLACE TABLE fact_order_items (
        id INTEGER, order_id INTEGER, product_id INTEGER,
        quantity INTEGER, unit_price BIGINT, line_total BIGINT
    );
    CREATE OR REPLACE TABLE fact_payments (
        order_id INTEGER, payment_method_id INTEGER, amount BIGINT,
        status VARCHAR, paid_at TIMESTAMP
    );
    CREATE OR REPLACE TABLE fact_inventory (
        product_id INTEGER, store_id INTEGER, snapshot_date DATE,
        quantity_on_hand INTEGER, quantity_received INTEGER, quantity_sold INTEGER
    );
    CREATE OR REPLACE TABLE fact_support_tickets (
        id INTEGER PRIMARY KEY, customer_id INTEGER, ticket_type VARCHAR,
        priority VARCHAR, status VARCHAR, assigned_employee_id INTEGER,
        created_at TIMESTAMP, resolved_at TIMESTAMP
    );
    CREATE OR REPLACE TABLE fact_marketing_spend (
        campaign_id INTEGER, spend_date DATE, spend_amount BIGINT,
        impressions INTEGER, clicks INTEGER, conversions INTEGER
    );
    CREATE OR REPLACE TABLE fact_web_sessions (
        id INTEGER, customer_id INTEGER, source VARCHAR, device VARCHAR,
        pages_viewed INTEGER, duration_seconds INTEGER, converted BOOLEAN,
        session_start TIMESTAMP
    );
    """)
    print("   ✅ Schema created (15 tables)")


@timer
def gen_dim_regions(db):
    print("📍 dim_regions (63)...")
    df = pd.DataFrame({
        "id": range(1, 64),
        "name": TINH_THANH,
        "vung": ["Bắc"] * 21 + ["Trung"] * 20 + ["Nam"] * 22,
        "population": np.random.randint(200_000, 10_000_000, 63),
    })
    db.execute("INSERT INTO dim_regions SELECT * FROM df")


@timer
def gen_dim_customers(db, n=500_000):
    print(f"👤 dim_customers ({n:,})...")
    ho = np.random.choice(HO, n)
    dem = np.random.choice(TEN_DEM, n)
    ten = np.random.choice(TEN, n)
    names = np.char.add(np.char.add(np.char.add(ho, " "), np.char.add(dem, " ")), ten)
    ids = np.arange(1, n + 1)

    df = pd.DataFrame({
        "id": ids,
        "name": names,
        "email": [f"u{i}@email.com" for i in ids],
        "phone": [f"09{random.randint(10000000, 99999999)}" for _ in range(n)],
        "region_id": np.random.randint(1, 64, n),
        "tier": np.random.choice(["standard", "premium", "vip", "enterprise"], n, p=[0.6, 0.25, 0.1, 0.05]),
        "created_at": pd.to_datetime(
            START_DATE.timestamp() + np.random.randint(0, 730 * 86400, n), unit='s'
        ),
    })
    db.execute("INSERT INTO dim_customers SELECT * FROM df")


@timer
def gen_dim_products(db, n=5000):
    print(f"📦 dim_products ({n:,})...")
    cats = np.random.choice(CATEGORIES, n)
    prices = np.where(
        np.isin(cats, ["Điện tử"]), np.random.randint(500_000, 50_000_000, n),
        np.where(
            np.isin(cats, ["SaaS", "Dịch vụ"]), np.random.randint(1_000_000, 100_000_000, n),
            np.where(
                np.isin(cats, ["Thực phẩm"]), np.random.randint(10_000, 500_000, n),
                np.random.randint(50_000, 5_000_000, n)
            )
        )
    )
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "name": [f"SP-{c[:2]}-{i:05d}" for i, c in enumerate(cats, 1)],
        "sku": [f"SKU-{i:05d}" for i in range(1, n + 1)],
        "category": cats,
        "unit_price": prices,
        "cost_price": (prices * np.random.uniform(0.3, 0.7, n)).astype(int),
        "is_active": np.random.random(n) > 0.05,
        "avg_rating": np.round(np.random.uniform(1, 5, n), 1),
    })
    db.execute("INSERT INTO dim_products SELECT * FROM df")


@timer
def gen_dim_employees(db, n=200):
    print(f"👔 dim_employees ({n})...")
    depts = np.random.choice(["Sales", "Marketing", "Support", "Engineering", "Operations"], n, p=[0.4, 0.15, 0.2, 0.15, 0.1])
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "name": [f"{random.choice(HO)} {random.choice(TEN_DEM)} {random.choice(TEN)}" for _ in range(n)],
        "department": depts,
        "position": ["Rep" if d == "Sales" else "Spec" for d in depts],
        "region_id": np.random.randint(1, 64, n),
        "hire_date": pd.to_datetime(
            datetime(2020, 1, 1).timestamp() + np.random.randint(0, 2000 * 86400, n), unit='s'
        ).date,
        "is_active": np.random.random(n) > 0.1,
    })
    db.execute("INSERT INTO dim_employees SELECT * FROM df")


@timer
def gen_dim_stores(db, n=50):
    print(f"🏪 dim_stores ({n})...")
    rids = np.random.randint(1, 64, n)
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "name": [f"Store {TINH_THANH[r - 1]} #{i}" for i, r in enumerate(rids, 1)],
        "region_id": rids,
        "store_type": np.random.choice(["Flagship", "Standard", "Mini", "Online", "Warehouse"], n),
        "capacity": np.random.randint(50, 500, n),
        "open_date": pd.to_datetime(
            datetime(2019, 1, 1).timestamp() + np.random.randint(0, 2500 * 86400, n), unit='s'
        ).date,
        "is_active": np.random.random(n) > 0.05,
    })
    db.execute("INSERT INTO dim_stores SELECT * FROM df")


@timer
def gen_dim_suppliers(db, n=100):
    print(f"🚛 dim_suppliers ({n})...")
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "name": [f"NCC-{i:03d} {random.choice(HO)} Corp" for i in range(1, n + 1)],
        "country": np.random.choice(["Việt Nam", "Trung Quốc", "Hàn Quốc", "Nhật Bản", "Khác"], n, p=[0.5, 0.2, 0.1, 0.1, 0.1]),
        "contact_email": [f"ncc{i}@supplier.com" for i in range(1, n + 1)],
        "lead_time_days": np.random.randint(7, 45, n),
        "quality_score": np.round(np.random.uniform(3, 5, n), 1),
    })
    db.execute("INSERT INTO dim_suppliers SELECT * FROM df")


@timer
def gen_dim_campaigns(db, n=300):
    print(f"📢 dim_campaigns ({n})...")
    starts = pd.to_datetime(START_DATE.timestamp() + np.random.randint(0, 700 * 86400, n), unit='s')
    durations = np.random.randint(7, 90, n)
    df = pd.DataFrame({
        "id": range(1, n + 1),
        "name": [f"Camp #{i} {random.choice(CATEGORIES)}" for i in range(1, n + 1)],
        "channel": np.random.choice(CAMPAIGN_CHANNELS, n),
        "start_date": starts.date,
        "end_date": (starts + pd.to_timedelta(durations, unit='D')).date,
        "budget": np.random.randint(5_000_000, 500_000_000, n),
        "status": np.random.choice(["active", "completed", "paused", "draft"], n),
    })
    db.execute("INSERT INTO dim_campaigns SELECT * FROM df")


@timer
def gen_dim_payment_methods(db):
    print("💳 dim_payment_methods (8)...")
    df = pd.DataFrame(PAYMENT_METHODS, columns=["id", "name", "description"])
    db.execute("INSERT INTO dim_payment_methods SELECT * FROM df")


@timer
def gen_fact_orders(db, n=1_000_000):
    """Generate in 250K batches using vectorized numpy"""
    print(f"🛒 fact_orders ({n:,})...")
    batch_size = 250_000

    for batch_start in range(0, n, batch_size):
        batch_n = min(batch_size, n - batch_start)
        ids = np.arange(batch_start + 1, batch_start + batch_n + 1)

        # Exponential distribution → more recent orders
        days_ago = np.clip(np.random.exponential(180, batch_n), 0, 730).astype(int)
        timestamps = pd.to_datetime(
            END_DATE.timestamp() - days_ago * 86400 + np.random.randint(0, 86400, batch_n),
            unit='s'
        )

        amounts = np.random.randint(50_000, 50_000_000, batch_n)
        discount_mask = np.random.random(batch_n) > 0.6
        discounts = (amounts * np.random.uniform(0, 0.3, batch_n) * discount_mask).astype(int)

        df = pd.DataFrame({
            "id": ids,
            "customer_id": np.random.randint(1, 500_001, batch_n),
            "store_id": np.random.randint(1, 51, batch_n),
            "employee_id": np.random.randint(1, 201, batch_n),
            "payment_method_id": np.random.randint(1, 9, batch_n),
            "status": np.random.choice(ORDER_STATUSES, batch_n),
            "amount": amounts,
            "discount": discounts,
            "shipping_fee": np.random.choice([0, 15000, 25000, 30000, 50000], batch_n, p=[0.3, 0.25, 0.2, 0.15, 0.1]),
            "created_at": timestamps,
        })
        db.execute("INSERT INTO fact_orders SELECT * FROM df")
        print(f"   ...{batch_start + batch_n:,}")


@timer
def gen_fact_order_items(db, n_orders=1_000_000):
    """~2.5 items per order average → ~2.5M rows"""
    print(f"📋 fact_order_items (~{n_orders * 2.5:,.0f})...")
    batch_size = 250_000
    item_id = 1

    for batch_start in range(0, n_orders, batch_size):
        batch_n = min(batch_size, n_orders - batch_start)
        order_ids = np.arange(batch_start + 1, batch_start + batch_n + 1)

        # Random items per order (1-5)
        items_per = np.random.choice([1, 2, 3, 4, 5], batch_n, p=[0.25, 0.35, 0.25, 0.10, 0.05])
        total_items = items_per.sum()

        oid_repeated = np.repeat(order_ids, items_per)
        iids = np.arange(item_id, item_id + total_items)
        item_id += total_items

        pids = np.random.randint(1, 5001, total_items)
        qtys = np.random.randint(1, 10, total_items)
        uprices = np.random.randint(10_000, 5_000_000, total_items)

        df = pd.DataFrame({
            "id": iids,
            "order_id": oid_repeated,
            "product_id": pids,
            "quantity": qtys,
            "unit_price": uprices,
            "line_total": uprices * qtys,
        })
        db.execute("INSERT INTO fact_order_items SELECT * FROM df")
        print(f"   ...{batch_start + batch_n:,} orders → {item_id - 1:,} items")


@timer
def gen_fact_payments(db, n_orders=1_000_000):
    print(f"💰 fact_payments ({n_orders:,})...")
    batch_size = 250_000
    statuses = np.array(["success"] * 80 + ["failed"] * 10 + ["pending"] * 10)

    for batch_start in range(0, n_orders, batch_size):
        batch_n = min(batch_size, n_orders - batch_start)
        order_ids = np.arange(batch_start + 1, batch_start + batch_n + 1)

        days_ago = np.clip(np.random.exponential(180, batch_n), 0, 730).astype(int)
        timestamps = pd.to_datetime(END_DATE.timestamp() - days_ago * 86400, unit='s')

        df = pd.DataFrame({
            "order_id": order_ids,
            "payment_method_id": np.random.randint(1, 9, batch_n),
            "amount": np.random.randint(50_000, 50_000_000, batch_n),
            "status": np.random.choice(statuses, batch_n),
            "paid_at": timestamps,
        })
        db.execute("INSERT INTO fact_payments SELECT * FROM df")
        print(f"   ...{batch_start + batch_n:,}")


@timer
def gen_fact_inventory(db, n_products=5000, n_stores=50, n_days=30):
    """Daily snapshot, sampled stores × products"""
    total = n_days * 10 * 100  # 10 stores * 100 products * 30 days
    print(f"📦 fact_inventory (~{total:,})...")
    rows = []
    for d in range(n_days):
        dt = (END_DATE - timedelta(days=n_days - d)).date()
        stores = np.random.choice(range(1, n_stores + 1), 10, replace=False)
        for sid in stores:
            pids = np.random.choice(range(1, n_products + 1), 100, replace=False)
            for pid in pids:
                rows.append((int(pid), int(sid), dt, random.randint(0, 500),
                             random.randint(0, 200), random.randint(0, 150)))

    df = pd.DataFrame(rows, columns=["product_id", "store_id", "snapshot_date",
                                       "quantity_on_hand", "quantity_received", "quantity_sold"])
    db.execute("INSERT INTO fact_inventory SELECT * FROM df")


@timer
def gen_fact_support_tickets(db, n=100_000):
    print(f"🎫 fact_support_tickets ({n:,})...")
    days_ago = np.random.randint(0, 730, n)
    created = pd.to_datetime(END_DATE.timestamp() - days_ago * 86400, unit='s')
    resolve_mask = np.random.random(n) > 0.2
    resolved = created + pd.to_timedelta(np.random.randint(1, 168, n) * resolve_mask, unit='h')
    resolved = resolved.where(resolve_mask, pd.NaT)

    df = pd.DataFrame({
        "id": range(1, n + 1),
        "customer_id": np.random.randint(1, 500_001, n),
        "ticket_type": np.random.choice(TICKET_TYPES, n),
        "priority": np.random.choice(PRIORITY, n, p=[0.3, 0.4, 0.2, 0.1]),
        "status": np.random.choice(TICKET_STATUS, n),
        "assigned_employee_id": np.random.randint(1, 201, n),
        "created_at": created,
        "resolved_at": resolved,
    })
    db.execute("INSERT INTO fact_support_tickets SELECT * FROM df")


@timer
def gen_fact_marketing_spend(db, n_campaigns=300, n_days=365):
    print(f"💸 fact_marketing_spend...")
    rows = []
    for d in range(n_days):
        dt = (END_DATE - timedelta(days=n_days - d)).date()
        active = random.sample(range(1, n_campaigns + 1), random.randint(5, 30))
        for cid in active:
            rows.append((cid, dt, random.randint(100_000, 20_000_000),
                         random.randint(100, 50_000), random.randint(5, 2000), random.randint(0, 500)))
    df = pd.DataFrame(rows, columns=["campaign_id", "spend_date", "spend_amount", "impressions", "clicks", "conversions"])
    db.execute("INSERT INTO fact_marketing_spend SELECT * FROM df")
    print(f"   {len(rows):,} rows")


@timer
def gen_fact_web_sessions(db, n=1_000_000):
    print(f"🌐 fact_web_sessions ({n:,})...")
    batch_size = 250_000
    sources = ["organic", "paid", "direct", "social", "email", "referral"]
    devices = ["mobile", "desktop", "tablet"]

    for batch_start in range(0, n, batch_size):
        batch_n = min(batch_size, n - batch_start)

        cust_ids = np.random.randint(1, 500_001, batch_n)
        # 40% anonymous
        anon_mask = np.random.random(batch_n) < 0.4
        cust_ids_nullable = pd.array(cust_ids, dtype=pd.Int64Dtype())
        cust_ids_nullable[anon_mask] = pd.NA

        days_ago = np.random.randint(0, 365, batch_n)
        timestamps = pd.to_datetime(
            END_DATE.timestamp() - days_ago * 86400 + np.random.randint(0, 86400, batch_n),
            unit='s'
        )

        df = pd.DataFrame({
            "id": np.arange(batch_start + 1, batch_start + batch_n + 1),
            "customer_id": cust_ids_nullable,
            "source": np.random.choice(sources, batch_n),
            "device": np.random.choice(devices, batch_n, p=[0.55, 0.35, 0.10]),
            "pages_viewed": np.random.randint(1, 30, batch_n),
            "duration_seconds": np.random.randint(10, 1800, batch_n),
            "converted": np.random.random(batch_n) > 0.95,
            "session_start": timestamps,
        })
        db.execute("INSERT INTO fact_web_sessions SELECT * FROM df")
        print(f"   ...{batch_start + batch_n:,}")


def print_stats(db):
    print("\n" + "=" * 60)
    print("📊 KUR DATABASE STATISTICS")
    print("=" * 60)
    tables = db.execute("SHOW TABLES").fetchall()
    total = 0
    for (name,) in tables:
        count = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        total += count
        print(f"   {name:<30} {count:>12,} rows")
    db_size = os.path.getsize(DB_PATH)
    print(f"\n   {'TOTAL':<30} {total:>12,} rows")
    print(f"   {'Database size':<30} {db_size / 1024 / 1024:>10.1f} MB")
    print("=" * 60)


if __name__ == "__main__":
    print("🔮 Kur V2 — Data Generation (Optimized)")
    print(f"   Database: {DB_PATH}\n")

    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = duckdb.connect(DB_PATH)

    t0 = time.time()
    create_schema(db)

    # Dims (~5s total)
    gen_dim_regions(db)
    gen_dim_customers(db, 500_000)
    gen_dim_products(db, 5_000)
    gen_dim_employees(db, 200)
    gen_dim_stores(db, 50)
    gen_dim_suppliers(db, 100)
    gen_dim_campaigns(db, 300)
    gen_dim_payment_methods(db)

    # Facts (~30-60s total)
    gen_fact_orders(db, 1_000_000)          # Scale to 10_000_000 for 2.5GB
    gen_fact_order_items(db, 1_000_000)
    gen_fact_payments(db, 1_000_000)
    gen_fact_inventory(db)
    gen_fact_support_tickets(db, 100_000)
    gen_fact_marketing_spend(db)
    gen_fact_web_sessions(db, 1_000_000)

    print_stats(db)
    db.close()

    elapsed = time.time() - t0
    print(f"\n🎉 Done in {elapsed:.0f}s! Database: {DB_PATH}")
