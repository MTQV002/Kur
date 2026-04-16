-- Kur Sample Data — Vietnamese business scenario
-- Auto-loaded by PostgreSQL on first boot

-- Regions
CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(10) NOT NULL
);

INSERT INTO regions (name, code) VALUES
    ('Hà Nội', 'HN'),
    ('Hồ Chí Minh', 'HCM'),
    ('Đà Nẵng', 'DN'),
    ('Cần Thơ', 'CT'),
    ('Hải Phòng', 'HP');

COMMENT ON TABLE regions IS 'Khu vực / vùng miền bán hàng';
COMMENT ON COLUMN regions.name IS 'Tên khu vực';
COMMENT ON COLUMN regions.code IS 'Mã khu vực viết tắt';

-- Customers
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    email VARCHAR(200),
    region_id INTEGER REFERENCES regions(id),
    tier VARCHAR(20) DEFAULT 'standard',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE customers IS 'Khách hàng';
COMMENT ON COLUMN customers.name IS 'Tên khách hàng';
COMMENT ON COLUMN customers.tier IS 'Hạng khách: standard, premium, vip';
COMMENT ON COLUMN customers.region_id IS 'FK → regions.id — khu vực khách hàng';

-- Products
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    unit_price NUMERIC(12,2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

COMMENT ON TABLE products IS 'Sản phẩm / dịch vụ';
COMMENT ON COLUMN products.category IS 'Danh mục: electronics, clothing, food, services';
COMMENT ON COLUMN products.unit_price IS 'Đơn giá (VND)';

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL DEFAULT 1,
    amount NUMERIC(15,2) NOT NULL,
    discount NUMERIC(12,2) DEFAULT 0,
    status VARCHAR(20) DEFAULT 'completed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE orders IS 'Đơn hàng — bảng fact chính';
COMMENT ON COLUMN orders.amount IS 'Tổng tiền trước giảm giá (VND)';
COMMENT ON COLUMN orders.discount IS 'Số tiền giảm giá (VND)';
COMMENT ON COLUMN orders.status IS 'Trạng thái: pending, completed, cancelled, refunded';
COMMENT ON COLUMN orders.customer_id IS 'FK → customers.id';
COMMENT ON COLUMN orders.product_id IS 'FK → products.id';

-- Seed 20 products
INSERT INTO products (name, category, unit_price) VALUES
    ('iPhone 16 Pro', 'electronics', 32990000),
    ('MacBook Air M4', 'electronics', 27990000),
    ('Samsung Galaxy S25', 'electronics', 23990000),
    ('Áo thun nam basic', 'clothing', 199000),
    ('Quần jeans slim fit', 'clothing', 599000),
    ('Giày sneaker trắng', 'clothing', 1290000),
    ('Trà sữa trân châu', 'food', 35000),
    ('Phở bò tái chín', 'food', 55000),
    ('Cà phê sữa đá', 'food', 29000),
    ('Bánh mì thịt', 'food', 25000),
    ('Cloud hosting tháng', 'services', 2500000),
    ('SEO package', 'services', 5000000),
    ('Logo design', 'services', 3000000),
    ('iPad Air M3', 'electronics', 16990000),
    ('AirPods Pro 3', 'electronics', 6990000),
    ('Váy công sở', 'clothing', 890000),
    ('Túi xách da', 'clothing', 2500000),
    ('Combo trà chiều', 'food', 89000),
    ('Web development', 'services', 15000000),
    ('Mobile app MVP', 'services', 30000000);

-- Seed 50 customers
INSERT INTO customers (name, email, region_id, tier) VALUES
    ('Nguyễn Văn An', 'an.nguyen@email.com', 1, 'vip'),
    ('Trần Thị Bình', 'binh.tran@email.com', 2, 'premium'),
    ('Lê Hoàng Cường', 'cuong.le@email.com', 1, 'standard'),
    ('Phạm Thị Dung', 'dung.pham@email.com', 3, 'premium'),
    ('Hoàng Minh Em', 'em.hoang@email.com', 2, 'vip'),
    ('Vũ Thị Phương', 'phuong.vu@email.com', 4, 'standard'),
    ('Đặng Quốc Huy', 'huy.dang@email.com', 5, 'premium'),
    ('Bùi Thị Hoa', 'hoa.bui@email.com', 1, 'standard'),
    ('Ngô Thanh Hùng', 'hung.ngo@email.com', 2, 'vip'),
    ('Đỗ Thị Lan', 'lan.do@email.com', 3, 'standard'),
    ('Trịnh Văn Mạnh', 'manh.trinh@email.com', 1, 'premium'),
    ('Lý Thị Ngọc', 'ngoc.ly@email.com', 2, 'standard'),
    ('Mai Văn Phong', 'phong.mai@email.com', 4, 'vip'),
    ('Dương Thị Quỳnh', 'quynh.duong@email.com', 5, 'premium'),
    ('Hồ Văn Sơn', 'son.ho@email.com', 1, 'standard'),
    ('Tạ Thị Thu', 'thu.ta@email.com', 2, 'premium'),
    ('Lương Văn Tuấn', 'tuan.luong@email.com', 3, 'standard'),
    ('Phan Thị Uyên', 'uyen.phan@email.com', 1, 'vip'),
    ('Châu Văn Vinh', 'vinh.chau@email.com', 2, 'standard'),
    ('Đinh Thị Xuân', 'xuan.dinh@email.com', 4, 'premium');

-- Seed 200 orders (realistic distribution over last 6 months)
DO $$
DECLARE
    i INTEGER;
    cust_id INTEGER;
    prod_id INTEGER;
    qty INTEGER;
    amt NUMERIC;
    disc NUMERIC;
    stat VARCHAR;
    dt TIMESTAMP;
BEGIN
    FOR i IN 1..200 LOOP
        cust_id := (random() * 19 + 1)::int;
        prod_id := (random() * 19 + 1)::int;
        qty := (random() * 4 + 1)::int;
        
        SELECT unit_price * qty INTO amt FROM products WHERE id = prod_id;
        disc := CASE WHEN random() > 0.7 THEN amt * (random() * 0.2) ELSE 0 END;
        stat := CASE 
            WHEN random() > 0.9 THEN 'cancelled'
            WHEN random() > 0.85 THEN 'refunded'
            WHEN random() > 0.8 THEN 'pending'
            ELSE 'completed'
        END;
        dt := CURRENT_TIMESTAMP - (random() * 180)::int * INTERVAL '1 day' 
              + (random() * 23)::int * INTERVAL '1 hour';
        
        INSERT INTO orders (customer_id, product_id, quantity, amount, discount, status, created_at)
        VALUES (cust_id, prod_id, qty, amt, disc, stat, dt);
    END LOOP;
END $$;
