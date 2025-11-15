"""
generate_ecommerce_db.py

Generates 'ecommerce.db' - an SQLite database for an online retail store.
Uses sqlite3 cursor execution. Data is synthetic via Faker.

Tables:
 - Categories
 - Products
 - Customers  (1000 rows)
 - Orders     (~1400 rows)
 - Order_Items (composite PK: order_id, item_no)
 - Shipments

Realism:
 - ~2% missing customer contact fields
 - ~1% duplicate customer names/emails
 - ~1% duplicate-like orders
Data types: nominal, ordinal, interval (dates), ratio (price/quantity)
"""

import sqlite3
import random
from faker import Faker
from datetime import datetime, timedelta

#  Config & Setup 
fake = Faker()
Faker_seed = 100
Faker.seed(Faker_seed)
random.seed(Faker_seed)


# Helper date functions
def rand_date(start_year=2021, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).date().isoformat()

def rand_order_date():
    return rand_date(2021, 2024)

def rand_shipment_date(order_date_iso):
    # shipment 0-10 days after order, sometimes delayed (up to 20 days), sometimes missing
    if order_date_iso is None:
        return None
    od = datetime.fromisoformat(order_date_iso)
    add = random.choices([random.randint(0,10), random.randint(11,20), None], weights=[0.8,0.15,0.05])[0]
    if add is None:
        return None
    return (od + timedelta(days=add)).date().isoformat()

# Data Generation Parameters 
NUM_CUSTOMERS = 1000
NUM_CATEGORIES = 12
NUM_PRODUCTS = 300
NUM_ORDERS = 1400

# Product price ranges, realistic
PRICE_MIN = 5.0
PRICE_MAX = 1000.0

# Customer tiers (ordinal)
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]  # Bronze < Silver < Gold < Platinum

# Genders (nominal)
GENDERS = ["Male", "Female", "Non-binary", "Prefer not to say"]

#  Create DB and Schema 
DB_FILENAME = "ecommerce.db"
conn = sqlite3.connect(DB_FILENAME)
cur = conn.cursor()
cur.execute("PRAGMA foreign_keys = ON;")


# Create schema
cur.executescript("""
CREATE TABLE Categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE Products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    price REAL NOT NULL CHECK(price >= 0),
    stock INTEGER NOT NULL CHECK(stock >= 0),
    FOREIGN KEY(category_id) REFERENCES Categories(category_id) ON DELETE RESTRICT
);

CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    gender TEXT,
    date_of_birth TEXT,
    email TEXT,
    phone_number TEXT,
    address TEXT,
    customer_tier TEXT,  -- ordinal
    registration_date TEXT,
    total_spent REAL DEFAULT 0 CHECK(total_spent >= 0)
);

CREATE TABLE Orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TEXT,
    order_total REAL CHECK(order_total >= 0),
    promo_code TEXT,
    FOREIGN KEY(customer_id) REFERENCES Customers(customer_id) ON DELETE CASCADE
);

CREATE TABLE Order_Items (
    order_id INTEGER,
    item_no INTEGER,
    product_id INTEGER,
    quantity INTEGER CHECK(quantity >= 0),
    unit_price REAL CHECK(unit_price >= 0),
    line_total REAL CHECK(line_total >= 0),
    PRIMARY KEY(order_id, item_no), -- composite PK
    FOREIGN KEY(order_id) REFERENCES Orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY(product_id) REFERENCES Products(product_id) ON DELETE RESTRICT
);

CREATE TABLE Shipments (
    shipment_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    shipped_date TEXT,
    delivery_date TEXT,
    carrier TEXT,
    tracking_number TEXT,
    FOREIGN KEY(order_id) REFERENCES Orders(order_id) ON DELETE CASCADE
);
""")
conn.commit()

#  Populate Categories 
categories = [
    "Electronics","Clothing","Home & Kitchen","Books","Toys","Sports",
    "Beauty","Groceries","Automotive","Office","Garden","Health"
]
cur.executemany("INSERT INTO Categories (category_id, category_name) VALUES (?,?)",
                [(i+1, categories[i]) for i in range(len(categories))])
conn.commit()

#  Populate Products 
products = []
for pid in range(1, NUM_PRODUCTS+1):
    cat_id = random.randint(1, NUM_CATEGORIES)
    pname = f"{fake.word().capitalize()} {random.choice(['Pro','X','Plus','Mini','Max','Series'])} {random.randint(100,999)}"
    price = round(random.uniform(PRICE_MIN, PRICE_MAX), 2)
    stock = random.randint(0, 500)
    products.append((pid, pname, cat_id, price, stock))

cur.executemany(
    "INSERT INTO Products (product_id, product_name, category_id, price, stock) VALUES (?,?,?,?,?)",
    products
)
conn.commit()

#  Populate Customers (1000 rows) 
customers = []
for cid in range(1, NUM_CUSTOMERS+1):
    name = fake.name()
    gender = random.choice(GENDERS)
    dob = (datetime.now() - timedelta(days=random.randint(18*365, 70*365))).date().isoformat()
    email = fake.email()
    phone = fake.phone_number()
    address = fake.address().replace("\n", ", ")
    tier = random.choices(TIERS, weights=[0.5,0.3,0.15,0.05])[0]
    reg_date = rand_date(2018, 2024)
    total_spent = 0.0
    customers.append([cid, name, gender, dob, email, phone, address, tier, reg_date, total_spent])

# Keeping 2% missing contact info
num_missing = int(0.02 * len(customers))
for idx in random.sample(range(len(customers)), num_missing):
    col = random.choice([4,5,6])  # email index 4, phone index 5, address index 6 (0-based in list)
    customers[idx][col] = None

# Keeping 1% duplicate names/emails
num_dup = int(0.01 * len(customers))
dup_indices = random.sample(range(len(customers)), num_dup)
for idx in dup_indices:
    src = random.choice(customers)
    customers[idx][1] = src[1]  # duplicate name
    if random.random() < 0.5:
        customers[idx][4] = src[4]  # duplicate email

cur.executemany("""
INSERT INTO Customers (customer_id, customer_name, gender, date_of_birth, email, phone_number, address, customer_tier, registration_date, total_spent)
VALUES (?,?,?,?,?,?,?,?,?,?)
""", customers)
conn.commit()

#  Populate Orders (~1400 rows) 
orders = []
order_items = []
shipments = []
order_id_seq = 1
for _ in range(NUM_ORDERS):
    cust = random.randint(1, NUM_CUSTOMERS)
    order_date = rand_order_date()
    promo = random.choice([None, "NEW10", "FREESHIP", "SUMMER20", None, None])
    # choose 1-4 items per order
    n_items = random.choices([1,2,3,4], weights=[0.6,0.25,0.1,0.05])[0]
    item_no = 1
    order_total = 0.0
    for _ in range(n_items):
        prod = random.choice(products)  # (pid, name, cat, price, stock)
        pid = prod[0]
        unit_price = prod[3]
        qty = random.randint(1,5)
        line_total = round(unit_price * qty * (1 - random.choice([0,0,0.05,0.1])), 2)  # occasional discount
        order_items.append((order_id_seq, item_no, pid, qty, unit_price, line_total))
        order_total += line_total
        item_no += 1
    # Round order_total:
    order_total = round(order_total, 2)
    orders.append((order_id_seq, cust, order_date, order_total, promo))
    # 80% of orders have shipment record, shipment date sometimes missing (delayed)
    if random.random() < 0.8:
        shipped = rand_shipment_date(order_date)
        delivered = rand_shipment_date(shipped) if shipped is not None else None
        shipments.append((None, order_id_seq, shipped, delivered, random.choice(["DHL","FedEx","UPS","Local"]), fake.bothify(text='TRACK-#####')))
    order_id_seq += 1

# Add ~1% duplicate-like orders (same cust/order_date but new order_id)
num_order_dupes = max(1, int(0.01 * len(orders)))
for _ in range(num_order_dupes):
    row = random.choice(orders)
    cust = row[1]
    order_date = row[2]
    promo = row[4]
    # small order
    prod = random.choice(products)
    unit_price = prod[3]
    qty = 1
    line_total = round(unit_price * qty, 2)
    orders.append((order_id_seq, cust, order_date, line_total, promo))
    order_items.append((order_id_seq, 1, prod[0], qty, unit_price, line_total))
    if random.random() < 0.8:
        shipped = rand_shipment_date(order_date)
        delivered = rand_shipment_date(shipped) if shipped is not None else None
        shipments.append((None, order_id_seq, shipped, delivered, random.choice(["DHL","FedEx","UPS","Local"]), fake.bothify(text='TRACK-#####')))
    order_id_seq += 1

# Bulk insert Orders
cur.executemany("INSERT INTO Orders (order_id, customer_id, order_date, order_total, promo_code) VALUES (?,?,?,?,?)", orders)
conn.commit()

# Bulk insert Order_Items (composite PK order_id, item_no)
cur.executemany("INSERT INTO Order_Items (order_id, item_no, product_id, quantity, unit_price, line_total) VALUES (?,?,?,?,?,?)", order_items)
conn.commit()

# Bulk insert Shipments (shipment_id is auto)
cur.executemany("INSERT INTO Shipments (shipment_id, order_id, shipped_date, delivery_date, carrier, tracking_number) VALUES (?,?,?,?,?,?)", shipments)
conn.commit()

#  Update Customers.total_spent (aggregate)
# Compute total spent per customer from Orders and update Customers.total_spent
cur.execute("""
SELECT customer_id, SUM(order_total) FROM Orders GROUP BY customer_id
""")
cust_totals = cur.fetchall()
cur.executemany("UPDATE Customers SET total_spent = ? WHERE customer_id = ?", [(round(total if total else 0,2), cid) for cid, total in cust_totals])
conn.commit()

#  Final Integrity Checks 
# Enable foreign key check
cur.execute("PRAGMA foreign_keys = ON;")
# You can run PRAGMA foreign_key_check later in sqlite shell to confirm

# Summary print
cur.execute("SELECT COUNT(*) FROM Customers")
print("Customers:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM Products")
print("Products:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM Orders")
print("Orders:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM Order_Items")
print("Order_Items:", cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM Shipments")
print("Shipments:", cur.fetchone()[0])

conn.close()
print("Database generated:", DB_FILENAME)

