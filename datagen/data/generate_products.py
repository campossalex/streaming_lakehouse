# One-off build tool — NOT part of the live lab pipeline.
#
# Regenerates products.csv: 500 synthetic-but-realistic products loaded into
# PostgreSQL's product_catalog table (see ../../pg_shop_ddl.sql),
# which is then replicated into fluss.orders.product_lookup via postgres-cdc (Lab 2).
#
# Usage: python3 generate_products.py > products.csv
#   (requires: pip install faker)

import csv
import random
import sys
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = {
    "Electronics":       (["Wireless", "Portable", "Smart", "HD", "Bluetooth", "USB-C", "Compact"],
                           ["Headphones", "Speaker", "Charger", "Webcam", "Router", "Power Bank", "Earbuds", "Monitor Stand"]),
    "Home & Kitchen":     (["Stainless Steel", "Non-Stick", "Ceramic", "Electric", "Reusable", "Insulated"],
                           ["Espresso Machine", "Blender", "Cutting Board", "Knife Set", "Kettle", "Mixing Bowl", "Air Fryer"]),
    "Apparel":            (["Slim-Fit", "Organic Cotton", "Lightweight", "Classic", "Waterproof", "Merino Wool"],
                           ["Running Shoes", "Hoodie", "Denim Jacket", "T-Shirt", "Rain Jacket", "Socks", "Baseball Cap"]),
    "Sports & Outdoors":  (["Adjustable", "Foldable", "Non-Slip", "Heavy-Duty", "Ultralight"],
                           ["Yoga Mat", "Camping Tent", "Water Bottle", "Resistance Bands", "Hiking Backpack", "Bike Helmet"]),
    "Books":              (["Illustrated", "Bestselling", "Collector's Edition", "Annotated"],
                           ["Novel", "Cookbook", "Field Guide", "Biography", "Short Story Collection", "Travel Guide"]),
    "Grocery":            (["Organic", "Fair-Trade", "Gluten-Free", "Single-Origin", "Small-Batch"],
                           ["Coffee Beans", "Olive Oil", "Trail Mix", "Herbal Tea", "Granola", "Honey"]),
    "Beauty":             (["Hydrating", "Fragrance-Free", "Vegan", "Mineral", "Overnight"],
                           ["Face Serum", "Shampoo", "Lip Balm", "Body Lotion", "Sunscreen", "Face Mask"]),
    "Toys":               (["Wooden", "Educational", "Glow-in-the-Dark", "Interactive", "Collectible"],
                           ["Puzzle", "Building Blocks", "Board Game", "Plush Toy", "Model Kit", "Action Figure"]),
    "Office Supplies":    (["Ergonomic", "Refillable", "Adjustable", "Wireless", "Recycled"],
                           ["Desk Lamp", "Notebook", "Standing Desk Mat", "Pen Set", "Monitor Arm", "Label Maker"]),
    "Pet Supplies":       (["Durable", "Orthopedic", "Interactive", "Grain-Free", "Washable"],
                           ["Dog Bed", "Cat Tree", "Chew Toy", "Pet Carrier", "Feeding Bowl", "Leash"]),
}

ROWS_PER_CATEGORY = 50
writer = csv.writer(sys.stdout)
writer.writerow([
    "product_id", "sku", "product_name", "category", "brand",
    "unit_price", "cost", "weight_kg", "in_stock", "rating", "created_at",
])

product_num = 1
for category, (adjectives, nouns) in CATEGORIES.items():
    for _ in range(ROWS_PER_CATEGORY):
        product_id = f"PRD-{product_num:05d}"
        sku = "SKU-" + fake.bothify(text="????####").upper()
        brand = fake.company().split(",")[0].replace(" LLC", "").replace(" Inc", "").replace(" Group", "")
        product_name = f"{brand} {random.choice(adjectives)} {random.choice(nouns)}"
        unit_price = round(random.uniform(5.0, 450.0), 2)
        cost = round(unit_price * random.uniform(0.35, 0.65), 2)
        weight_kg = round(random.uniform(0.05, 12.0), 2)
        in_stock = random.random() > 0.08
        rating = round(random.uniform(3.0, 5.0), 1)
        created_at = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 900))).strftime("%Y-%m-%d %H:%M:%S")

        writer.writerow([
            product_id, sku, product_name, category, brand,
            unit_price, cost, weight_kg, in_stock, rating, created_at,
        ])
        product_num += 1
