import sqlite3
import pandas as pd

conn = sqlite3.connect("shipment_database.db")
cursor = conn.cursor()

# --- Part 1: Insert from shipping_data_0.csv ---
df0 = pd.read_csv("data/shipping_data_0.csv")
for _, row in df0.iterrows():
    product_name = row['product']
    cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
    result = cursor.fetchone()
    product_id = result[0] if result else cursor.execute("INSERT INTO product (name) VALUES (?)", (product_name,)).lastrowid
    if not result:
        print(f"Inserted missing product '{product_name}' with id {product_id}")
    cursor.execute("INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
                   (product_id, row['product_quantity'], row['origin_warehouse'], row['destination_store']))

# --- Part 2: Merge shipping_data_1 and shipping_data_2 ---
df1 = pd.read_csv("data/shipping_data_1.csv")
df2 = pd.read_csv("data/shipping_data_2.csv")
merged = pd.merge(df1, df2, on="shipment_identifier")

shipment_counts = merged.groupby(['shipment_identifier', 'product']).size().reset_index(name='quantity')
merged_with_counts = pd.merge(shipment_counts, df2, on="shipment_identifier")

for _, row in merged_with_counts.iterrows():
    product_name = row['product']
    cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
    result = cursor.fetchone()
    product_id = result[0] if result else cursor.execute("INSERT INTO product (name) VALUES (?)", (product_name,)).lastrowid
    if not result:
        print(f"Inserted missing product '{product_name}' with id {product_id}")
    cursor.execute("INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
                   (product_id, row['quantity'], row['origin_warehouse'], row['destination_store']))

conn.commit()
conn.close()
print("All records from all spreadsheets inserted successfully.")