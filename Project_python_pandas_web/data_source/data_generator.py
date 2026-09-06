import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# Lấy đúng folder chứa file .py
current_folder = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(current_folder, "hardware_sales_2019_2025_raw.csv")

n_rows = 20000

product_categories = [
    "RAM", "CPU", "GPU", "VGA", "Monitor", "Case", "Fan",
    "Power Supply", "Mouse", "Keyboard", "Motherboard", "SSD", "HDD"
]

brands = [
    "Intel", "AMD", "NVIDIA", "Corsair", "G.Skill", "Asus",
    "MSI", "Gigabyte", "Logitech", "Razer", "Dell", "HP", "Samsung"
]

customer_segments = ["Retail", "Wholesale", "Online", "Enterprise", "Government"]
regions = ["North America", "Europe", "Asia Pacific", "Vietnam", "Southeast Asia", "Middle East"]
payment_methods = ["Credit Card", "Cash", "Bank Transfer", "E-Wallet", "COD"]

def random_date():
    start = datetime(2019, 1, 1)
    end = datetime(2025, 12, 31)
    delta = end - start
    d = start + timedelta(days=random.randint(0, delta.days))
    fmt = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%Y/%d/%m"])
    return d.strftime(fmt)

def random_bool_messy():
    return random.choice(["True", "False", "1", "0", "yes", "no", "", None])

def random_unit_price():
    base = round(random.uniform(10, 2000), 2)
    return random.choice([base, f"{base} USD", f"{base}", "N/A", ""])

def random_quantity():
    q = random.randint(1, 10)
    return random.choice([q, str(q), f"{q} pcs", "two" if q == 2 else str(q), -1 if random.random() < 0.01 else q])

def random_total_amount(unit_price, quantity):
    if random.random() < 0.2:
        return None
    if isinstance(unit_price, (int, float)) and isinstance(quantity, int):
        correct = round(unit_price * quantity, 2)
    else:
        correct = round(random.uniform(20, 5000), 2)
    return random.choice([correct, f"{correct} dollars", correct * random.choice([0.9, 1.1, 2])])

rows = []
for i in range(n_rows):
    tid = f"T-{i:06d}"
    date = random_date()
    category = random.choice(product_categories)
    brand = random.choice(brands)
    product_name = f"{brand} {category} Model {random.randint(100, 999)}"

    if random.random() < 0.05:
        category = category.lower()
    if random.random() < 0.03:
        brand = brand.upper()

    unit_price = random_unit_price()
    quantity = random_quantity()
    total_amount = random_total_amount(
        unit_price if isinstance(unit_price, (int, float)) else random.uniform(10, 2000),
        quantity if isinstance(quantity, int) else random.randint(1, 10)
    )

    segment = random.choice(customer_segments)
    region = random.choice(regions)
    payment = random.choice(payment_methods)
    is_refund = random_bool_messy()

    if random.random() < 0.02:
        payment = None
    if random.random() < 0.02:
        brand = None

    rows.append({
        "Transaction_ID": tid,
        "Transaction_Date": date,
        "Product_Category": category,
        "Product_Name": product_name,
        "Brand": brand,
        "Unit_Price_USD": unit_price,
        "Quantity": quantity,
        "Total_Amount_USD": total_amount,
        "Customer_Segment": segment,
        "Region": region,
        "Payment_Method": payment,
        "Is_Refund": is_refund,
    })

df_raw = pd.DataFrame(rows)
df_raw.to_csv(output_path, index=False)

print("CSV generated at:", output_path)
