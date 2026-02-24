import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

np.random.seed(42)
RAW_DIR = os.path.expanduser('~/Desktop/supply_chain_project/data/raw')
os.makedirs(RAW_DIR, exist_ok=True)

print("Generating data...")

warehouses = pd.DataFrame({
    'warehouse_id':   [f'WH{str(i).zfill(2)}' for i in range(1, 11)],
    'warehouse_name': ['Chicago Central','Los Angeles West','New York East','Houston South','Phoenix Desert','Philadelphia Hub','San Antonio TX','San Diego Port','Dallas Fort Worth','San Jose Tech'],
    'region': ['Midwest','West','East','South','West','East','South','West','South','West'],
    'capacity_units': np.random.randint(5000, 20000, 10),
    'operating_cost_per_day': np.round(np.random.uniform(2000, 8000, 10), 2),
    'manager': ['Alice Johnson','Bob Martinez','Carol White','David Lee','Eva Brown','Frank Garcia','Grace Kim','Henry Wilson','Irene Taylor','James Davis']
})
warehouses.to_csv(f'{RAW_DIR}/warehouses.csv', index=False)
print("  warehouses.csv done")

supplier_names = ['GlobalParts Inc','FastShip Co','MegaSupply Ltd','Prime Logistics','QuickSource','EastCoast Supplies','WestWave Goods','TechParts USA','GreenRoute Logistics','Apex Distribution','Nexus Materials','BlueStar Shipping','Continental Goods','Horizon Supplies','Pacific Rim Traders','Mountain Supply Co','SunBelt Logistics','Lakeside Distributors','Coastal Freight','MetroCore Supply','Industrial Partners','Precision Parts Co','United Distributors','Gateway Logistics','Summit Supply Chain','Riverdale Goods','Clearwater Supply','Ironclad Logistics','Sterling Materials','Velocity Freight']
suppliers = pd.DataFrame({
    'supplier_id': [f'SUP{str(i).zfill(3)}' for i in range(1, 31)],
    'supplier_name': supplier_names,
    'country': np.random.choice(['USA','Canada','Mexico','China','Germany'], 30),
    'reliability_score': np.round(np.random.uniform(0.70, 0.99, 30), 2),
    'avg_lead_time_days': np.random.randint(2, 21, 30),
    'contract_value_usd': np.round(np.random.uniform(50000, 2000000, 30), 2)
})
suppliers.to_csv(f'{RAW_DIR}/suppliers.csv', index=False)
print("  suppliers.csv done")

products = pd.DataFrame({
    'product_id': [f'PROD{str(i).zfill(4)}' for i in range(1, 101)],
    'product_name': [f'Product_{i}' for i in range(1, 101)],
    'category': np.random.choice(['Electronics','Machinery','Raw Materials','Consumables','Packaging'], 100),
    'unit_weight_kg': np.round(np.random.uniform(0.1, 50.0, 100), 2),
    'unit_cost_usd': np.round(np.random.uniform(5.0, 500.0, 100), 2),
    'supplier_id': np.random.choice(suppliers['supplier_id'], 100)
})
products.to_csv(f'{RAW_DIR}/products.csv', index=False)
print("  products.csv done")

n = 10000
start_date = datetime(2023, 1, 1)
order_dates = [start_date + timedelta(days=int(x)) for x in np.random.randint(0, 730, n)]
promised = np.where(np.random.random(n) < 0.15, np.nan, np.random.randint(3, 15, n).astype(float))

orders = pd.DataFrame({
    'order_id': [f'ORD{str(i).zfill(6)}' for i in range(1, n+1)],
    'order_date': [d.strftime('%Y-%m-%d') for d in order_dates],
    'promised_delivery_days': promised,
    'warehouse_id': np.random.choice(warehouses['warehouse_id'], n),
    'product_id': np.random.choice(products['product_id'], n),
    'quantity_ordered': np.random.randint(1, 500, n),
    'order_priority': np.random.choice(['Low','Medium','High','Critical'], n, p=[0.30,0.40,0.20,0.10]),
    'customer_region': np.random.choice(['East','West','Midwest','South'], n),
    'sales_channel': np.random.choice(['Online','B2B','Retail'], n, p=[0.40,0.45,0.15])
})
orders.to_csv(f'{RAW_DIR}/orders.csv', index=False)
print("  orders.csv done")

status = np.random.choice(['On Time','Early','Late','Cancelled'], n, p=[0.72,0.12,0.13,0.03])
base = np.where(np.isnan(promised), 7, promised)
actual = np.where(status=='Early', base - np.random.randint(1,4,n),
         np.where(status=='Late', base + np.random.randint(1,10,n), base)).astype(float)
actual = np.where(np.random.random(n) < 0.05, np.nan, actual)
costs = np.round(np.random.uniform(15, 300, n) + (np.where(np.isnan(actual),7,actual) * np.random.uniform(2,8,n)), 2)
costs = np.where(np.random.random(n) < 0.01, np.random.uniform(5000,20000,n), costs)

shipments = pd.DataFrame({
    'shipment_id': [f'SHP{str(i).zfill(6)}' for i in range(1, n+1)],
    'order_id': orders['order_id'],
    'warehouse_id': orders['warehouse_id'],
    'carrier': np.random.choice(['FedEx','UPS','DHL','USPS','Amazon Logistics','XPO Freight'], n),
    'ship_date': orders['order_date'],
    'promised_delivery_days': promised,
    'actual_delivery_days': actual,
    'delivery_status': status,
    'shipment_cost_usd': costs,
    'weight_kg': np.round(np.random.uniform(0.5, 200.0, n), 2),
    'distance_km': np.random.randint(50, 5000, n),
})
shipments.to_csv(f'{RAW_DIR}/shipments.csv', index=False)
print("  shipments.csv done")
print(f"\nAll done! 5 CSV files created in {RAW_DIR}")
