"""
Supply Chain ETL Pipeline
Extract -> Validate -> Clean -> Transform -> Load
"""
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

RAW_DIR  = os.path.expanduser('~/Desktop/supply_chain_project/data/raw')
PROC_DIR = os.path.expanduser('~/Desktop/supply_chain_project/data/processed')
LOG_DIR  = os.path.expanduser('~/Desktop/supply_chain_project/logs')
os.makedirs(PROC_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

def extract():
    log.info("STEP 1: EXTRACT")
    tables = {}
    for name in ['warehouses','suppliers','products','orders','shipments']:
        df = pd.read_csv(f'{RAW_DIR}/{name}.csv')
        tables[name] = df
        log.info(f"  Loaded {name}: {len(df):,} rows")
    return tables

def validate(tables):
    log.info("STEP 2: VALIDATE")
    for name, df in tables.items():
        nulls = df.isnull().sum()
        null_cols = nulls[nulls > 0]
        if not null_cols.empty:
            for col, cnt in null_cols.items():
                log.warning(f"  [{name}] NULL: {col} has {cnt:,} missing values ({cnt/len(df)*100:.1f}%)")
    log.info("  Validation complete")

def clean(tables):
    log.info("STEP 3: CLEAN")
    orders = tables['orders'].copy()
    median_days = orders['promised_delivery_days'].median()
    nulls = orders['promised_delivery_days'].isna().sum()
    orders['promised_delivery_days'] = orders['promised_delivery_days'].fillna(median_days)
    log.info(f"  [orders] Imputed {nulls} missing promised_delivery_days with median={median_days}")
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    orders['order_month'] = orders['order_date'].dt.month
    orders['order_year']  = orders['order_date'].dt.year

    shipments = tables['shipments'].copy()
    p99 = shipments['shipment_cost_usd'].quantile(0.99)
    outliers = (shipments['shipment_cost_usd'] > p99).sum()
    shipments['shipment_cost_usd'] = shipments['shipment_cost_usd'].clip(upper=p99)
    log.info(f"  [shipments] Capped {outliers} cost outliers at ${p99:,.2f}")
    shipments['promised_delivery_days'] = shipments['promised_delivery_days'].fillna(
        shipments['promised_delivery_days'].median())
    mask = shipments['actual_delivery_days'].isna() & (shipments['delivery_status'] != 'Cancelled')
    shipments.loc[mask, 'actual_delivery_days'] = shipments.loc[mask, 'promised_delivery_days'] + 3

    tables['orders']    = orders
    tables['shipments'] = shipments
    return tables

def transform(tables):
    log.info("STEP 4: TRANSFORM — Calculating KPIs")
    shipments  = tables['shipments'][tables['shipments']['delivery_status'] != 'Cancelled'].copy()
    warehouses = tables['warehouses'].copy()

    shipments['is_on_time']  = shipments['delivery_status'].isin(['On Time','Early']).astype(int)
    shipments['delay_days']  = shipments['actual_delivery_days'] - shipments['promised_delivery_days']

    wh_kpi = shipments.groupby('warehouse_id').agg(
        total_shipments   = ('shipment_id','count'),
        on_time_shipments = ('is_on_time','sum'),
        total_cost_usd    = ('shipment_cost_usd','sum'),
        avg_cost_usd      = ('shipment_cost_usd','mean'),
        avg_delay_days    = ('delay_days','mean'),
    ).reset_index()
    wh_kpi['on_time_pct'] = round(wh_kpi['on_time_shipments'] / wh_kpi['total_shipments'] * 100, 2)
    wh_kpi = wh_kpi.merge(warehouses[['warehouse_id','warehouse_name','region']], on='warehouse_id')
    log.info(f"  Warehouse KPIs calculated for {len(wh_kpi)} warehouses")
    return {'warehouse_kpi': wh_kpi, 'shipments_enriched': shipments}

def load(transformed):
    log.info("STEP 5: LOAD")
    transformed['warehouse_kpi'].to_csv(f'{PROC_DIR}/kpi_warehouse.csv', index=False)
    transformed['shipments_enriched'].to_csv(f'{PROC_DIR}/shipments_enriched.csv', index=False)
    log.info("  Saved kpi_warehouse.csv and shipments_enriched.csv")

if __name__ == '__main__':
    log.info("Starting ETL Pipeline...")
    tables = extract()
    validate(tables)
    cleaned = clean(tables)
    transformed = transform(cleaned)
    load(transformed)
    log.info("ETL Pipeline Complete!")
