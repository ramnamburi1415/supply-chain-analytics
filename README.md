# Supply Chain Performance Analytics

End-to-end supply chain analytics pipeline built with MySQL, Python, and Pandas.

## Tech Stack
- **MySQL 9.1** — relational database with 5 tables, FKs, indexes
- **Python + Pandas** — ETL pipeline (extract, validate, clean, transform, load)
- **SQL** — 5 analytics queries including 4-table JOINs
- **HTML + Chart.js** — interactive KPI dashboard

## Dataset
- 10,000 orders and shipments
- 10 warehouses across 4 US regions
- 6 carriers, 30 suppliers, 100 products
- Jan 2023 – Dec 2024

## Key Findings
- Philadelphia Hub leads OTD at 89.55%
- FedEx best carrier at 88.54% on-time
- USPS worst with 251 late deliveries
- Supplier self-reporting unreliable — 27pp gap found

## How to Run
```bash
# Generate data
python3 python/generate_data.py

# Run ETL pipeline
python3 python/etl_pipeline.py

# Load into MySQL
python3 python/db_runner.py

# View dashboard
open dashboard/supply_chain_dashboard.html
```
