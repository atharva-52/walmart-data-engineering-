# Walmart Data Engineering Project — Airflow + dbt + Databricks

## Overview
An end-to-end data pipeline that models Walmart retail sales data using a **Bronze → Silver → Gold (medallion) architecture**. Raw source tables are incrementally loaded, denormalized into a wide staging table, and modeled into a **star schema** (fact + SCD Type 2 dimensions) using dbt on Databricks, all orchestrated by Apache Airflow running in Docker.

## Architecture

```
Databricks (walmart.bronze)   →   dbt incremental models (walmart.silver_t)   →   One Big Table (obt_b)
      raw source tables              *_t.sql (customers_t, orders_t, etc.)          denormalized join of all silver tables
                                                                                              │
                                                                                              ▼
                                                                          eph_*.sql (staging dimension selects)
                                                                                              │
                                                                       ┌──────────────────────┴──────────────────────┐
                                                                       ▼                                              ▼
                                                        dbt snapshots → dim_* (walmart.gold, SCD Type 2)   fact_orders (walmart.gold)

Orchestrated end-to-end by Apache Airflow (CeleryExecutor, Dockerized)
```

## Tech Stack
- **Orchestration:** Apache Airflow 3.2.2 (CeleryExecutor, Postgres metadata DB, Redis broker), run via Docker Compose
- **Transformation:** dbt-core 1.11.x with the `dbt-databricks` adapter
- **Warehouse/Lakehouse:** Databricks (Delta tables across `bronze`, `silver_t`, and `gold` schemas in the `walmart` database)
- **Language:** SQL (Jinja-templated), Python

## Data Model

**Bronze (source):** raw tables registered in `sources.yml` under `walmart.bronze` — `orders`, `customers`, `products`, `order_items`, `stores`, `employees`.

**Silver:** one incremental model per source table (`orders_t`, `customers_t`, `products_t`, `order_items_t`, `stores_t`, `employees_t`). Each:
- Selects all columns from its bronze source plus a `processed_at` timestamp
- Uses `is_incremental()` to only pull rows where `updated_timestamp` is newer than the max already loaded, keyed on a natural unique key (e.g. `customer_id`, `order_id`)

**OBT (One Big Table):** `obt_b.sql` dynamically joins all six silver tables (orders as the base, left-joined to customers, order_items, products, employees, stores) using a Jinja config-driven loop, rather than hardcoded joins.

**Staging dimension selects:** `eph_customers`, `eph_employees`, `eph_orders`, `eph_products`, `eph_stores` — each pulls the relevant distinct columns for that entity off the OBT, adding a `*_gold_processed_at` timestamp.

**Gold — Dimensions (SCD Type 2):** dbt **snapshots** (`dim_customers`, `dim_employees`, `dim_orders`, `dim_products`, `dim_stores`) built on top of the `eph_*` staging models, using the `timestamp` strategy against each entity's `updated_timestamp` column, materialized into `walmart.gold`. Current records are flagged with `dbt_valid_to = '9999-12-31'` instead of `NULL`.

**Gold — Fact:** `fact_orders.sql` selects order/line-item grain measures (`quantity`, `unit_price`, `line_amount`, `total_amount`) and foreign keys (`order_id`, `product_id`, `store_id`, `employee_id`, `customer_id`) off the OBT.

## dbt Features Used
- **Sources** (`sources.yml`) to declare and reference raw Databricks tables
- **Incremental models** with `is_incremental()` watermark logic for efficient re-runs
- **Jinja macros** for a config-driven dynamic join (`obt_b.sql`) instead of repeated boilerplate SQL
- **Custom schema macro** (`generate_schema_name`) so models land in explicit schemas (`silver_t`, `gold`) rather than dbt's default `<target_schema>_<custom>` naming
- **Snapshots** for SCD Type 2 dimension history
- **Generic tests** (`not_null`, `unique`, including a conditional `unique` test scoped with a `where` clause) defined in `properties.yml`
- **Singular test** (`test_obt.sql`) that warns if any foreign key in the OBT is null, catching join issues early

## Project Structure
```
├── dags/                       # Airflow DAGs orchestrating the dbt runs
├── walmart_project/             # dbt project
│   ├── models/
│   │   ├── silver/               # customers_t, employees_t, order_items_t, orders_t, products_t, stores_t
│   │   ├── obt_b.sql             # one big table (join of all silver models)
│   │   ├── staging/              # eph_customers, eph_employees, eph_orders, eph_products, eph_stores
│   │   ├── fact_orders.sql
│   │   └── properties.yml        # model-level data tests
│   ├── snapshots/                # dim_customers, dim_employees, dim_orders, dim_products, dim_stores (SCD2)
│   ├── tests/                    # test_obt.sql (singular test)
│   ├── macros/                   # custom_schema.sql
│   └── models/sources.yml
├── Dockerfile                    # extends the Airflow image with dbt + dependencies
├── docker-compose.yaml           # Airflow (API server, scheduler, worker, triggerer, dag-processor) + Postgres + Redis
├── requirements.txt
└── .env                          # AIRFLOW_UID, FERNET_KEY, DB/Databricks credentials (not committed)
```
*Folder names above are inferred from the dbt model references — adjust to match your actual directory layout if it differs.*

## Getting Started

### Prerequisites
- Docker & Docker Compose
- A Databricks workspace with a `walmart` database (`bronze` schema populated with source tables)
- A dbt `profiles.yml` configured with your Databricks SQL warehouse / cluster credentials

### Setup
```bash
# Clone the repository
git clone https://github.com/anshlambagit/Walmart_Airflow_DBT_Project.git
cd Walmart_Airflow_DBT_Project

# Create your .env file (AIRFLOW_UID, FERNET_KEY, etc.)
cp .env.example .env   # if provided, otherwise create manually

# Build and start Airflow
docker-compose up airflow-init
docker-compose up -d

# Airflow UI available at http://localhost:8080 (default user/pass: airflow/airflow)
```

### Running dbt manually (for local testing)
```bash
cd walmart_project
dbt run          # builds silver models, obt_b, staging, and fact_orders
dbt snapshot      # builds/updates SCD Type 2 dimension snapshots
dbt test          # runs generic + singular data tests
```

## Testing & Data Quality
- Uniqueness/null checks on key columns (`product_id`, `order_id`) via `properties.yml`
- `test_obt.sql` flags (warns, doesn't fail the build) if any join in the OBT produced a null foreign key — useful for catching upstream data issues without blocking the pipeline

## Future Improvements
- Add CI to run `dbt build` and `dbt test` on pull requests
- Add data freshness checks on bronze sources
- Extend fact table grain to support returns/refunds if present in source data


