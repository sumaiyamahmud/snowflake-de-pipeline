# 🛒 Online Retail — Snowflake Data Engineering Pipeline

> End-to-end data pipeline that extracts transactional retail data from PostgreSQL, lands it in S3, loads it into Snowflake, transforms it into a dimensional model with dbt, validates it with tests, and orchestrates the whole flow on a daily schedule with Airflow.

![Pipeline Status](https://img.shields.io/badge/pipeline-passing-brightgreen)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![Snowflake](https://img.shields.io/badge/snowflake-warehouse-blue)
![Airflow](https://img.shields.io/badge/airflow-2.10-red)
![Python](https://img.shields.io/badge/python-3.12-yellow)

---

## 📐 Architecture

```
┌──────────────┐     ┌────────┐     ┌──────────────┐     ┌─────────────────────┐
│   AWS RDS    │ ──▶│    S3   │ ──▶│  Snowflake   │ ──▶│  dbt: STAGING  ──▶  │
│  PostgreSQL  │     │  raw/  │     │  RAW schema  │     │      WAREHOUSE      │
└──────────────┘     └────────┘     └──────────────┘     └─────────────────────┘
       │                                                            │
       │                                                            ▼
       │                                                 ┌─────────────────────┐
       └──────────────────  Airflow DAG  ────────────────│ dbt tests           │
                       (daily orchestration)             │ (16 quality checks) │
                                                         └─────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Tool |
|---|---|
| **Source database** | AWS RDS PostgreSQL |
| **Object storage** | AWS S3 |
| **Data warehouse** | Snowflake |
| **Transformation** | dbt (Snowflake adapter) |
| **Orchestration** | Apache Airflow |
| **Extract scripting** | Python (boto3, pandas, SQLAlchemy) |
| **Version control** | Git / GitHub |

---

## 📊 Dataset

UCI Online Retail dataset — **541,909 transactional rows** from a UK-based online retailer (December 2010 – December 2011). Each row represents one product on one invoice line item.

---

## 🗂 Data Model

The warehouse uses a **Kimball-style dimensional model** at line-item grain.

### Layers

| Schema             | Purpose                                                | Materialization |
|--------------------|--------------------------------------------------------|-----------------|
| `RAW`              | Source data loaded directly from S3 via `COPY INTO`    | Table |
| `STAGING`          | Cleaned, renamed, type-cast                            | View |
| `WAREHOUSE`        | Dimensional model — facts + dimensions                 | Tables |

### Models

**🔹 Staging — `STAGING.stg_online_retail`**
- One row per source line item
- Renames raw columns to consistent snake_case (`invoice_no` → `invoice_id`, etc.)
- Filters out rows with missing keys

**🔹 Dimensions**
- `dim_customers` — one row per customer with derived behavioral attributes (first/most recent order date, total orders)
- `dim_products` — one row per product

**🔹 Fact**
- `fct_orders` — grain = one row per `(invoice_id, product_id)`. Contains raw measures (`quantity`, `unit_price`), a calculated measure (`line_total`), and a business-rule flag (`is_cancellation` for invoices starting with "C")

### Why this grain?

Line-item grain is the most flexible — invoice-level and customer-level analytics can always be **aggregated up** from line items, but the reverse isn't possible. Storing the most granular fact preserves all analytical optionality.

---

## ✅ Data Quality Tests

**16 dbt tests** run after every transform:

| Test type                         | Coverage |
|-----------------------------------|----------|
| **Uniqueness**                    | `dim_customers.customer_id`, `dim_products.product_id` |
| **Grain integrity**               | `fct_orders` is unique on `(invoice_id, product_id)` (`dbt_utils.unique_combination_of_columns`) |
| **Foreign key integrity**         | every `customer_id` and `product_id` in `fct_orders` exists in its dimension (relationships test) |
| **Not-null**                      | primary keys, foreign keys, and key measures across all layers |

---

## ⚙️ Orchestration

Airflow DAG `online_retail_pipeline` runs **daily**, with three tasks chained sequentially:

```
extract_to_s3  ──▶  dbt_run  ──▶  dbt_test
```

- If any task fails, downstream tasks are skipped and the DAG fails.
- Failed tasks **retry once after 5 minutes** before final failure.
- The DAG file lives in Airflow's venv but uses `BashOperator` to **shell out** to dbt's separate venv — Airflow and dbt have conflicting Python dependencies, so they're isolated. This is a standard production pattern.

---

## 📁 Repo Structure

```
snowflake-de-project/
├── airflow/
│   └── dags/
│       └── online_retail_pipeline.py    # Airflow DAG
├── extract/
│   └── extract_to_s3.py                 # RDS → S3 extract
├── online_retail/                       # dbt project
│   ├── dbt_project.yml
│   ├── packages.yml                     # dbt_utils dependency
│   ├── macros/
│   │   └── generate_schema_name.sql     # custom schema naming
│   └── models/
│       ├── staging/
│       │   ├── stg_online_retail.sql
│       │   ├── sources.yml
│       │   └── schema.yml
│       └── warehouse/
│           ├── dim_customers.sql
│           ├── dim_products.sql
│           ├── fct_orders.sql
│           └── schema.yml
├── .gitignore
└── README.md
```

---

## 🚀 Running Locally

### Prerequisites
- AWS account with S3 bucket and RDS PostgreSQL instance
- Snowflake account with database and IAM-role-based S3 integration
- Python 3.12, Ubuntu / WSL2

### Setup
1. **Clone the repo**
2. **Create two venvs** — one for dbt + extract, one for Airflow (conflicting dependencies)
3. **Populate `.env`** at the project root with credentials (see `.env.example`)
4. **Configure `~/.dbt/profiles.yml`** with Snowflake connection details
5. **Initialize Airflow** with `AIRFLOW_HOME` pointed at `airflow/`
6. **Trigger the DAG** from the Airflow UI

### Manual run (without Airflow)
```bash
# Extract
python extract/extract_to_s3.py

# Snowflake load (run in Snowflake worksheet)
COPY INTO RAW.ONLINE_RETAIL FROM @online_retail_stage/online_retail.csv;

# Transform + test
cd online_retail
dbt run
dbt test
```

---

## 💡 Design Decisions

**Why not Glue / Spark?**
Source dataset is 48 MB — pandas handles it in-memory in seconds. Spark/Glue would add complexity for no benefit. Tool selection should match data scale.

**Why dbt for transforms (not Python)?**
Modern data stack convention is ELT — load raw into the warehouse, transform in SQL with dbt. Cheaper, faster, more testable than processing in Python and loading the result.

**Why Snowflake storage integrations (not inline credentials)?**
Centralizes auth, allows credential rotation in one place, separates account-admin work from analyst work via least privilege.

**Why two separate venvs?**
Airflow and dbt-core have incompatible Python dependency pins. Airflow runs in `airflow_venv`, dbt runs in `venv`, and the DAG shells out between them via `BashOperator`. Standard production pattern.

**Why `ACCOUNTADMIN` in dev?**
Portfolio convenience. In production, dbt would run under a least-privilege role (`TRANSFORMER`) with `USAGE` on the database/warehouse and `CREATE` on specific schemas only.

---
