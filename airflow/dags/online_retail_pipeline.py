from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# --- Paths ---
PROJECT_ROOT = "/home/sumaiyamahmud/snowflake-de-project"
DBT_VENV = f"{PROJECT_ROOT}/venv"
DBT_PROJECT = f"{PROJECT_ROOT}/online_retail"
EXTRACT_SCRIPT = f"{PROJECT_ROOT}/extract/extract_to_s3.py"

# --- Default args applied to every task ---
default_args = {
    "owner": "sumaiya",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
with DAG(
    dag_id="online_retail_pipeline",
    description="RDS -> S3 -> Snowflake -> dbt transforms + tests",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "snowflake", "dbt"],
) as dag:

    extract_to_s3 = BashOperator(
        task_id="extract_to_s3",
        bash_command=f"source {DBT_VENV}/bin/activate && python {EXTRACT_SCRIPT}",
    )

    # COPY INTO is run by Snowflake. We trigger it via dbt or snowsql.
    # For this portfolio, we treat it as a one-time setup, not a daily task.
    # If we wanted to re-run COPY INTO daily, we'd add it here.

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"source {DBT_VENV}/bin/activate && "
            f"cd {DBT_PROJECT} && dbt run"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"source {DBT_VENV}/bin/activate && "
            f"cd {DBT_PROJECT} && dbt test"
        ),
    )

    extract_to_s3 >> dbt_run >> dbt_test