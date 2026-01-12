from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Chemin racine du projet dans le container
PROJECT_PATH = "/opt/airflow/project"

default_args = {
    "owner": "data-engineer",
    "retries": 1
}

with DAG(
    dag_id="adventureworks_pipeline",
    description="Pipeline Data Engineering AdventureWorks",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    clean_customers = BashOperator(
        task_id="clean_customers",
        bash_command=f"python {PROJECT_PATH}/scripts/transform/clean_customers.py"
    )

    clean_products = BashOperator(
        task_id="clean_products",
        bash_command=f"python {PROJECT_PATH}/scripts/transform/clean_products.py"
    )

    clean_salesorderheader = BashOperator(
        task_id="clean_salesorderheader",
        bash_command=f"python {PROJECT_PATH}/scripts/transform/clean_salesorderheader.py"
    )

    clean_salesorderdetail = BashOperator(
        task_id="clean_salesorderdetail",
        bash_command=f"python {PROJECT_PATH}/scripts/transform/clean_salesorderdetail.py"
    )

    build_fact_sales = BashOperator(
        task_id="build_fact_sales",
        bash_command=f"python {PROJECT_PATH}/scripts/mart/build_fact_sales.py"
    )

    # Dépendances
    [
        clean_customers,
        clean_products,
        clean_salesorderheader,
        clean_salesorderdetail
    ] >> build_fact_sales