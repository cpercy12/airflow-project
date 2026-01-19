from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Chemin racine du projet dans le container
PROJECT_PATH = "/opt/airflow"

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

    # --------------------
    # CLEANING
    # --------------------
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

    # --------------------
    # DIMENSIONS
    # --------------------
    load_dim_customer = BashOperator(
        task_id="load_dim_customer",
        bash_command=f"python {PROJECT_PATH}/scripts/warehouse/load_dim_customer.py"
    )

    load_dim_product = BashOperator(
        task_id="load_dim_product",
        bash_command=f"python {PROJECT_PATH}/scripts/warehouse/load_dim_product.py"
    )

    load_dim_date = BashOperator(
        task_id="load_dim_date",
        bash_command=f"python {PROJECT_PATH}/scripts/warehouse/load_dim_date.py"
    )

    # --------------------
    # FACT
    # --------------------
    load_fact_sales = BashOperator(
        task_id="load_fact_sales",
        bash_command=f"python {PROJECT_PATH}/scripts/warehouse/load_fact_sales.py"
    )

    # --------------------
    # DEPENDENCIES
    # --------------------
    clean_tasks = [
        clean_customers,
        clean_products,
        clean_salesorderheader,
        clean_salesorderdetail
    ]

    # CLEAN → DIMENSIONS
    clean_tasks >> [
        load_dim_customer,
        load_dim_product,
        load_dim_date
    ]

    # DIMENSIONS → FACT
    [
        load_dim_customer,
        load_dim_product,
        load_dim_date
    ] >> load_fact_sales
