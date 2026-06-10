from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
	dag_id="discount_sales",
	start_date=datetime(2026, 1, 1),
	schedule="*/5 * * * *", # 5 minutes
	catchup=False,
) as dag:

	discount_sales = SQLExecuteQueryOperator(
		task_id="discount_sales",
		conn_id="ecommerce_db",
		sql="sql/aggregate_discount_sales.sql"
	)