from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
	dag_id="product_stock_alerts",
	start_date=datetime(2026, 1, 1),
	schedule="*/5 * * * *", # 5 minutes
	catchup=False,
) as dag:

	product_stock_alerts = SQLExecuteQueryOperator(
		task_id="product_stock_alerts",
		conn_id="ecommerce_db",
		sql="sql/aggregate_product_stock_alerts.sql"
	)