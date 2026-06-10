from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
	dag_id="user_performance",
	start_date=datetime(2026, 1, 1),
	schedule="*/5 * * * *", # 5 minutes
	catchup=False,
) as dag:

	user_performance = SQLExecuteQueryOperator(
		task_id="user_performance",
		conn_id="ecommerce_db",
		sql="sql/aggregate_user_performance.sql"
	)