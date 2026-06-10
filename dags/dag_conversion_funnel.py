from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
	dag_id="conversion_funnel",
	start_date=datetime(2026, 1, 1),
	schedule="*/5 * * * *", # 5 minutes
	catchup=False,
) as dag:

	conversion_funnel = SQLExecuteQueryOperator(
		task_id="conversion_funnel",
		conn_id="ecommerce_db",
		sql="sql/aggregate_conversion_funnel.sql"
	)