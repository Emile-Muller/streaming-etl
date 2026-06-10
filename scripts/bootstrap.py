import os

import psycopg2
from psycopg2.extensions import connection as PgConnection
from generate_product_datatables import generate_product_datatables
from generate_user_datatables import generate_user_datatables

def _require_env(key: str) -> str:
	value = os.getenv(key)
	if value is None:
		raise RuntimeError(f"Required environment variable '{key}' is not set.")
	return value

NUM_USERS: int = int(_require_env("NUM_USERS"))

PG_CONFIG: dict[str, str | int] = {
	"host": _require_env("POSTGRES_HOST"),
	"port": int(_require_env("POSTGRES_PORT")),
	"database": _require_env("POSTGRES_DB"),
	"user": _require_env("POSTGRES_USER"),
	"password": _require_env("POSTGRES_PASSWORD"),
}
	
def bootstrap() -> None:
	"""
	Fill the database with dimension and filter tables.
	
	Runs the product and user datatable generators inside a single transaction.
	Commits on success; rolls back and re-raises on any error.
	"""
	conn: PgConnection = psycopg2.connect(**PG_CONFIG)
	try:
		generate_product_datatables(conn)
		generate_user_datatables(conn, NUM_USERS)
		conn.commit()
		print("Bootstrap completed successfully.")
	except Exception as e:
		conn.rollback()
		print(f"Bootstrap failed, all changes rolled back: {e}")
		raise
	finally:
		conn.close()
		
if __name__ == "__main__":
	bootstrap()