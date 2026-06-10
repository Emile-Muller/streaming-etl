from pathlib import Path

import pandas as pd
import numpy as np
from psycopg2.extensions import connection as PgConnection, cursor as PgCursor
from psycopg2.extras import execute_values

def normalize(series: pd.Series) -> pd.Series:
	"""Normalize a pandas Series to the [0, 1] range using min-max scaling."""
	return round((series - series.min()) / (series.max() - series.min()), 5)
	
def compute_conversion_score(df: pd.DataFrame) -> pd.Series:
	"""Compute a weighted conversion score from pre-normalized product features."""
	return (
		0.4 * df["rating_norm"] +
		0.25 * df["discount_norm"] +
		0.2 * df["price_penalty"] +
		0.15 * df["popularity_norm"]
	).round(5)

def generate_product_datatables(conn: PgConnection) -> None:
	"""
	Fill all product-related dimension and filter tables from the raw CSV dataset.
	
	Reads ``data/ecommerce_dataset.csv``, filters to USD-denominated rows,
	calculates conversion-score features, and populates the following tables,
	skipping gracefully if they are already populated:
	
		- ``dim.products``: static product catalogue
		- ``dim.products_features``: features used for event generation
		- ``filter.product_brands``: distinct brands for dashboard slicers
		- ``filter.product_categories``: distinct categories for dashboard slicers
		- ``filter.product_marketplaces``: distinct marketplaces for dashboard slicers
		
	All writes use ``ON CONFLICT ... DO NOTHING`` so the function is safe to call
	repeatedly without producing duplicates.
	"""
	BASE_DIR: Path = Path(__file__).parent.parent / "data"
	dataset_path: Path = BASE_DIR / "ecommerce_dataset.csv"
	
	if not dataset_path.exists():
		print("Initial dataset missing, cannot populate clean datasets.")
		return

	cur: PgCursor = conn.cursor()
	try:
		cur.execute("SELECT COUNT(*) FROM filter.product_marketplaces")
		if cur.fetchone()[0] > 0:
			print("Product datasets already populated, skipping clean dataset population.")
			cur.close()
			return

		df: pd.DataFrame = pd.read_csv(dataset_path)
		df = df[df["currency"] == "USD"].reset_index(drop=True)
		
		df["name"] = df["title"]
		df["marketplace"] = df["source"]
		df["price_original"] = df["price_original"].fillna(df["price_current"])
		df["discount_pct"] = df["discount_pct"].fillna(0)
		df["reviews_count"] = df["reviews_count"].fillna(0)
		df["rating_score"] = df["rating_score"].fillna(df["rating_score"].median().round(3))
		
		df["rating_norm"] = normalize(df["rating_score"])
		df["discount_norm"] = normalize(df["discount_pct"])
		df["price_penalty"] = (1 - normalize(np.log1p(df["price_original"]))).round(5)
		df["popularity_norm"] = normalize(df["reviews_count"])
		
		df["conversion_score"] = compute_conversion_score(df)
		df["conversion_score"] = normalize(df["conversion_score"])
		
		dim_cols = [
			"product_id", "name", "brand", "category", "marketplace",
			"price_current", "price_original", "currency",
			"discount_pct", "availability"
		]
		execute_values(cur, """
			INSERT INTO dim.products (
				product_id, name, brand, category, marketplace,
				price_current, price_original, currency,
				discount_pct, availability
			) VALUES %s
			ON CONFLICT (product_id) DO NOTHING
		""", df[dim_cols].itertuples(index=False, name=None))
		print(f"dim.products populated ({len(df)} products).")
		
		features_cols = [
			"product_id", "brand", "category", "price_current",
			"discount_pct", "availability", "conversion_score"
		]
		execute_values(cur, """
			INSERT INTO dim.products_features (
				product_id, brand, category, price_current,
				discount_pct, availability, conversion_score
			) VALUES %s
			ON CONFLICT (product_id) DO NOTHING
		""", df[features_cols].itertuples(index=False, name=None))
		print(f"dim.products_features populated ({len(df)} products).")
		
		for table, col in (
			("filter.product_brands", "brand"),
			("filter.product_categories", "category"),
			("filter.product_marketplaces", "marketplace"),
		):
			execute_values(cur, f"""
				INSERT INTO {table} ({col})
				VALUES %s
				ON CONFLICT ({col}) DO NOTHING
			""", df[[col]].drop_duplicates().itertuples(index=False, name=None))
			print(f"{table} populated.")
		
	finally:
		cur.close()
	