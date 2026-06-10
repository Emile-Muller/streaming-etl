import random
from datetime import datetime, timedelta

from psycopg2.extensions import connection as PgConnection, cursor as PgCursor
from psycopg2.extras import execute_values

def weighted_choice(weight_map: dict[str, int]) -> str:
	"""Return a single randomly sampled key from a weighted mapping."""
	options = list(weight_map.keys())
	weights = list(weight_map.values())
	
	return random.choices(options, weights=weights, k=1)[0]

def random_date_skewed(start: datetime, end: datetime) -> str:
	"""
	Return a random date between ``start`` and ``end``, biased toward
	more recent dates to simulate an organic growth curve.
	
	The bias is achieved by drawing two uniform samples over the date
	range and keeping the larger one, which shifts the distribution
	toward the upper end of the interval.
	"""
	total_days: int = (end - start).days
	day_offset: int = max(random.randint(0, total_days), random.randint(0, total_days))
	return (start + timedelta(days=day_offset)).strftime("%Y-%m-%d")
	
COUNTRY_WEIGHTS: dict[str, int] = {
	"US": 40, "UK": 15, "CA": 12, "AU": 10,
	"DE":  8, "FR":  7, "JP":  5, "BR":  3,
}

AGE_WEIGHTS: dict[str, int] = {
	"18-24": 15, "25-34": 32, "35-44": 28, "45-54": 16, "55+": 9,
}

TIER_WEIGHTS: dict[str, int] = {"free": 90, "premium": 10}

REGION_WEIGHTS: dict[str, dict[str, int]] = {
	"US": {"US-Northeast": 28, "US-Southeast": 25, "US-Midwest": 20, "US-Southwest": 12, "US-West": 15},
	"UK": {"England": 84, "Scotland": 8, "Wales": 5, "Northern Ireland": 3},
	"CA": {"Ontario": 40, "Quebec": 25, "British Columbia": 20, "Alberta": 12, "CA-Other": 3},
	"AU": {"New South Wales": 40, "Victoria": 32, "Queensland": 20, "AU-Other": 8},
	"DE": {"Bavaria": 28, "North Rhine-Westphalia": 38, "Baden-Württemberg": 22, "DE-Other": 12},
	"FR": {"Île-de-France": 40, "Auvergne-Rhône-Alpes": 20, "Provence-Alpes-Côte d'Azur": 15, "FR-Other": 25},
	"JP": {"Kantō": 55, "Kansai": 25, "Chūbu": 12, "JP-Other": 8},
	"BR": {"BR-Southeast": 55, "BR-Northeast": 25, "BR-South": 15, "BR-Other": 5},
}

DEVICE_WEIGHTS_BY_AGE: dict[str, dict[str, int]] = {
	"18-24": {"mobile": 78, "desktop": 18, "tablet": 4},
	"25-34": {"mobile": 70, "desktop": 25, "tablet": 5},
	"35-44": {"mobile": 60, "desktop": 33, "tablet": 7},
	"45-54": {"mobile": 48, "desktop": 45, "tablet": 7},
	"55+":   {"mobile": 35, "desktop": 58, "tablet": 7},
}
	
def generate_user_datatables(conn: PgConnection, num_users: int) -> None:
	"""
	Fill all user-related dimension and filter tables with synthetic data.
	
	Generates ``num_users`` randomized user records using the module-level
	weight tables and populates the following tables, skipping gracefully
	if ``dim.users`` is already populated:
	
		- ``dim.users``: user dimension with demographic attributes
		- ``filter.user_countries``: distinct countries for dashboard slicers
		- ``filter.user_regions``: distinct regions for dashboard slicers
		
	All writes use ``ON CONFLICT ... DO NOTHING`` so the function is safe to call
	repeatedly without producing duplicates.
	"""
	cur: PgCursor = conn.cursor()
	try:
		cur.execute("SELECT COUNT(*) FROM filter.user_regions")
		if cur.fetchone()[0] > 0:
			print("filter.user_regions already populated, skipping user generation.")
			cur.close()
			return
	 
		signup_start = datetime(2024, 1, 1)
		signup_end = datetime(2026, 1, 1)

		users: list[tuple] = []
		for i in range(num_users):
			country = weighted_choice(COUNTRY_WEIGHTS)
			age_group = weighted_choice(AGE_WEIGHTS)
			account_tier = weighted_choice(TIER_WEIGHTS)
			
			device_type = weighted_choice(DEVICE_WEIGHTS_BY_AGE[age_group])
			region = weighted_choice(REGION_WEIGHTS[country])
			
			users.append((
				f"user_{i:05d}",
				random_date_skewed(signup_start, signup_end),
				country,
				region,
				age_group,
				random.choice(["M", "F"]),
				account_tier,
				device_type,
			))
			
		execute_values(cur, """
			INSERT INTO dim.users (
				user_id, signup_date, country, region,
				age_group, gender, account_tier, device_type
			) VALUES %s
			ON CONFLICT (user_id) DO NOTHING
		""", users)
		print(f"dim.users populated ({num_users} users).")
		
		for table, col, idx in (
			("filter.user_countries", "country", 2),
			("filter.user_regions", "region", 3),
		):
			values = list({(u[idx],) for u in users})
			execute_values(cur, f"""
				INSERT INTO {table} ({col})
				VALUES %s
				ON CONFLICT ({col}) DO NOTHING
			""", values)
			print(f"{table} populated ({len(values)} {col}s).")
		
	finally:
		cur.close()