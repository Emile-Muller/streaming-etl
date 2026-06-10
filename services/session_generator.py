import math
import random
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

def inject_noise(event: dict) -> dict:
	"""
	Randomly corrupt an event to simulate real-world data quality issues.
	
	Two independent noise types are applied at a combined 10% rate:
	
		- 5%: ``event_type`` is replaced with ``"invalid_event"``.
		- 5%: ``timestamp`` is backdated by 1-10 minutes to simulate an out-of-order message.
	"""
	r: float = random.random()

	if r < 0.05:
		event["event_type"] = "invalid_event"
		
	elif r < 0.10:
		event["timestamp"] = (
			datetime.utcnow() - timedelta(minutes=random.randint(1, 10))
		).isoformat()

	return event
	
def pick_user(df_user: pd.DataFrame) -> str:
	"""
	Sample a user ID weighted by signup recency.
	
	Users who signed up more recently are assigned a higher sampling
	weight via an inverse log-decay on days-since-signup, reflecting
	the assumption that newer users are more likely to be active.
	"""
	signup_dates = pd.to_datetime(df_user["signup_date"])
	days_since_signup = (pd.Timestamp.now() - signup_dates).dt.days
	weight = 1 / (1 + np.log1p(days_since_signup))

	weight = weight / weight.sum()

	return df_user["user_id"].sample(weights=weight).iloc[0]

def generate_user_session(df_product: pd.DataFrame, df_user: pd.DataFrame) -> list[dict]:
	"""
	Simulate a single e-commerce user session and return its event stream.
	
	A session consists of:
		1. Browsing: 1–4 products are viewed, sampled by ``conversion_score``.
		2. Window-shopping exit (20% chance): session ends after browsing.
		3. Conversion attempt: on the highest-scoring browsed product via
		:func:`try_convert` (add-to-cart → optional remove → purchase).
		4. Related-product browsing (30% chance): up to 3 products from the
		same brand/category are viewed, with a further 20% chance of a second
		conversion attempt.
		
	Each raw event is passed through :func:`inject_noise` before being
    returned, introducing realistic data-quality issues.
	"""
	if df_product.empty:
		raise ValueError("Product dataframe cannot be empty.")
	if df_user.empty:
		raise ValueError("Users dataframe cannot be empty.")
		
	session_id: str = str(uuid.uuid4())
	user_id: str = pick_user(df_user)

	events: list[dict] = []
	
	def finalize() -> list[dict]:
		"""Apply noise to every event and return the cleaned list."""
		return [e for e in (inject_noise(event) for event in events) if e is not None]
		
	def time_format(ts: datetime) -> str:
		"""Format a datetime as an ISO 8601 string with a UTC ``Z`` suffix."""
		return ts.isoformat() + "Z"
		
	def base_event(product: pd.Series, event_type: str, t: datetime) -> dict:
		"""Build a minimal event dict shared by all event types."""
		return {
			"session_id": session_id,
			"event_id": str(uuid.uuid4()),
			"user_id": user_id,
			"product_id": product["product_id"],
			"event_type": event_type,
			"timestamp": time_format(t)
	}
	
	def generate_random_quantity(price: float, max_quantity: int = 10) -> int:
		"""
		Generate a purchase quantity inversely influenced by unit price.
		
		A logarithmic decay on ``price`` is used to compute a mean for
		a clamped uniform draw, so higher-priced items are less likely
		to be bought in bulk.
		"""
		price_factor: float = 1 / (1 + math.log1p(price))
		mean_quantity: float = 1 + (max_quantity - 1) * price_factor

		quantity: int = random.randint(1, max(1, round(mean_quantity)))
		return min(quantity, max_quantity)
	
	def purchase_event(product: pd.Series, t: datetime) -> dict:
		"""Build a purchase event dict enriched with pricing and quantity."""
		price_unit = product["price_current"]
		quantity = generate_random_quantity(price_unit)
		
		return {
			**base_event(product, "purchase", t),
			"price_unit": float(price_unit),
			"discount_pct": float(product["discount_pct"]),
			"quantity": int(quantity),
			"price_total": round(float(price_unit * quantity), 2)
		}
	
	def try_convert(product: pd.Series, t: datetime) -> datetime:
		"""
		Attempt to convert a product view into add-to-cart and purchase events.
		
		Conversion probabilities scale with ``conversion_score``:
		
			- add_to_cart: 10% – 50%
			- remove_from_cart: 30% – 10% (inverse of score)
			- purchase: 20% – 70%
			
		Out-of-stock products are skipped immediately.		
		"""
		score: float = product["conversion_score"]

		if product["availability"] == "out_of_stock":
			return t
	
		if random.random() >= (0.1 + 0.4 * score):
			return t
		# Chance to add_to_cart: 10% - 50% (based on score)
		t += timedelta(seconds=random.uniform(5, 30))
		events.append(base_event(product, "add_to_cart", t))

		if random.random() < (0.3 - 0.2 * score):
			# Chance to remove_from_cart: 30% - 10% (based on score)
			t += timedelta(seconds=random.uniform(20, 60))
			events.append(base_event(product, "remove_from_cart", t))
			return t

		if random.random() >= (0.2 + 0.5 * score):
			return t
		# Chance to purchase: 20% - 70% (based on score)
		t += timedelta(seconds=random.uniform(5, 60))
		events.append(purchase_event(product, t))
		return t
	
	def related_products(product: pd.Series, n: int = 3) -> pd.DataFrame | None:
		"""Sample up to ``n`` products from the same brand or category."""
		same_brand = df_product["brand"] == product["brand"]
		same_category = df_product["category"] == product["category"]
		is_different = df_product["product_id"] != product["product_id"]

		pool = df_product[is_different & (same_brand | same_category)]

		if pool.empty:
			return None

		n = min(n, len(pool))
		return pool.sample(n, weights="conversion_score")

	t: datetime = datetime.utcnow()
	
	browsing_count: int = random.randint(1, 4)
	browsed: pd.DataFrame = df_product.sample(browsing_count, weights="conversion_score")
	
	for _, p in browsed.iterrows():
		events.append(base_event(p, "view_product", t))
		t += timedelta(seconds=random.uniform(10, 60))
		
	# Chance of pure window shopping: 20%, no action taken
	if random.random() < 0.2:
		return finalize()
		
	# Pick the browsed product to act on, higher score = higher chance to be picked
	product = browsed.sample(1, weights="conversion_score").iloc[0]
	t = try_convert(product, t)
	
	# Chance to browse related products after adding to cart: 30%
	if random.random() < 0.3:
		related = related_products(product)
		if related is not None:
			for _, p in related.iterrows():
				events.append(base_event(p, "view_product", t))
				t += timedelta(seconds=random.uniform(10, 45))

			# Chance to act on a related product too: 20%
			if random.random() < 0.2:
				related_pick = related.sample(1, weights="conversion_score").iloc[0]
				t = try_convert(related_pick, t)

	return finalize()