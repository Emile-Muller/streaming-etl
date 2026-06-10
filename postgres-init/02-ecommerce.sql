\connect ecommerce;

CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS filter;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS dim.users (
	user_id VARCHAR(20) PRIMARY KEY,
	signup_date DATE NOT NULL,
	country CHAR(2) NOT NULL,
	region VARCHAR(50) NOT NULL,
	age_group VARCHAR(10) NOT NULL,
	gender CHAR(1) NOT NULL,
	account_tier VARCHAR(10) NOT NULL,
	device_type VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.products (
	product_id VARCHAR(50) PRIMARY KEY,
	name TEXT NOT NULL,
	brand VARCHAR(50) NOT NULL,
	category VARCHAR(50) NOT NULL,
	marketplace VARCHAR(50) NOT NULL,
	price_current NUMERIC(10, 2) NOT NULL,
	price_original NUMERIC(10, 2) NOT NULL,
	currency CHAR(3) NOT NULL DEFAULT 'USD',
	discount_pct NUMERIC(5, 2) NOT NULL,
	availability VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.products_features (
	product_id VARCHAR(50) PRIMARY KEY REFERENCES dim.products(product_id),
	price_current NUMERIC(10, 2) NOT NULL,
	discount_pct NUMERIC(5, 2) DEFAULT 0,
	availability VARCHAR(20) NOT NULL,
	conversion_score NUMERIC(8, 5) NOT NULL,
	brand VARCHAR(50),
	category VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw.events (
	id SERIAL PRIMARY KEY,
	event_id UUID UNIQUE,
	user_id VARCHAR(20) REFERENCES dim.users(user_id),
	session_id UUID,
	product_id VARCHAR(50) REFERENCES dim.products(product_id),
	event_type VARCHAR(100),
	price_unit NUMERIC(10, 2),
	discount_pct NUMERIC(5, 2),
	quantity INTEGER,
	price_total NUMERIC(10, 2),
	timestamp TIMESTAMP,
	ingested_at TIMESTAMPTZ DEFAULT NOW(),
	raw_payload JSONB
);

CREATE TABLE IF NOT EXISTS filter.product_brands (
	brand VARCHAR(50) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS filter.product_categories (
	category VARCHAR(50) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS filter.product_marketplaces (
	marketplace VARCHAR(50) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS filter.time_windows (
	window_start TIMESTAMP PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS filter.user_countries (
	country CHAR(2) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS filter.user_regions (
	region CHAR(50) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS analytics.conversion_funnel (
	window_start TIMESTAMP,
	avg_view_to_cart_sec NUMERIC(10, 2),
	avg_cart_to_purchase_sec NUMERIC(10, 2),
	avg_view_to_purchase_sec NUMERIC(10, 2),
	avg_cart_to_delete_sec NUMERIC(10, 2),
	avg_view_to_cart_rate NUMERIC(8, 4),
	avg_cart_to_purchase_rate NUMERIC(8, 4),
	avg_view_to_purchase_rate NUMERIC(8, 4),
	avg_cart_to_delete_rate NUMERIC(8, 4),
	PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS analytics.discount_sales (
	window_start TIMESTAMP,
	discount_bucket VARCHAR(20),
	purchases INTEGER,
	total_quantity INTEGER,
	revenue NUMERIC(10, 2),
	avg_discount_pct NUMERIC(5, 2),
	PRIMARY KEY (window_start, discount_bucket)
);

CREATE TABLE IF NOT EXISTS analytics.overall_performance (
	window_start TIMESTAMP,
	views INTEGER,
	add_to_carts INTEGER,
	remove_from_carts INTEGER,
	purchases INTEGER,
	revenue NUMERIC(10, 2),
	avg_order_value NUMERIC(10, 2),
	PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS analytics.product_dim_performance (
	window_start TIMESTAMP,
	brand VARCHAR(50),
	category VARCHAR(50),
	marketplace VARCHAR(50),
	purchases INTEGER,
	total_quantity INTEGER,
	revenue NUMERIC(10, 2),
	avg_order_value NUMERIC(10, 2),
	PRIMARY KEY (window_start, brand, category, marketplace)
);

CREATE TABLE IF NOT EXISTS analytics.product_performance (
	window_start TIMESTAMP,
	product_id VARCHAR(50) REFERENCES dim.products(product_id),
	name TEXT,
	brand VARCHAR(50),
	category VARCHAR(50),
	marketplace VARCHAR(50),
	purchases INTEGER,
	total_quantity INTEGER,
	revenue NUMERIC(10, 2),
	avg_order_value NUMERIC(10, 2),
	PRIMARY KEY (window_start, product_id)
);

CREATE TABLE IF NOT EXISTS analytics.product_stock_alerts (
	window_start TIMESTAMP,
	product_id VARCHAR(50) REFERENCES dim.products(product_id),
	name TEXT,
	brand VARCHAR(50),
	category VARCHAR(50),
	marketplace VARCHAR(50),
	availability VARCHAR(20),
	views INTEGER,
	PRIMARY KEY (window_start, product_id)
);

CREATE TABLE IF NOT EXISTS analytics.user_geo_performance (
	window_start TIMESTAMP,
	country CHAR(2),
	region VARCHAR(50),
	purchases INTEGER,
	revenue NUMERIC(10, 2),
	avg_order_value NUMERIC(10, 2),
	PRIMARY KEY (window_start, country, region)
);

CREATE TABLE IF NOT EXISTS analytics.user_performance (
	window_start TIMESTAMP,
	user_id VARCHAR(20) REFERENCES dim.users(user_id),
	country CHAR(2),
	region VARCHAR(50),
	account_tier VARCHAR(10),
	device_type VARCHAR(10),
	age_group VARCHAR(10),
	gender VARCHAR(1),
	signup_date DATE,
	purchases INTEGER,
	total_quantity INTEGER,
	revenue NUMERIC(10, 2),
	avg_order_value NUMERIC(10, 2),
	session_count INTEGER,
	PRIMARY KEY (window_start, user_id)
);