SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

SELECT 'CREATE DATABASE ecommerce'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'ecommerce'
)\gexec