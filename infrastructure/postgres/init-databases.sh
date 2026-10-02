#!/bin/bash
set -e

# Create additional databases required by the platform.
# The default database (order_db) is created by POSTGRES_DB env var.
# This script creates inventory_db and stock_db for their respective services.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE inventory_db OWNER $POSTGRES_USER'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'inventory_db')\gexec

    SELECT 'CREATE DATABASE stock_db OWNER $POSTGRES_USER'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'stock_db')\gexec
EOSQL
