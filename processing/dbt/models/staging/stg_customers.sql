-- Staging: current-state customers straight from the Iceberg silver layer
select
    id,
    name,
    email,
    city,
    updated_at,
    silver_processed_at
from lakehouse.silver.customers
