-- Mart: dimensional customers view used by BI (Superset) and OLAP (StarRocks MVs)
with stg as (
    select * from {{ ref('stg_customers') }}
)
select
    id,
    name,
    email,
    city,
    cast(updated_at as timestamp) as updated_at,
    lower(split_part(email, '@', 2)) as email_domain
from stg
