select id as customer_id,
        first_name,
        last_name
 from {{ source('jaffle_shop', 'raw_jaffle_shop_customers') }}