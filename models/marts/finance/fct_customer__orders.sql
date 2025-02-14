-- with statement
-- import CTEs
-- logical CTEs
-- final CTE
-- simple select statement

with

-- Import CTEs
customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (

    select * from {{ ref('int_orders') }}

),

customer_order_history as (

    select 

        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,

        min(orders.order_date) as first_order_date,

        min(orders.order_date) as first_non_returned_order_date,

        max(orders.order_date) as most_recent_non_returned_order_date,

        coalesce(max(user_order_seq),0) as order_count,

        coalesce(count(case 
            when orders.order_date is not null
            then 1 
            end),0) as non_returned_order_count,

        sum(case 
            when orders.order_date is not null
            then orders.order_value_dollars else 0 
        end) as total_lifetime_value,

        sum(case 
            when orders.order_date is not null
            then orders.order_value_dollars else 0 
        end)
        / nullif(count(case 
            when orders.order_date is not null
            then 1 
        end),0) as avg_non_returned_order_value,

        array_agg(distinct orders.order_id) as order_ids

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    group by customers.customer_id,
    customers.full_name,
    customers.surname,
    customers.givenname
),

-- Final CTEs 
final as (

    select 

        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        order_value_dollars,
        orders.order_status,

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    join customer_order_history
    on orders.customer_id = customer_order_history.customer_id

)

-- Simple Select Statement
select * from final