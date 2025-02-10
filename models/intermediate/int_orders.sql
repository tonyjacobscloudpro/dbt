with

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'

),

order_totals as (

    select order_Id,
        payment_status,
        sum(payment_amount) as order_value_dollars   

    from payments
    group by order_Id,
        payment_status

),

order_values_joined as (

    select orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars

    from orders
    left join order_totals 
        on orders.order_Id = order_totals.order_Id

)

select * from order_values_joined

