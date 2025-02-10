with

source as (

    select * from {{ source('stripe', 'raw_stripe_payments') }}

),

transformed as (

    select 

        id as payment_id,
        orderid as order_id,
        status as payment_status,
        paymentmethod as payment_method,
        -- amount is stored in cents, convert it to dollars
        round(amount / 100.0,2) as payment_amount,
        created as created_at
    from source

)

select * from transformed