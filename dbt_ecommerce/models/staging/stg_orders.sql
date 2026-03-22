-- ==========================================
-- STAGING: Orders
-- Source: RAW.ORDERS
-- Fix: VARCHAR to DATE cast karna padta hai
-- ==========================================

with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        -- Keys
        order_id,
        customer_id,
        product_id,

        -- Date fields - VARCHAR ko DATE mein convert karo
        try_to_date(order_date, 'YYYY-MM-DD')      as order_date,
        try_to_date(delivery_date, 'YYYY-MM-DD')    as delivery_date,

        -- Delivery days calculation
        datediff('day',
            try_to_date(order_date, 'YYYY-MM-DD'),
            try_to_date(delivery_date, 'YYYY-MM-DD')
        )                                           as delivery_days,

        -- Order details
        try_to_number(quantity)                      as quantity,
        try_to_double(unit_price)                    as unit_price,
        round(try_to_double(unit_price) * try_to_number(quantity), 2)
                                                    as gross_amount,
        coalesce(try_to_double(discount_percent), 0) as discount_percent,
        coalesce(try_to_double(discount_amount), 0)  as discount_amount,
        try_to_double(final_amount)                  as net_amount,

        -- Order info
        initcap(trim(payment_method))               as payment_method,
        initcap(trim(order_status))                 as order_status,

        -- Shipping
        trim(shipping_address)                      as shipping_address,
        initcap(trim(shipping_city))                as shipping_city,
        upper(trim(shipping_state))                 as shipping_state,

        -- Flags
        case when trim(order_status) = 'Cancelled' then true else false end as is_cancelled,
        case when trim(order_status) = 'Returned'  then true else false end as is_returned,
        case when trim(order_status) = 'Delivered'  then true else false end as is_delivered,

        -- Time dimensions - DATE se extract karo
        year(try_to_date(order_date, 'YYYY-MM-DD'))         as order_year,
        month(try_to_date(order_date, 'YYYY-MM-DD'))        as order_month,
        dayofweek(try_to_date(order_date, 'YYYY-MM-DD'))    as order_day_of_week,
        to_char(try_to_date(order_date, 'YYYY-MM-DD'), 'YYYY-MM') as order_year_month,

        -- Metadata
        loaded_at

    from source
    where order_id is not null
      and customer_id is not null
      and product_id is not null
      and try_to_number(quantity) > 0
)

select * from cleaned