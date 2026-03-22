-- ==========================================
-- INTERMEDIATE: Order Details (Enriched)
-- Orders + Customers + Products joined
-- ==========================================

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched as (
    select
        -- Order info
        o.order_id,
        o.order_date,
        o.order_year,
        o.order_month,
        o.order_year_month,
        o.order_day_of_week,
        o.order_status,
        o.is_cancelled,
        o.is_returned,
        o.is_delivered,

        -- Customer info
        o.customer_id,
        c.full_name              as customer_name,
        c.email                  as customer_email,
        c.city                   as customer_city,
        c.state                  as customer_state,
        c.gender                 as customer_gender,
        c.age                    as customer_age,
        c.is_active              as customer_is_active,

        -- Product info
        o.product_id,
        p.product_name,
        p.category               as product_category,
        p.sub_category           as product_sub_category,
        p.brand                  as product_brand,
        p.price_tier             as product_price_tier,

        -- Financial info
        o.quantity,
        o.unit_price,
        o.gross_amount,
        o.discount_percent,
        o.discount_amount,
        o.net_amount,
        p.cost_price,
        round(p.cost_price * o.quantity, 2) as total_cost,
        round(o.net_amount - (p.cost_price * o.quantity), 2) as profit,

        -- Payment & Shipping
        o.payment_method,
        o.shipping_city,
        o.shipping_state,
        o.delivery_date,
        o.delivery_days

    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p  on o.product_id  = p.product_id
)

select * from enriched