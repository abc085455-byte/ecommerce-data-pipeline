-- ==========================================
-- MART: Customer Dimension Table
-- Customer details + purchase analytics
-- ==========================================

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        count(distinct order_id)        as lifetime_orders,
        round(sum(net_amount), 2)       as lifetime_revenue,
        round(avg(net_amount), 2)       as avg_order_value,
        min(order_date)                 as first_order_date,
        max(order_date)                 as last_order_date,
        sum(quantity)                   as total_items_bought,
        count(distinct product_id)      as unique_products,
        sum(case when is_returned then 1 else 0 end) as total_returns

    from {{ ref('int_order_details') }}
    where is_cancelled = false
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        c.country,
        c.age,
        c.gender,
        c.registration_date,
        c.is_active,

        -- Purchase metrics
        coalesce(os.lifetime_orders, 0)     as lifetime_orders,
        coalesce(os.lifetime_revenue, 0)    as lifetime_revenue,
        coalesce(os.avg_order_value, 0)     as avg_order_value,
        os.first_order_date,
        os.last_order_date,
        coalesce(os.total_items_bought, 0)  as total_items_bought,
        coalesce(os.unique_products, 0)     as unique_products,
        coalesce(os.total_returns, 0)       as total_returns,

        -- Customer segment
        case
            when coalesce(os.lifetime_revenue, 0) >= 5000 then 'Premium'
            when coalesce(os.lifetime_revenue, 0) >= 2000 then 'Gold'
            when coalesce(os.lifetime_revenue, 0) >= 500  then 'Silver'
            when coalesce(os.lifetime_revenue, 0) > 0     then 'Bronze'
            else 'Inactive'
        end                                 as customer_segment,

        -- Recency
        datediff('day', os.last_order_date, current_date()) as days_since_last_order,

        -- Return rate
        round(
            coalesce(os.total_returns, 0) / nullif(os.lifetime_orders, 0) * 100, 2
        )                                   as return_rate_pct,

        current_timestamp()                 as created_at

    from customers c
    left join order_stats os on c.customer_id = os.customer_id
)

select * from final