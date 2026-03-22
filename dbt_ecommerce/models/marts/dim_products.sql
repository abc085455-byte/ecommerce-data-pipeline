-- ==========================================
-- MART: Product Dimension Table
-- Product details + sales performance
-- ==========================================

with products as (
    select * from {{ ref('stg_products') }}
),

sales_stats as (
    select
        product_id,
        count(distinct order_id)        as total_orders,
        sum(quantity)                   as total_quantity_sold,
        round(sum(net_amount), 2)       as total_revenue,
        round(sum(profit), 2)           as total_profit,
        round(avg(net_amount), 2)       as avg_sale_value,
        count(distinct customer_id)     as unique_buyers,
        min(order_date)                 as first_sold_date,
        max(order_date)                 as last_sold_date,
        sum(case when is_returned then 1 else 0 end) as return_count

    from {{ ref('int_order_details') }}
    where is_cancelled = false
    group by product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.sub_category,
        p.brand,
        p.selling_price,
        p.cost_price,
        p.profit_per_unit,
        p.profit_margin_pct,
        p.weight_kg,
        p.price_tier,
        p.is_available,
        p.created_date,

        -- Sales metrics
        coalesce(s.total_orders, 0)         as total_orders,
        coalesce(s.total_quantity_sold, 0)   as total_quantity_sold,
        coalesce(s.total_revenue, 0)         as total_revenue,
        coalesce(s.total_profit, 0)          as total_profit,
        coalesce(s.avg_sale_value, 0)        as avg_sale_value,
        coalesce(s.unique_buyers, 0)         as unique_buyers,
        s.first_sold_date,
        s.last_sold_date,
        coalesce(s.return_count, 0)          as return_count,

        -- Product ranking by revenue
        dense_rank() over (
            order by coalesce(s.total_revenue, 0) desc
        )                                    as revenue_rank,

        -- Product ranking within category
        dense_rank() over (
            partition by p.category
            order by coalesce(s.total_revenue, 0) desc
        )                                    as category_revenue_rank,

        -- Return rate
        round(
            coalesce(s.return_count, 0) / nullif(s.total_orders, 0) * 100, 2
        )                                    as return_rate_pct,

        current_timestamp()                  as created_at

    from products p
    left join sales_stats s on p.product_id = s.product_id
)

select * from final