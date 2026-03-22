-- ==========================================
-- MART: Monthly Revenue Fact Table
-- Monthly trends for revenue analysis
-- ==========================================

with order_details as (
    select * from {{ ref('int_order_details') }}
    where is_cancelled = false
),

monthly_agg as (
    select
        order_year,
        order_month,
        order_year_month,
        count(distinct order_id)        as total_orders,
        count(distinct customer_id)     as unique_customers,
        count(distinct product_id)      as unique_products_sold,
        sum(quantity)                   as total_items_sold,
        round(sum(net_amount), 2)       as total_revenue,
        round(sum(profit), 2)           as total_profit,
        round(sum(discount_amount), 2)  as total_discounts,
        round(avg(net_amount), 2)       as avg_order_value,
        round(sum(net_amount) / nullif(count(distinct customer_id), 0), 2)
                                        as revenue_per_customer

    from order_details
    group by order_year, order_month, order_year_month
),

with_growth as (
    select
        *,
        lag(total_revenue) over (order by order_year, order_month)
                                        as prev_month_revenue,
        round(
            (total_revenue - lag(total_revenue) over (order by order_year, order_month))
            / nullif(lag(total_revenue) over (order by order_year, order_month), 0)
            * 100, 2
        )                               as revenue_growth_pct,
        round(total_profit / nullif(total_revenue, 0) * 100, 2)
                                        as profit_margin_pct,
        current_timestamp()             as created_at

    from monthly_agg
)

select * from with_growth
order by order_year, order_month