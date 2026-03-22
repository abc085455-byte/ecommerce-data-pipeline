-- ==========================================
-- MART: Daily Sales Fact Table
-- Daily revenue, orders, items summary
-- ==========================================

with order_details as (
    select * from {{ ref('int_order_details') }}
    where is_cancelled = false
),

daily_agg as (
    select
        order_date,
        count(distinct order_id)        as total_orders,
        count(distinct customer_id)     as unique_customers,
        sum(quantity)                   as total_items_sold,
        round(sum(net_amount), 2)       as total_revenue,
        round(sum(gross_amount), 2)     as gross_revenue,
        round(sum(discount_amount), 2)  as total_discounts,
        round(sum(profit), 2)           as total_profit,
        round(avg(net_amount), 2)       as avg_order_value,
        sum(case when is_delivered then 1 else 0 end) as delivered_count,
        sum(case when is_returned then 1 else 0 end)  as returned_count

    from order_details
    group by order_date
)

select
    *,
    dayname(order_date)                 as day_name,
    dayofweek(order_date)               as day_of_week,
    case
        when dayofweek(order_date) in (1, 7) then 'Weekend'
        else 'Weekday'
    end                                 as day_type,
    round(total_profit / nullif(total_revenue, 0) * 100, 2)
                                        as profit_margin_pct,
    current_timestamp()                 as created_at

from daily_agg
order by order_date