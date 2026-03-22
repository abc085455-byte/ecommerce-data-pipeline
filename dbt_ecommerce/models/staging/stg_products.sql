-- ==========================================
-- STAGING: Products
-- Source: RAW.PRODUCTS
-- Fix: VARCHAR to proper types
-- ==========================================

with source as (
    select * from {{ source('raw', 'products') }}
),

cleaned as (
    select
        -- Primary Key
        product_id,

        -- Product info
        initcap(trim(product_name))         as product_name,
        initcap(trim(category))             as category,
        initcap(trim(sub_category))         as sub_category,
        initcap(trim(brand))                as brand,

        -- Pricing - VARCHAR to NUMBER
        try_to_double(price)                as selling_price,
        try_to_double(cost_price)           as cost_price,
        round(try_to_double(price) - try_to_double(cost_price), 2)
                                            as profit_per_unit,
        round(
            ((try_to_double(price) - try_to_double(cost_price))
            / nullif(try_to_double(price), 0)) * 100, 2
        )                                   as profit_margin_pct,

        -- Product details
        coalesce(try_to_double(weight_kg), 0) as weight_kg,
        case
            when lower(trim(is_available)) = 'true' then true
            else false
        end                                 as is_available,
        try_to_date(created_date, 'YYYY-MM-DD') as created_date,

        -- Price tier
        case
            when try_to_double(price) >= 1000 then 'Premium'
            when try_to_double(price) >= 500  then 'High'
            when try_to_double(price) >= 100  then 'Medium'
            else 'Budget'
        end                                 as price_tier,

        -- Metadata
        loaded_at

    from source
    where product_id is not null
      and try_to_double(price) > 0
)

select * from cleaned