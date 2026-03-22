-- ==========================================
-- STAGING: Customers
-- Source: RAW.CUSTOMERS
-- Fix: VARCHAR to DATE cast
-- ==========================================

with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        -- Primary Key
        customer_id,

        -- Name fields
        initcap(trim(first_name))           as first_name,
        initcap(trim(last_name))            as last_name,
        initcap(trim(first_name)) || ' ' || initcap(trim(last_name))
                                            as full_name,

        -- Contact info
        lower(trim(email))                  as email,
        trim(phone)                         as phone,

        -- Address fields
        trim(address)                       as address,
        initcap(trim(city))                 as city,
        upper(trim(state))                  as state,
        trim(zip_code)                      as zip_code,
        coalesce(upper(trim(country)), 'US') as country,

        -- Demographics - VARCHAR to DATE convert
        try_to_date(date_of_birth, 'YYYY-MM-DD') as date_of_birth,
        datediff('year',
            try_to_date(date_of_birth, 'YYYY-MM-DD'),
            current_date()
        )                                   as age,
        coalesce(gender, 'Unknown')         as gender,

        -- Account info
        try_to_date(registration_date, 'YYYY-MM-DD') as registration_date,
        case
            when lower(trim(is_active)) = 'true' then true
            else false
        end                                 as is_active,

        -- Metadata
        loaded_at

    from source
    where customer_id is not null
      and email is not null
)

select * from cleaned