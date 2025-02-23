{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
    where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit Card')
)
    select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month, 

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare

    from trips_data
    group by 1,2,3