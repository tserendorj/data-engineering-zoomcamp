{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
      from {{ source('staging','fhv_tripdata') }}
)
select

    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,
    
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("string")) }} as passenger_count,
    {{ dbt.safe_cast("affilated_base_number", api.Column.translate_type("string")) }} as passenger_count,
    
from tripdata


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}