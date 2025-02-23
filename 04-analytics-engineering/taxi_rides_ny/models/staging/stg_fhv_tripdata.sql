{{
    config(
        materialized='view'
    )
}}

select

    dispatching_base_num,    
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    cast(PULocationID as integer ) as pickup_locationid,
    cast(DOLocationID as integer )  as dropoff_locationid,

    
    sr_flag
    
from {{ source('staging','fhv_tripdata') }}


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}