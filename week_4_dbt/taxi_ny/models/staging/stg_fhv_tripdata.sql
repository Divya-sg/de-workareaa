{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('staging','external_fhv_tripdata_partitoned_clustered') }}
)
select
    -- identifiers

    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag, dispatching_base_num
    
from tripdata
where DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31'

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}