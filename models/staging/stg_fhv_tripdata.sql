{{ config(materialized='view') }}
/*with tripdata as
(
 select *,
 row_number() over(
     partition by 
     dispatching_base_num,
     pickup_datetime
     order by
     dropoff_datetime) as rn
 from {{ source('staging', 'fhv_tripdata') }}
 where dispatching_base_num is not null
)*/
select 
-- identifiers
    {{dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime'])}} as tripid,
    cast(dispatching_base_num as string) as vendor_id,
    cast(PULocationID as integer) as pickup_locationid,			
    cast(DOLocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,			
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(SR_Flag as string) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
    
--from tripdata	
--where rn = 1
from {{ source('staging', 'fhv_tripdata') }}
--where dispatching_base_num is not null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}