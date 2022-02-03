select "VendorID", yellow_taxi_data.tpep_pickup_datetime, yellow_taxi_data.tpep_dropoff_datetime, 
"PULocationID", "DOLocationID", fare_amount,passenger_count, payment_type
from yellow_taxi_data where
(tpep_pickup_datetime between '2021-01-15 00:00:00' and '2021-01-15 23:59:59')
order by passenger_count desc
-- 53024

select date(tpep_pickup_datetime), max(tip_amount) 
from yellow_taxi_data where
(tpep_pickup_datetime between '2021-01-01 00:00:00' and '2021-01-31 23:59:59')

group by date(tpep_pickup_datetime)
order by max(tip_amount) desc
--2021-01-20

select yellow_taxi_data."PULocationID", yellow_taxi_data."DOLocationID", zones."Zone", count(*) as destination 
from yellow_taxi_data 
join zones on yellow_taxi_data."DOLocationID" = zones."LocationID"

where "PULocationID" in (select "LocationID" from zones where lower(zones."Zone") like '%central park%')
and
(tpep_pickup_datetime between '2021-01-14 00:00:00' and '2021-01-14 23:59:59')
group by "PULocationID", "DOLocationID", zones."Zone"
order by destination desc
-- Upper East Side South -97

select "PULocationID", "DOLocationID", count(*) as num_of_trips, avg(total_amount)
from yellow_taxi_data
group by "PULocationID", "DOLocationID" 
order by avg(total_amount) desc
-- Alphabet City/Unknown



