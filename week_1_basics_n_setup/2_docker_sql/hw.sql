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

docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v {pwd}/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5433:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13


docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin\
  dpage/pgadmin4


docker build -t taxi_ingest:v001 .

export URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"

docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=172.18.0.2 \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_trips \
        --url=${URL}


