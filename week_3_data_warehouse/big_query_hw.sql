--Question 1:
--What is count for fhv vehicles data for year 2019
--Can load the data for cloud storage and run a count(*)
-- Answer: 42084899

CREATE OR REPLACE EXTERNAL TABLE `xenon-axe-338500.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_xenon-axe-338500/parquet/fhv_tripdata_2019-*.parquet']
);

SELECT count(*) FROM `xenon-axe-338500.trips_data_all.external_fhv_tripdata`


--Question 2:
--How many distinct dispatching_base_num we have in fhv for 2019
--Can run a distinct query on the table from question 1
-- Answer : 792

select count(distinct dispatching_base_num) from `xenon-axe-338500.trips_data_all.external_fhv_tripdata`

--Question 3:
--Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
--Review partitioning and clustering video.
--We need to think what will be the most optimal strategy to improve query performance and reduce cost.
-- Answer: Partion by DATE(dropoff_datetime) and cluster by dispatching_base_num


--Question 4:
--What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
--Create a table with optimized clustering and partitioning, and run a count(*). Estimated data processed can be found in top right corner and actual data processed can be found after the query is executed.
-- Answer: 26647, Estimated : 400.1MB, Actual: 139.1 MB

CREATE OR REPLACE TABLE xenon-axe-338500.trips_data_all.external_fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM xenon-axe-338500.trips_data_all.external_fhv_tripdata;
-- Executed in 31.2 secs 1.6 GB

select count(*) from xenon-axe-338500.trips_data_all.external_fhv_tripdata_partitoned_clustered
where DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
and dispatching_base_num in ('B00987', 'B02060', 'B02279')
-- Estimated : 400.1MB
-- Actual: 139.1 MB


--Question 5:
--What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
--Review partitioning and clustering video. Clustering cannot be created on all data types.
-- Answer: Strategy - partion only on dispatching_base_num as SR_flag is majority null value although it has less cardinality.

--5383635 (not null)
-- 36701264 (null)


Question 6:
--What improvements can be seen by partitioning and clustering for data size less than 1 GB
--Partitioning and clustering also creates extra metadata.
--Before query execution this metadata needs to be processed.
--Answer: Clustering is prefered for data size less than 1GB to reduce the amount of metadata created thru the process of partioning
-- The improvements seens by clusering data would be faster query run-time and reduced cost.


--(Not required) Question 7:
--In which format does BigQuery save data
--Review big query internals video.
--Answer: Columnar format