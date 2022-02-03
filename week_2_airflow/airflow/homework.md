## Week 2 Homework

In this homework, we'll prepare data for the next week. We'll need
to put these datasets to our data lake:

* Yellow taxi data for 2019 and 2020. We'll use them for Week #3 lessons
* FHV data (for-hire vehicles) for 2019 - for Week #3 homework

You can find all the URLs on [the dataset page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


In this homework, we will:

* Modify the DAG we created during the lessons for transferring the yellow taxi data
* Create a new dag for transferring the FHV data
* Create another dag for the Zones data




## Question 1: Start date for the Yellow taxi data (1 point)

You'll need to parametrize the DAG for processing the yellow taxi data that
we created in the videos. 

What should be the start date for this dag?

* 2019-01-01
* 2020-01-01
* 2021-01-01
* days_ago(1)

### Answer 1: 2019-01-01

## Question 2: Frequency for the Yellow taxi data (1 point)

How often do we need to run this DAG?

* Daily
* Monthly
* Yearly
* Once

### Answer 2: Monthly

For the yellow taxi data, Created the DAG [yellow_taxi_data_ingestion_gcs_dag_v01] [dags/yellow_taxi_data_ingestion_gcs_dag_v01.py]

## Question 3: DAG for FHV Data (2 points)

Now create another DAG - for uploading the FHV data. 

We will need three steps: 

* Download the data
* Parquetize it 
* Upload to GCS

If you don't have a GCP account, for local ingestion you'll need two steps:

* Download the data
* Ingest to Postgres

Use the same frequency and the start date as for the yellow taxi dataset

Question: how many DAG runs are green for data in 2019 after finishing everything? 

Note: when processing the data for 2020-01 you probably will get an error. It's up 
to you to decide what to do with it - for Week 3 homework we won't need 2020 data.

### Answer 3:  12 DAG runs are green
Created the DAG [fhv_data_ingestion_gcs_dag_v01] [dags/fhv_data_ingestion_gcs_dag_v01.py]
Got the parsing error for file 2020-01

## Question 4: DAG for Zones (2 points)


Create the final DAG - for Zones:

* Download it
* Parquetize 
* Upload to GCS

(Or two steps for local ingestion: download -> ingest to postgres)

How often does it need to run?

* Daily
* Monthly
* Yearly
* Once

### Answer 4: Once
Created the DAG [zone_data_ingestion_gcs_dag] [dags/zone_data_ingestion_gcs_dag.py]