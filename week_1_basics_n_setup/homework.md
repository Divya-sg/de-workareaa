## Week 1 Homework

## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

# Answer 1: Got the version

## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output to the form

# Answer 2: Created the infrastructure using terraform


## Question 3. Count records 

How many taxi trips were there on January 15?

```bash
select "VendorID", yellow_taxi_data.tpep_pickup_datetime, yellow_taxi_data.tpep_dropoff_datetime, 
"PULocationID", "DOLocationID", fare_amount,passenger_count, payment_type
from yellow_taxi_data where
(tpep_pickup_datetime between '2021-01-15 00:00:00' and '2021-01-15 23:59:59')
order by passenger_count desc
```
# Answer: 53024

## Question 4. Average

Find the largest tip for each day. 
On which day it was the largest tip in January?

(note: it's not a typo, it's "tip", not "trip")

```bash
select date(tpep_pickup_datetime), max(tip_amount) 
from yellow_taxi_data where
(tpep_pickup_datetime between '2021-01-01 00:00:00' and '2021-01-31 23:59:59')
group by date(tpep_pickup_datetime)
order by max(tip_amount) desc
```
# Answer: 2021-01-20


## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Enter the district name (not id)

```bash
select yellow_taxi_data."PULocationID", yellow_taxi_data."DOLocationID", zones."Zone", count(*) as destination 
from yellow_taxi_data 
join zones on yellow_taxi_data."DOLocationID" = zones."LocationID"
where "PULocationID" in (select "LocationID" from zones where lower(zones."Zone") like '%central park%')
and (tpep_pickup_datetime between '2021-01-14 00:00:00' and '2021-01-14 23:59:59')
group by "PULocationID", "DOLocationID", zones."Zone"
order by destination desc
```
# Answer: Upper East Side South (97)


## Question 6. 

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

```bash
select "PULocationID", "DOLocationID", count(*) as num_of_trips, avg(total_amount)
from yellow_taxi_data
group by "PULocationID", "DOLocationID" 
order by avg(total_amount) desc
```
# Answer: Alphabet City/Unknown



