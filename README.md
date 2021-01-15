# Constructing a ETL Pipeline with Airflow

## Overview
This is the capstone project of Udacity Data Engineer Nano Degree course. 
The goal of this project is to combine "Iowa Liquor Sales" dataset in Kaggle
with several datasets such as county demographic and weather data to provide 
a richer data warehouse that could potentially help analyzing the relationship between 
the type of liquor consumption and factors such as weather, population, poverty rate
and so on. We will build a data pipeline that extracts the datasets from variouse sources, 
stores them in Amazon S3, transforms the data, and loads the data to an Amazon Redshift cluster.

## Datasets to Combine

- [**Iowa Liquor Sales:**](https://www.kaggle.com/residentmario/iowa-liquor-sales)
	- contains over 12 million records of liquor sales in Iowa from 2012 through 2017 
	- format: csv 
- [**Daily Temperature of Major Cities:**](https://www.kaggle.com/sudalairajkumar/daily-temperature-of-major-cities)
	- contains temperature data of major cities in the world, which includes two major cities in Iowa; Des Moines and Sioux City 
	- format: json (This file was originally csv but converted to json for the sake of project requirement)
- [**US Census Demographic Data:**](https://www.kaggle.com/muonneutrino/us-census-demographic-data) 
	- contains census data such as total population,population percentage by sex, race, and age, unemployment rate etc.
	-- format: csv
- [**United States crime rates by county:**](https://www.kaggle.com/mikejohnsonjr/united-states-crime-rates-by-county) 
	- contain data regarding crimes by US county. We will use crime rate per 1000,000 data. 
	- -format: csv

	
## Technical Overview
This project uses the following technologies.
- Python
- Apache Airflow
- AWS Redshift
- AWS S3

## Usage
This project can be executed on either Apache Airflow or Python script basis, 
whichever you prefer.
Follow the steps described below:

First, you need to create an Amazon Redshift cluster and store the four datasets
in your Amazon S3 bucket. 

1. Create an Amazon Redshift cluster
2. Store the four datasets in your Amazon S3 bucket. 
3. Fill all the keys in a configuration file named `dwh.cfg`
4. Get Airflow web UI up and running by `$ airflow sebserver` and `$ airflow scheduler`
5. Update the Airflow database by `$ airflow initdb`
6. Run a DAG (Directed Acyclic Graph) named `etl_process` on the Airflow web UI
   
- Alternatively, if you want to run the process on script basis, replace the step 4 through 6 with
the following: 

4. `python create_tables.py` will create the staging and final tables in the Redshift cluster 
5. `python etl.py`  will stage the data in the Redshift cluster and insert rows to the final tables.  
6. `python check_tables.py` will checks the copy and insert were done properly with sinple queries.

## Airflow
The Airflow DAG is as follows. Basically, it loads data to the staging tables from 
four different sources separately and runs quality checks on each table. Once all the check are passed, 
it will insert data into the final tables separately and runs the quality check on those tables.

![Tree View](DAG.png)


## Data Schema

![Data Schema](ERD.png)

#### `sales_fact` table (Fact Table)
| Column | Type | Description |
| ------ | ---- | ----------- |
| `sales_id` | `INTEGER` |The sales id, main id for the table| 
| `date` | `DATE` | Shows when the purchase was made. References time_dim |
| `store_id` | `INTEGER` | The id number of the store where the liquors were sold|
| `brand_id` | `INTEGER` | The id number of the liquor brand|
| `item_id` | `INTEGER` | The id nuber of the item|
| `sold_count` | `INTEGER` | The number of the items sold |
| `volume_sold` | `INTEGER` | The total volume of the liquor in ml|
| `sales` | `NUMERIC(6,2)` | The amount (in dollars) sold|
|`county`|`VARCHAR(25)`|The county name of the store location|
|`city`|`VARCHAR(25)`|The city name of the store location|

#### `store_dim` table (Dimension Table)
| Column | Type | Description |
| ------ | ---- | ----------- |
|`store_id`|`INTEGER`|The id number of the store. The main id for the table|
|`store_name`|`VARCHAR(100)`| The name of the store|
|`address`|`VARCHAR(200)`|The address of the store|
|`city`|`VARCHAR(25)`|The city name of the store location|
|`zip_code`|`VARCHAR(5)`|The zip code of the address|
|`long`|`NUMERIC`|The longitude of the store location|
|`lat`|`NUMERIC`|The latitude of the store location|
|`county`|`VARCHAR(25)`|The county name of the store location|



#### `item_dim` table (Dimension Table)
| Column | Type | Description |
| ------ | ---- | ----------- |
| `item_id` | `INTEGER` | The id nuber of the item. The main id for the table|
| `brand_id` | `INTEGER` | The id number of the liquor brand|
|`item_description`|`VARCHAR(200)`|The description of the item|
|`brand_name`|`VARCHAR(100)`|The brand name of the item|
|`bottle_volume`|`INTEGER`|The bottle volume in ml|
|`state_bottle_cost`|`NUMERIC(6,2)`|The bottle cost in US dollars|
|`state_bottle_retail`|`NUMERIC(6,2)`|The bottle retail in US dollars|

#### `time_dim` table (Dimension Table)
| Column | Type | Description |
| ------ | ---- | ----------- |
|`date`|`DATE`|The timestamp, works as the id|
|`year`|`NUMERIC`|The year from the timestamp|
|`month`|`NUMERIC`|The month from the timestamp|
|`day`|`NUMERIC`|The day of the month from the timestamp|
|`weekday`|`NUMERIC`|The day of week from the timestamp|


#### `temperature_dim` table (Dimension Table)
| Column | Type | Description |
| ------ | ---- | ----------- |
|`date`|`DATE`|The date of the sales. One of the composite keys|
|`city`|`VARCHAR(25)`|The name of the city. One of the composite keys|
|`temperature`|`NUMERIC`|The average temperature of the day|

#### `county_census_dim` table (Dimension Table)
| Column | Type | Description |
| ------ | ---- | ----------- |
|`county`|`VARCHAR(25)`|The county name of the store location. The main id for this table.|
|`total_pop`|`INTEGER`|The total population of the county|
|`men`|`INTEGER`|The total male population of the county|
|`women`|`INTEGER`|The total female population of the county|
|`hispanic`|`NUMERIC`|The percentage of the hispanic people in the county|
|`white`|`NUMERIC`|The percentage of the white people in the county|
|`black`|`NUMERIC`|The percentage of the black people in the county|
|`native`|`NUMERIC`|The percentage of the native american people in the county|
|`asian`|`NUMERIC`|The percentage of the asian people in the county|
|`pacific`|`NUMERIC`|The percentage of the pacific islander people in the county|
|`voting_age_citizen`|`INTEGER`|The population in voting-age|
|`income`|`INTEGER`|The mean income|
|`income_per_cap`|`INTEGER`|The mean income per capita|
|`poverty`|`NUMERIC`|The poverty rate|
|`child_poverty`|`NUMERIC`|The child poverty rate|
|`unemployment`|`NUMERIC`|The unemployment rate|
|`crime_rate_per_100000`|`NUMERIC`|The crime rate per 100,000|


## Future Scenarios

#### Data increases 100x
Since most part of our project is carried out in AWS eco system, which is highly scalable, 
our project won't be significantly affected by data size. 
We may need to increase the number of nodes on Redshift, which can be easily done on the AWS dashboard. 
Another thing is to consider is that we may need to be able to upload the dataset to 
the Amazon S3 bucket directly from the source.   

#### The data populates a dashboard that must be updated on a daily basis by 7am every day
Apace Airflow can handle this kind of scheduled execution well. We need set `start_date` and
`schedule_interval` parameters in the dag definition properly.  

#### The database needed to be accessed by 100+ people
Amazon Redshift can handle 500 connections so 100+ shouldn't be a problem. However, since you are charged
by the amount of data you scaned, we need to pay closer attention to the usage cost.

