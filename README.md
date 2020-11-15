# Capstone Immigration project

## Project overview

The purpose of this project is to build a data warehouse for Immigration data of US. This data can be used to analyze the statistics or patterns of people entering US 
from various parts of the world based on Weather, ethnicities and the states that they are entring to.

The data warehouse is built by running a data pipeline using Airflow. The data from various sources is ingested, transformed using Spark and staged to Redshift which is
the data warehouse.

## Use-case and queries:

Below are some of the use cases for running this pipeline:
-To know if there is an impact of weather on people from their native countries to immigrate to USA. If there is any pattern where people are coming from more hotter or colder countries.
-To identify which is the busiest port of entry in US and also during what times these port entries are busiest.
3. To identify the races of people arriving in US and settling in different states of the US.

Below are some of the insights that can be obtained by running the below type of queries:
1. The port of entry with highest number of people for a particular month.
2. The median age of people coming from different countries across the world.
3. The count of number of people belonging to a particular race in a State.


## Database Model

This database model follows a star schema approcah with 1 fact table representing I94 transactions  and 4 dimension tables for Weather , States, AIrports   and Dates.

### Immigration Table (Fact) :

| Table Column | Data Type | Description |
| -------- | ------------- | --------- |
| cicid (PRIMARY_KEY) | CIC ID |
| i94yr | INTEGER | I94 Year |
| i94mo | INTEGER | I94 month |
| i94cit | INTEGER | Country of Citizenship |
| i94res | INTEGER | Country of residense |
| i94port | VARCHAR | Port of Entry |
| i94visa | INTEGER | Visa Type |
| i94mode | INTEGER | Mode of transport |
| i94bir | INTEGER | Birth year |
| arrdate | DATE | Arrival date |
| depdate | DATE | Departure Date |
| airline | VARCHAR | Airline |
| fltno   | VARCHAR | Flight number |
| visatype | VARCHAR | Visa Type |
| gender | VARCHAR | Gender |
| i94addr | VARCHAR | Address in US |


### Weather Table (Dimension):

| Table Column | Data Type | Description |
| -------- | ------------- | --------- |
| Country_code(Primary Key) | INTEGER | Country Code |
| Country | VARCHAR | Country name |
| TEMPERATURE | FLOAT | Temperature |
| LATITUDE | VARCHAR | Latitude |
| LONGITUDE | VARCHAR | Longitude |


### States Table (Dimension):

| Table Column | Data Type | Description |
| -------- | ------------- | --------- |
| State_code(Primary Key) | VARCHAR | State Code |
| Total_population | INTEGER | Total Population |
| Female_population | INTEGER | Female Count |
| Median_age | FLOAT | Average age |
| Male_population | INTEGER | Male counts |
| State | VARCHAR | STate name |
| AmericanIndian_and_AlaskaNative | INTEGER | Count |
| Asian | INTEGER | Count |
| Black_or_AfricanAmerican | INTEGER | Count |
| Hispanic_or_Latino | INTEGER | Count |
| White | INTEGER | Count |


### Airports Table(Dimension) :

| Table Column | Data Type | Description |
| -------- | ------------- | --------- |
| ident(PRIMARY KEY) | VARCHAR | Identifier |
| type | VARCHAR | Airport type |
| name | VARCHAR | Airport name |
| iso_country | VARCHAR | Country |
| iata_code | VARCHAR | Airport Cose |




## Tools and Technologies

### Data Source:
Amazon S3 is used as a data Source and also to store intermediate transformed data. The data sources are in csv and sas format while the intermediate transformed results are written to S3 in parquet format after processing

### Processing:
Apache Spark with Python is used on EMR Cluster to do the cleaning and the transformations on the data from the data sources. The data is transformed and stored in S3 to be staged in Redshift.

### Data Warehouse: 
Redshift is used as a data Warehouse where the tables follow the Star schema. The transformed data in S3 is staged into Redshift to be loaded into fact and dimension tables.

### Data Pipeline:
Airflow on EC2 is used to orchestrate the data from data source to data warehouse. This workflow submits the spark job on EMR cluster for the transformation, creates tables in Redshift, loads the transformed datasets from S3 to Redshift and finally performs the data quality checks.


## Project Datasets

### Immigration Dataset

This data comes from the US National Tourism and Trade Office. 

### Weather Dataset

This dataset came from Kaggle. 

### Demographics Dataset

This data comes from OpenSoft. 

### Airports Dataset

This is a simple table of airport codes and corresponding cities.  

## ETL Workflow Steps

Below are the tasks performed in the DAG :

1. Create an EMR connection and submit Spark job on EMR cluster using SSH. This step will extract the data (I94 data, weather data, demographics data and airports data), transform them and loads the transformed data in parquet format in S3.
2. Create Fact and Dimension tables in Redshift.
3. Load Fact and dimension tables from the transformed data present in S3.
4. Perform data quality checks for the records loaded in Redshift.

## How to run

1. Start the AIrflow weberver and access the Airflow UI.
2. Create the Emr connection, AWS connection and Redshift connection by entering the credentials in the Airflow UI.
3. Toggle the DAG twitch to ON for the scheduler to pick the job and start running as per the schedule.



## Scenarios
Data increase by 100x. 
- For improving the read performance, data can be stored in parquet format in S3 which consumes less space compared to other data formats and also reduces IO operations.
- The processing of the data increase can be handled by added more number of compute nodes in the EMR cluster. We can also take advantage of automatic scaling feature available in Amazon EMR to scale the number of nodes in the cluster up on the fly depending on the workload. The increase of read and writes to the database can be handled by adding more number of nodes to the Redshift cluster using elastic resize feature of Redshift. 
- Dist keys and sort keys can be used in Redshift for faster query performance and to attain efficient parallelism. 



Pipelines would be run on 7am daily. how to update dashboard? would it still work?
-  To run the pieline by 7 pm daily, the start_date parameter can be set to set both date and time. The schedule interval can be set to @daily to run the pipelines everyday. 
Airflow's bulit in feature to automatically email the failed tasks and jobs can be set to notify the team of job failure. The DAG can be quicly fixed and be triggered again to run to update the dashboard. If it is taking longer to fix the issue, the dashboard will use the previous day data.


Make it available to 100+ people
-  Concurrency scaling can be enabled in Redshift which will automatically add additional cluster capacity to process an increase in concurrent read queries.
