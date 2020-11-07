# Capstone Immigration project

## Project overview

The purpose of this project is to build a data warehouse for Immigration data of US. This data can be used to analyze the statistics or patterns of people entering US 
from various parts of the world based on Weather, ethnicities and the states that they are entring to.

The data warehouse is built by running a data pipeline using Airflow. The data from various sources is ingested, transformed using Spark and staged to Redshift which is
the data warehouse.

## Project details

### Data Source    : Amazon S3 is used as a data Source and also to store intermediate transformed data.
### Processing     : Pyspark is used on EMR Cluster to run the transformations.
### Data Warehouse : Redshift is used as a data Warehouse where the tables follow the Star schema.
### Data Pipeline  : Airflow on EC2 is used to orchestrate the data from data source to data warehouse.


## Project Datasets

### Immigration Dataset

This data comes from the US National Tourism and Trade Office. 

### Weather Dataset

This dataset came from Kaggle. 

### Demographics Dataset

This data comes from OpenSoft. 

### Airports Dataset

This is a simple table of airport codes and corresponding cities.  


## How to run

Start the AIrflow weberver and toggle to DAG switch to ON for the scheduler to pick up the DAG and start running.



## Scenarios
- Data increase by 100x. 
    EMR cluster size can be increased to process higher volume of data.


- Pipelines would be run on 7am daily. how to update dashboard? would it still work?
   DAG can be scheduled at 7AM daily as this dag is set to run once everyday. Upon failure of the DAG, emails can be sent to the team notifying about the failures.


- Make it available to 100+ people
    - Redshift can be scaled to handle heavy workloads and can be run by as many users as required.
