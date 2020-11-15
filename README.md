# Capstone Immigration project

## Project overview

The purpose of this project is to build a data warehouse for Immigration data of US. This data can be used to analyze the statistics or patterns of people entering US 
from various parts of the world based on Weather, ethnicities and the states that they are entring to.

The data warehouse is built by running a data pipeline using Airflow. The data from various sources is ingested, transformed using Spark and staged to Redshift which is
the data warehouse.

## Project details

### Data Source:
Amazon S3 is used as a data Source and also to store intermediate transformed data.

### Processing:
Pyspark is used on EMR Cluster to run the transformations.

### Data Warehouse: 
Redshift is used as a data Warehouse where the tables follow the Star schema.

### Data Pipeline:
Airflow on EC2 is used to orchestrate the data from data source to data warehouse.


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
Data increase by 100x. 
- For improving the read performance, data can be stored in parquet format in S3 which consumes less space compared to other data formats and also reduces IO operations.
- The processing of the data increase can be handled by added more number of compute nodes in the EMR cluster. We can also take advantage of automatic scaling feature available in Amazon EMR to scale the number of nodes in the cluster up on the fly depending on the workload. The increase of read and writes to the database can be handled by adding more number of nodes to the Redshift cluster using elastic resize feature of Redshift. 
- Dist keys and sort keys can be used in Redshift for faster query performance and to attain efficient parallelism. 



Pipelines would be run on 7am daily. how to update dashboard? would it still work?
-  To run the pieline by 7 pm daily, the start_date parameter can be set to set both date and time. The schedule interval can be set to @daily to run the pipelines everyday. 
Airflow's bulit in feature to automatically email the failed tasks and jobs can be set to notify the team of job failure. The DAG can be quicly fixed and be triggered again to run to update the dashboard. If it is taking longer to fix the issue, the dashboard will use the previous day data.


Make it available to 100+ people
-  Concurrency scaling can be enabled in Redshift which will automatically add additional cluster capacity to process an increase in concurrent read queries.
