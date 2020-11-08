from pyspark.sql import SparkSession
from immigration_transform import ImmTransform

def create_spark_session():
    """
    Initialize a spark session
    """

    return SparkSession   \
           .builder       \
           .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")    \
           .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0") \
           .enableHiveSupport()                                                \
           .getOrCreate()

def main():
    """
    This main method transforms the datasets and stores the results in S3 in parquet format:
    """
    spark = create_spark_session()
    imm = ImmTransform(spark)

    #Transform Immigration data
    imm.transform_i94_data()
    #Transform Weather data
    imm.transform_weather_data()
    #Trandform Demographics data
    imm.transform_demographics_data()
    #Transform Airport data
    imm.transform_airport_data()

# Entry point for the pipeline
if __name__ == "__main__":
    main()
