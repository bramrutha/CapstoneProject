import configparser
import logging
from pyspark.sql.types import IntegerType
import immigration_utility as fn
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dwh.cfg')
immigration_save_path = config['SAVE']['IMMIGRATION']
date_save_path = config['SAVE']['DATES']
weather_save_path = config['SAVE']['WEATHER']
state_save_path = config['SAVE']['STATE']
airport_save_path = config['SAVE']['AIRPORT']


class ImmTransform:

    def __init__(self,spark):
        self.spark = spark
        self.load_path = config['S3']['LANDING_ZONE']
        self.save_path = config['S3']['TRANSFORMED_ZONE']


    def transform_i94_data(self):
        logging.debug("Inside I94 transformation")
        #i94_df = self.spark.read.csv(self.load_path + config['LOAD']['I94_DATA'], header=True, mode='PERMISSIVE', inferSchema=True)
        i94_df = self.spark.read.format('com.github.saurfang.sas.spark').load(self.load_path + config['LOAD']['I94_DATA'] )
        #drop the columns that are not required for analysis
        i94_drop_cols =  ['count','visapost','occup','matflag','biryear','insnum']
        i94_df = i94_df.drop(*i94_drop_cols)

        int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'i94mode','depdate', 'i94bir', 'i94visa']
        i94_df = fn.cast_type(i94_df,dict(zip(int_cols,len(int_cols)*[IntegerType()])))

        date_cols = ['arrdate','depdate']
        for c in date_cols:
            i94_df = i94_df.withColumn(c,fn.sas_to_date_udf(i94_df[c]))

        #Create dates dataframe to be stored as a separate file
        arrdate  = i94_df.select('arrdate').distinct()
        depdate  = i94_df.select('depdate').distinct()
        date_df = arrdate.union(depdate)
        date_df = date_df.withColumnRenamed('arrdate','date')
        date_df = date_df.withColumn('year',F.year(date_df.date))   \
                         .withColumn('month',F.month(date_df.date)) \
                         .withColumn('day',F.dayofmonth(date_df.date))  \
                         .withColumn('dayofweek',F.dayofweek(date_df.date))  \
                         .withColumn('dayofyear',F.dayofyear(date_df.date))

        date_df.write.mode('overwrite').parquet(self.save_path + date_save_path)

        i94_df.write.partitionBy('i94yr','i94mon').mode('overwrite').parquet(self.save_path + immigration_save_path)


    def transform_weather_data(self):

        weather_df = self.spark.read.csv(self.load_path + config['LOAD']['WEATHER_DATA'], header=True, mode='PERMISSIVE', inferSchema=True)
        weather_df = weather_df.filter(weather_df.dt.startswith('2013'))
        weather_df = weather_df.groupBy('Country').agg({'AverageTemperature' : 'avg', 'Latitude' : 'first', 'Longitude' : 'first'})
        weather_df = weather_df.withColumnRenamed('avg(AverageTemperature)','Temperature') \
                               .withColumnRenamed('first(Latitude)', 'Latitude')            \
                               .withColumnRenamed('first(Longitude)','Longitude')

        weather_df = weather_df.withColumn('Country', F.upper(weather_df.Country))

        countries = [("Country", "Congo (Democratic Republic Of The)", "Congo"), ("Country", "Côte D'Ivoire", "Ivory Coast")]
        for field,old,new in countries:
            weather_df = weather_df.withColumn(field,F.when(weather_df[field] == old,new).otherwise(weather_df[field]))


        i94ctry_df = self.spark.read.csv(self.load_path + config['LOOKUP']['I94CTRY'], header=True,mode='PERMISSIVE',inferSchema=True)

        countries  = [("I94CTRY", "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"),
                      ("I94CTRY", "INVALID: CANADA", "CANADA"),
                      ("I94CTRY", "CHINA, PRC", "CHINA"),
                      ("I94CTRY", "GUINEA-BISSAU", "GUINEA BISSAU"),
                      ("I94CTRY", "INVALID: PUERTO RICO", "PUERTO RICO"),
                      ("I94CTRY", "INVALID: UNITED STATES", "UNITED STATES")]

        for field,old,new in countries:
            i94ctry_df = i94ctry_df.withColumn(field,F.when(i94ctry_df[field] == old,new).otherwise(i94ctry_df[field]))

        country_df = i94ctry_df.join(weather_df, i94ctry_df.I94CTRY == weather_df.Country ,how='LEFT')
        country_df = country_df.withColumn("I94CTRY", F.when(F.isnull(country_df["Country"]),country_df["I94CTRY"]).otherwise(country_df["Country"]))
        country_df = country_df.drop('Country')
        country_df = country_df.withColumnRenamed('I94CTRY','Country')   \
                               .withColumnRenamed('Code','Country_code')

        country_df.write.save(self.save_path + weather_save_path, mode="overwrite", format="parquet", partitionBy=None)



    def transform_demographics_data(self):
        demo_df = self.spark.read.csv(self.load_path + config['LOAD']['DEMO_DATA'], sep=';',header=True,mode='PERMISSIVE', inferSchema=True)
        state_agg_df = demo_df.groupBy('State Code').agg({'State' : 'First','Median Age' : 'avg','Male Population':'sum','Female Population' : 'sum',
                                                      'Total Population' : 'sum'})
        state_agg_df = state_agg_df.withColumnRenamed('State Code','State_code')  \
                                     .withColumnRenamed('first(State)','State')     \
                                     .withColumnRenamed('sum(Male Population)','Male_population') \
                                     .withColumnRenamed('sum(Female Population)', 'Female_population') \
                                     .withColumnRenamed('avg(Median Age)', 'Median_age') \
                                     .withColumnRenamed('sum(Total Population)', 'Total_population')
        race_df = demo_df.groupBy('State Code').pivot('Race').sum('Count')
        race_df = race_df.withColumnRenamed('State Code','State_code')  \
                         .withColumnRenamed('American Indian and Alaska Native','AmericanIndian_and_AlaskaNative')     \
                         .withColumnRenamed('Black or African-American', 'Black_or_AfricanAmerican') \
                         .withColumnRenamed('Hispanic or Latino', 'Hispanic_or_Latino')

        state_df = state_agg_df.join(race_df,state_agg_df.State_code == race_df.State_code)
        state_df = state_df.drop('State_Code')

        state_df.write.save(self.save_path + state_save_path, mode="overwrite", format="parquet", partitionBy=None)


    def transform_airport_data(self):
        airport_df = self.spark.read.csv(self.load_path + config['LOAD']['AIRPORT_DATA'], header=True,mode='PERMISSIVE', inferSchema=True)
        airport_df = airport_df.filter((airport_df.iata_code.isNotNull()) & (airport_df.iso_country == 'US')).select('ident','type','name','iso_country','iata_code')
        airport_df.write.save(self.save_path + airport_save_path, mode="overwrite", format="parquet", partitionBy=None)





