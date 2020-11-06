from pyspark.sql.types import IntegerType,DateType
from pyspark.sql.functions import udf
from datetime import datetime,timedelta

sas_to_date_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime("%Y-%m-%d"))

def cast_type(df,cols):
    for k,v in cols.items():
        df = df.withColumn(k,df[k].cast(v))
    return df

