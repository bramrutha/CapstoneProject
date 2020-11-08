from pyspark.sql.types import IntegerType,DateType
from pyspark.sql.functions import udf
from datetime import datetime,timedelta

#udf to convert SAS date to string date format
sas_to_date_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime("%Y-%m-%d"))

#function to convert the column type
def cast_type(df,cols):
    for k,v in cols.items():
        df = df.withColumn(k,df[k].cast(v))
    return df

