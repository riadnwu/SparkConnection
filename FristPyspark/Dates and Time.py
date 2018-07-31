from pyspark.sql import SparkSession
from pyspark.sql.functions import (dayofmonth,month,dayofyear,year,weekofyear,format_number,date_format)

spark=SparkSession.builder.appName('dateTime').getOrCreate()
df=spark.read.csv('appl_stock.csv',header=True,inferSchema=True)
df.select(['Date','Open']).show()
#date of day
df.select(dayofmonth(df['Date'])).show()
df.select(month(df['Date'])).show()

df.select(year(df['Date'])).show()
df.withColumn("year",year(df['Date'])).show()


