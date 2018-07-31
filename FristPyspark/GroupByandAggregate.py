from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct,format_number

spark=SparkSession.builder.appName('aggs').getOrCreate()
df=spark.read.csv('sales_info.csv',inferSchema=True,header=True)
df.show()
df.printSchema()

#grupeBy
df.groupBy("Company").mean().show()
df.groupBy("Company").max().show()
df.groupBy("Company").min().show()
df.groupBy("Company").count().show()

df.agg({'Sales':'max'}).show()

#grup Data
grupData=df.groupBy("company")
grupData.agg({'Sales':'max'}).show()

#count distint value
df.select(countDistinct('Sales').alias("Distint value")).show()

#Formate Number

sales=df.select(countDistinct('Sales').alias("Distint value")).show()
#sales.select(format_number("Distint value",2).alias("Distint value")).show()

#order By
df.orderBy("Sales").show()
df.orderBy(df['Sales'].desc()).show()