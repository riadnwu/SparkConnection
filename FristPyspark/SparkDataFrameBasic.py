from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('ops').getOrCreate()
df=spark.read.csv('appl_stock.csv',inferSchema=True,header=True)
df.printSchema()
print (df.head(3)[0])
#select value with condition
df.filter("Close < 500").select(['Open','Close']).show()

#and condition
df.filter((df['Close']<200) & (df['Open']>200)).show()

#use cases
result=df.filter(df['Low']==197.16).collect()
print(result)
row=result[0]
row.asDict()['Volume']
