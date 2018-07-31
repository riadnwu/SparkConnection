from pyspark.sql import SparkSession
#from pyspark.sql.functions import mean
spark=SparkSession.builder.appName('miss').getOrCreate()
df=spark.read.csv('ContainsNull.csv',inferSchema=True,header=True)
df.show()

#show without null value
df.na.drop(thresh=2).show()#how many null value exit
df.na.drop(how='all').show() # if all vlaue is null
df.na.drop(how='any').show() #if single value is null
df.na.drop(subset=['Sales']).show() #exit which colum null
#fill value with
df.na.fill('Fill value').show() #fill null value with
df.na.fill(0).show() #fill int valu
df.na.fill('No name',subset=['Name']).show()

