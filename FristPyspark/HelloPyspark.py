from pyspark.sql import SparkSession

from pyspark.sql.types import (StringType,StructField,IntegerType,StructType)

spark=SparkSession.builder.appName('Basics').getOrCreate()
df=spark.read.json('people.json')
df.show()
df.printSchema()
print ( df.columns)
df.describe().show()
df.select('name','age').show()

#add new colum
df.withColumn('newage',df['age']*3).show()
# rename of head
df.withColumnRenamed('Age of Name','age').show()
##Sql quary also add
#new1=spark.sql("SELECT * FROM people Where age=30;")
#new1.show()



'''dataSchema=[StructField('age',IntegerType(),False),
            StructField('name',StringType(),False)]
finalStruct=StructType(fields=dataSchema)
df=spark.read.json('people.json',schema=finalStruct)
df.printSchema()
df.show()'''

