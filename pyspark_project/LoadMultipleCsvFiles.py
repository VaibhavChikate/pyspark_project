from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("testing").master("local").getOrCreate()
inputpath="D:\\output\\*.csv"
df=spark.read.format("csv").option("header","true").option("inferschema","true").load(inputpath)

count=df.count()
df.show(count)
print(count)        #138554 records
