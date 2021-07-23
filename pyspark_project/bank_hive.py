from pyspark import *
from pyspark.sql import *
import sys
spark=SparkSession.builder.enableHiveSupport().master("local").appName("bankdata").getOrCreate()
#inputpath="C:\\Bigdata\\Datasets\\bank-full.csv"
#outputpath="C:\\Bigdata\\Datasets\\output\\banklocal"
inputpath=sys.argv[1]
table=sys.argv[2]
#outputpath=sys.argv[2]
df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(inputpath)
#df.show(5)
df.createTempView("bank")
res=spark.sql("select marital,education,count(*) from bank group by marital,education")
#res.coalesce(2).write.mode("overwrite").format("csv").option("header","true").save(outputpath)           --it will save the output in s3 bucket
res.write.mode("overwrite").format("hive").saveAsTable(table)                                #it will create managed table in hive
#res.write.mode("overwrite").format("hive").option("path","/sparkhive/bankexternal").saveAsTable      --it will create external table in hive
print("successfully saved result")
