from pyspark import *
from pyspark.sql import *
spark=SparkSession.builder.appName("testing").master("local").getOrCreate()
df=spark.read.json("C:\\Bigdata\\Datasets\\cars.json")
#print(df.count())
#df.show()
url="jdbc:mysql://mysqldb.c5logfp8eply.ap-south-1.rds.amazonaws.com:3306/sqldb"
df.write.format("jdbc").option("url",url).option("user","username").option("password","password").option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","cars").mode("overwrite").save()
print("succefully table is exported")

