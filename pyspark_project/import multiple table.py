from pyspark import *
from pyspark.sql import *
spark=SparkSession.builder.appName("testing").master("local").getOrCreate()
url="jdbc:mysql://mysqldb.c5logfp8eply.ap-south-1.rds.amazonaws.com:3306/sqldb"
table=["emp","dept"]
for i in table:
    df=spark.read.format("jdbc").option("url",url).option("user","username").option("password","password").option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable",i).load()
    df.show(5)
