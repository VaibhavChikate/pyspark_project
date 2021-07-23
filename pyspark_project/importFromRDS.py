from pyspark import *
from pyspark.sql import *
from pandas import *
spark=SparkSession.builder.master("local").appName("testing").getOrCreate()
url="jdbc:mysql://mysqldb.c5logfp8eply.ap-south-1.rds.amazonaws.com:3306/sqldb"
qry="(select * from bank )as tmp"
df=spark.read\
    .format("jdbc")\
    .option("url",url)\
    .option("user","username")\
    .option("password","password")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable",qry)\
    .load()
df.show(10)
print(df.count())
