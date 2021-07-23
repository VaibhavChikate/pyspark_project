from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
import sys
spark=SparkSession.builder.master("local").appName("hivetesting").enableHiveSupport().getOrCreate()
url="jdbc:mysql://mysqldb.c5logfp8eply.ap-south-1.rds.amazonaws.com:3306/sqldb"
table=sys.argv[1]
df=spark.read.format("jdbc").option("url",url).option("driver","com.mysql.cj.jdbc.Driver").option("user","username").option("password","password").option("dbtable","bank").load()
df.show()
df.createTempView("bank")
qry=spark.sql("select age,job,marital,education from bank where loan='yes'")
qry.show()

qry.write.format("hive").mode("overwrite").saveAsTable(table)
print("successfully data saved in hive")
