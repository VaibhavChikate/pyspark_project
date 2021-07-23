from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("testing").master("local").getOrCreate()
indata="C:\\Bigdata\\Datasets\\2008.csv"
outdata="D:\\output"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(indata)
#df.show(5)
df.createTempView("tab")
qry=spark.sql("select Year,FlightNum,Origin,Dest,count(*) from tab group by Year,FlightNum,Origin,Dest")
qry.show(10)
qry.write.format("csv").option("header","true").save(outdata)
