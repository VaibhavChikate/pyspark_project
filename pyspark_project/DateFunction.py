from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.master("local").appName("DateFunction").getOrCreate()
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:\\Bigdata\\Datasets\\donations.csv").cache()
#df.show()
df.printSchema()
ndf=df.withColumn("today",current_date()).withColumn("date",to_date("date","dd-MM-yyyy"))
#res=ndf.withColumn("datediff",datediff("today","date"))
#res=ndf.withColumn("date_add",date_add("today",127))
#res=ndf.withColumn("addmonths",add_months("today",5))
#res=ndf.withColumn("mothhbetween",months_between("today","date"))
#res=ndf.withColumn("nextday",next_day("today","Sunday"))
#res=ndf.withColumn("lastday",last_day("today"))
#res=ndf.withColumn("lastsaturday",next_day(date_add(last_day("date"),-7),"Saturday"))
#res=ndf.withColumn("serial",monotonically_increasing_id()+1)
#res=ndf.withColumn("datetrunc",date_trunc("mm","date"))\
#    .withColumn("datetrun",date_trunc("yy","date"))\
#    .withColumn("trunc",trunc("date","mm"))\
#    .withColumn("trun",trunc("date","yy"))
#res=ndf.withColumn("year",year("date")).withColumn("month",month("date"))
#res=ndf.withColumn("quarter",quarter("date"))
res=ndf.withColumn("dayofweek",dayofweek("date"))\
    .withColumn("dayofmonth",dayofmonth("date"))\
    .withColumn("dayofyear",dayofyear("date"))\
    .withColumn("ts",current_timestamp())\
    .withColumn("cd",current_date())
res.show()

