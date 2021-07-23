#to read text file
#if we read text file firstly have make rdd of it then using map function split by its delimiter
# and provide column name using Row function
#create dataframe of that output and then create spark.sql session
from pyspark.sql import *
from pyspark import *
spark=SparkSession.builder.appName("TEXT file").getOrCreate()
sc=spark.sparkContext
rdd=sc.textFile("C:/Users/BlackPerl/OneDrive/Desktop/DataSet/bev_table.txt")
split=rdd.map(lambda x: x.split(","))
columns=split.map(lambda x: Row(name=x[0],branch=x[1]))
df=spark.createDataFrame(columns)
df.createTempView("cafe")
query=spark.sql("select * from cafe").show()
query2=spark.sql("select branch,count(*) from cafe group by branch order by branch").show()
