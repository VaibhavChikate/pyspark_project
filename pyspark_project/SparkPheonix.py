import sys
from pyspark import *
from pyspark.sql import *
spark=SparkSession.builder.master("local").appName("pheonix").getOrCreate()
table=sys.argv[1]
df=spark.read.format("org.apache.phoenix.spark").option("table",table).option("zkUrl","localhost:2181").load()
df.show(5)
