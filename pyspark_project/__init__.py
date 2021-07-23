from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("practice").getOrCreate()
sc = spark.sparkContext
