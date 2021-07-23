#to read json file

from pyspark.sql import *
from pyspark import *
spark=SparkSession.builder.appName("sql").getOrCreate()
df=spark.read.json("C:/Users/BlackPerl/OneDrive/Desktop/DataSet/employee.json")
df.show()
df.createTempView("emp")
query=spark.sql("select * from emp").show()
