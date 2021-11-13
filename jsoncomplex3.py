import re
from pyspark import *
from pyspark.sql import *
from pyspark.sql import functions as F
import sys

spark=SparkSession.builder.appName("json").master("local").getOrCreate()
path="C:\\Bigdata\\Datasets\\nyc_school_attendance\\Restaurants_in_Durham_County_NC.json"
df=spark.read.json(path)
#df=df.toDF(*(re.sub(r"[^A-Za-z]","",i)for i in df.columns))
df.printSchema()
df2=df.withColumn("geolocation",F.explode(F.col("fields.geolocation")))\
    .withColumn("coordinates",F.explode(F.col("geometry.coordinates")))\
    .withColumn("root_closing_date",F.col("fields.closing_date"))\
    .withColumn("root_est_group_desc",F.col("fields.est_group_desc"))\
    .withColumn("root_hours_of_operation",F.col("fields.hours_of_operation"))\
    .withColumn("root_string",F.col("fields.id"))\
    .withColumn("root_insp_freq",F.col("fields.insp_freq"))\
    .withColumn("root_opening_date",F.col("fields.opening_date"))\
    .withColumn("root_premise_address1",F.col("fields.premise_address1"))\
    .withColumn("premise_address2",F.col("fields.premise_address2"))\
    .withColumn("root_premise_city",F.col("fields.premise_city"))\
    .withColumn("root_premise_name",F.col("fields.premise_name"))\
    .withColumn("root_premise_phone",F.col("fields.premise_phone"))\
    .withColumn("root_premise_state",F.col("fields.premise_state"))\
    .withColumn("root_premise_zip",F.col("fields.premise_zip"))\
    .withColumn("root_risk",F.col("fields.risk"))\
    .withColumn("root_rpt_area_desc",F.col("fields.rpt_area_desc"))\
    .withColumn("root_seats",F.col("fields.seats"))\
    .withColumn("root_sewage",F.col("fields.sewage"))\
    .withColumn("root_smoking_allowed",F.col("fields.smoking_allowed"))\
    .withColumn("root_status",F.col("fields.status"))\
    .withColumn("root_transitional_type_desc",F.col("fields.transitional_type_desc"))\
    .withColumn("root_type_description",F.col("fields.type_description"))\
    .withColumn("root_water",F.col("fields.water"))\
    .withColumn("root_type",F.col("geometry.type"))\
    .drop("fields","geometry")
df2.printSchema()

