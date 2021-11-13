from pyspark.sql import *
from pyspark.sql import functions as F
import re
import snowflake.connector as sf


spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\datasets\\world_bank.json"
df=spark.read.format("json").option("header","true").option("InferSchema","true").load(data)
df1 = df.withColumn("id",F.col("`_id`.`$oid`"))\
    .withColumn("majorsector_percent",F.explode(F.col("majorsector_percent"))) \
    .withColumn("majorsector_per cent_name",F.col("majorsector_percent.Name")) \
    .withColumn("majorsector_percent_percent",F.col("majorsector_percent.Percent")) \
    .withColumn("mjsector_namecode",F.explode(F.col("mjsector_namecode"))) \
    .withColumn("mjsector_namecode_code",F.col("mjsector_namecode.code")) \
    .withColumn("mjsector_namecode_name",F.col("mjsector_namecode.name")) \
    .withColumn("mjtheme",F.explode(F.col("mjtheme"))) \
    .withColumn("mjtheme_namecode",F.explode(F.col("mjtheme_namecode"))) \
    .withColumn("mjtheme_namecode_code",F.col("mjtheme_namecode.code")) \
    .withColumn("mjtheme_namecode_name",F.col("mjtheme_namecode.name")) \
    .withColumn("project_abstract",F.col("project_abstract.cdata")) \
    .withColumn("projectdocs",F.explode(F.col("projectdocs"))) \
    .withColumn("projectdocs_docdate",F.col("projectdocs.Docdate")) \
    .withColumn("projectdocs_doctype",F.col("projectdocs.DocType")) \
    .withColumn("projectdocs_doctypeDesc", F.col("projectdocs.DoctypeDesc")) \
    .withColumn("projectdocs_docURL", F.col("projectdocs.DocURL")) \
    .withColumn("projectdocs_EntityID",F.col("projectdocs.EntityID")) \
    .withColumn("sector",F.explode(F.col("sector"))) \
    .withColumn("sector_name",F.col("sector.Name")) \
    .withColumn("sector1_Name", F.col("sector1.Name")) \
    .withColumn("sector1_Percent", F.col("sector1.Percent")) \
    .withColumn("sector2_Name", F.col("sector2.Name")) \
    .withColumn("sector2_Percent", F.col("sector2.Percent")) \
    .withColumn("sector3_Name", F.col("sector3.Name")) \
    .withColumn("sector3_Percent", F.col("sector3.Percent")) \
    .withColumn("sector4_Name", F.col("sector4.Name")) \
    .withColumn("sector4_Percent", F.col("sector4.Percent")) \
    .withColumn("sector_namecode",F.explode(F.col("sector_namecode"))) \
    .withColumn("sector_namecode_code",F.col("sector_namecode.code")) \
    .withColumn("sector_namecode_name",F.col("sector_namecode.name")) \
    .withColumn("theme1_Name",F.col("theme1.Name")) \
    .withColumn("theme1_Percent",F.col("theme1.Percent")) \
    .withColumn("theme_namecode", F.explode(F.col("theme_namecode"))) \
    .withColumn("theme_namecode_code", F.col("theme_namecode.code")) \
    .withColumn("theme_namecode_name", F.col("theme_namecode.name")) \
    .drop("theme1","_id","majorsector_percent","mjsector_namecode","mjtheme_namecode","project_abstract")\
    .drop("projectdocs","sector","sector1","sector2","sector3","sector4","sector_namecode","theme1","theme_namecode")

df1.printSchema()
df1.show(5)


'''
sfOptions=.option("sfUrl","https://cv64318.ap-south-1.aws.snowflakecomputing.com")\
      .option("sfUser","Mangesh777")\
      .option("sfPassword","Mangesh4$")\
      .option("sfDatabase","DEMO_DB")\
      .option("sfSchema" ,"PUBLIC")\
      .option("sfWarehouse" , "COMPUTE_WH")
'''
#SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIATUYEKJWZLEMBEVST")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

df1.write.format('net.snowflake.spark.snowflake')\
      .option('sfUrl','https://cv64318.ap-south-1.aws.snowflakecomputing.com')\
      .option('sfUser','Mangesh777')\
      .option('sfPassword','Mangesh4$')\
      .option('sfDatabase','DEMO_DB')\
      .option('sfSchema' ,'PUBLIC')\
      .option('sfWarehouse' , 'COMPUTE_WH')\
      .option('dbtable', 'JSON_DATA')\
      .mode('overwrite')\
      .save()

