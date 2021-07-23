from pyspark import  *
from pyspark.sql import *
from pyspark.sql import functions as F
from pandas import *
spark=SparkSession.builder.master("local").appName("testing").getOrCreate()
df=spark.read.json("C:\\Bigdata\\Datasets\\world_bank.json")
df.cache()
#df.show(5)
#df.printSchema()
res=df.withColumn("majorsector_percent",F.explode(F.col("majorsector_percent")))\
    .withColumn("mjsector_namecode",F.explode(F.col("mjsector_namecode")))\
    .withColumn("mjtheme",F.explode(F.col("mjtheme")))\
    .withColumn("mjtheme_namecode",F.explode(F.col("mjtheme_namecode")))\
    .withColumn("projectdocs",F.explode(F.col("projectdocs")))\
    .withColumn("sector",F.explode(F.col("sector")))\
    .withColumn("theme_namecode",F.explode(F.col("theme_namecode")))\
    .withColumn("sector_namecode",F.explode(F.col("sector_namecode")))\
    .withColumn("_idi",F.col("`_id`.`$oid`"))\
    .withColumn("majorsector_percent_name",F.col("majorsector_percent.Name"))\
    .withColumn("majorsector_percent_percent",F.col("majorsector_percent.Percent"))\
    .withColumn("mjsector_namecode_code",F.col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name",F.col("mjsector_namecode.name"))\
    .withColumn("mjtheme_namecode_code",F.col("mjtheme_namecode.code"))\
    .withColumn("mjtheme_namecode_name",F.col("mjtheme_namecode.name"))\
    .withColumn("project_abstract_cdata",F.col("project_abstract.cdata"))\
    .withColumn("projectdocs_date",F.col("projectdocs.DocDate"))\
    .withColumn("projectdocs_type",F.col("projectdocs.DocType"))\
    .withColumn("projectdocs_typedesc",F.col("projectdocs.DocTypeDesc"))\
    .withColumn("projectdocs_DocURL",F.col("projectdocs.DocURL"))\
    .withColumn("projectdocs_entityid",F.col("projectdocs.EntityID"))\
    .withColumn("sectorName",F.col("sector.Name"))\
    .withColumn("sector1Name",F.col("sector1.Name"))\
    .withColumn("sector1percent", F.col("sector1.Percent")) \
    .withColumn("sector2Name", F.col("sector2.Name"))\
    .withColumn("sector2percent",F.col("sector2.Percent"))\
    .withColumn("sector3Name",F.col("sector3.Name"))\
    .withColumn("sector3percent",F.col("sector3.Percent"))\
    .withColumn("sector4Name",F.col("sector4.Name"))\
    .withColumn("sector4percent",F.col("sector4.Percent"))\
    .withColumn("sector_namecode_code",F.col("sector_namecode.code"))\
    .withColumn("sector_namecode_name",F.col("sector_namecode.name"))\
    .withColumn("theme1name",F.col("theme1.Name"))\
    .withColumn("theme1percent",F.col("theme1.Percent"))\
    .withColumn("theme_namecodecode",F.col("theme_namecode.code"))\
    .withColumn("theme_namecodename",F.col("theme_namecode.name"))\
    .drop("_id","majorsector_percent","mjsector_namecode","mjtheme_namecode","project_abstract","projectdocs")\
    .drop("sector","sector1","sector2","sector3","sector4","sector_namecode","theme1","theme_namecode")

#res.printSchema()
res.createTempView("worldbank")
url="jdbc:mysql://mysqldb.c5logfp8eply.ap-south-1.rds.amazonaws.com:3306/sqldb"
res.write.format("jdbc").option("url",url).mode("overwrite").option("user","username").option("password","password").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","sparkjson").save()
