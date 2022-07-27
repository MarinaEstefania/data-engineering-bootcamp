import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import *


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

#For the log_reviews.csv file:
#Map the structure for the DataFrame schema according to the log_review column that contains the xml as a string.
schemaLog = StructType([ \
    StructField("log_id",StringType(),True), \
    StructField("logDate",StringType(),True), \
    StructField("device",StringType(),True), \
    StructField("location", StringType(), True), \
    StructField("os", StringType(), True), \
    StructField("ipAddress", StringType(), True), \
    StructField("phoneNumber", StringType(), True) \
  ])

#Work with the log  column to get all the metadata and build your columns for your  DataFrame.
df = spark.read.format("xml").option("rootTag", "reviewlog").option("rowTag", "log").load("s3://manual-bucket-megc/raw-data/log_reviews.csv", schema=schemaLog)
#df.show(5)

#Don’t forget to drop the log column by the end
dropLogIdDF = df.drop("log_id")
#dropLogIdDF.show(5)

#Store your results into a new file in the STAGE area (log_id, log_date, device, os, location, browser, ip, phone_number)
#find os list
osValues = dropLogIdDF.select("os").dropDuplicates()
#osValues.show()

#add browser column  
withBrowserDF = dropLogIdDF.withColumn("browser", when(dropLogIdDF.os=="Apple iOS", "Safari") \
                                       .when(dropLogIdDF.os=="Apple MacOS", "Safari") \
                                       .when(dropLogIdDF.os=="Microsoft Windows", "Microsoft Edge") \
                                       .when(dropLogIdDF.os=="Linux", "Firefox") \
                                       .when(dropLogIdDF.os=="Google Android", "Google Chrome") \
                                       .when(dropLogIdDF.os=="Linux", "Firefox"))
#withBrowserDF.show(10)

#Rename columns
renameColsDF = withBrowserDF.withColumnRenamed("logDate","log_date").withColumnRenamed("ipAddress","ip").withColumnRenamed("phoneNumber","phone_number")
#renameColsDF.show(5)

#add log_id column
withIncreasingIDDF = renameColsDF.withColumn("monotonically_increasing_id", monotonically_increasing_id())
window = Window.orderBy(col("monotonically_increasing_id"))
reviewLogdDF = withIncreasingIDDF.withColumn("log_id", row_number().over(window)).drop("monotonically_increasing_id")
reviewLogdDF.show(10)

#Load results
reviewLogdDF.write.option("header","true").parquet("s3://manual-bucket-megc/stage-data/review_logs.parquet")