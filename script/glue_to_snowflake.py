#DEPENDENCIES
#Glue version: 2.0-supports spark 2.4, Scala 2, Python 3
#Language: Python 3
#Dependent JARS: spark-snowflake_2.11-2.9.3-spark_2.4.jar, snowflake-jdbc-3.13.21.jar
#Aditional modules: pyspark, snowflake-connector-python
#Params: [JOB_NAME, URL, WAREHOUSE, DB, SCHEMA, USERNAME, PASSWORD]

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import
from pyspark.sql.functions import split, lit, col

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'URL', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
## uj = sc._jvm.net.snowflake.spark.snowflake
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
"sfURL" : args['URL'],
"sfUser" : args['USERNAME'],
"sfPassword" : args['PASSWORD'],
"sfDatabase" : args['DB'],
"sfSchema" : args['SCHEMA'],
"sfWarehouse" : args['WAREHOUSE'],
"application" : "AWSGlue"
}

#Extract data from s3 bucket
df = spark.read.parquet("s3://manual-bucket-megc/stage-data/review_logs.parquet/")
df.show()

movies_df = spark.read.parquet("s3://manual-bucket-megc/stage-data/classified_movie_review.parquet/")
movies_df.show()

purchase_df = spark.read.option("Header", True).csv("s3://manual-bucket-megc/raw-data/user_purchase.csv")
purchase_df.show()

#Transform data (create dim dataframes)
dim_devices = df.select(col('log_id'),col('device')) \
  .drop_duplicates(['device']) \
  .withColumnRenamed('log_id', 'id_dim_devices')
dim_devices.show()

dim_location = df.select(col('log_id'),col('location')) \
  .drop_duplicates(['location']) \
  .withColumnRenamed('log_id', 'id_dim_location')
dim_location.show()

dim_os = df.select(col('log_id'),col('os')) \
  .drop_duplicates(['os']) \
  .withColumnRenamed('log_id', 'id_dim_os')
dim_os.show()

dim_browser = df.select(col('log_id'),col('browser')) \
  .drop_duplicates(['browser']) \
  .withColumnRenamed('log_id', 'id_dim_browser')
dim_browser.show()

dim_date = df.select(df.log_id, df.log_date) \
  .drop_duplicates(['log_date']) \
  .withColumnRenamed('log_id', 'id_dim_date')
dim_date = dim_date.withColumn('day', split(col('log_date'),'-').getItem(1)) \
  .withColumn('month', split(col('log_date'),'-').getItem(0)) \
  .withColumn('year', split(col('log_date'),'-').getItem(2)) \
  .withColumn('season', lit('under construction...'))
dim_date.show(5)

#Load data from AWS Glue Studio to Snowflake DataWarehouse
dim_devices.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "DIM_DEVICES").mode("overwrite").save()
dim_location.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "DIM_LOCATION").mode("overwrite").save()
dim_os.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "DIM_OS").mode("overwrite").save()
dim_browser.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "DIM_BROWSER").mode("overwrite").save()
dim_date.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "DIM_DATE").mode("overwrite").save()

df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "review_logs").mode("overwrite").save()
movies_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "classified_movie_review").mode("overwrite").save()
purchase_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "user_purchase").mode("overwrite").save()

job.commit()