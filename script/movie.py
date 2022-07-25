import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df = (spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3://manual-bucket-megc/raw-data/movie_review.csv"))
df.show(5)

#For the movie_review.csv file:
#a. Work with the cid and review_str columns to get a list of words used by users.
#Note: You can implement the pyspark.ml.feature.Tokenizer class to create a list of words named review_token.
tokenizedDF = Tokenizer(inputCol="review_str", outputCol="review_token") 
wordsListDF = tokenizedDF.transform(df)
wordsListDF.head()

#Remove stop words if needed with pyspark.ml.feature.StopWordsRemover.
listWOStop = StopWordsRemover(inputCol='review_token', outputCol='wo_stop_words')
listWOStopRemoved = listWOStop.transform(wordsListDF)
listWOStopRemoved.show(5)

#Look for data that contain the word  “good”, consider the review as positive, and name it as positive_review.
booleanDF = listWOStopRemoved.withColumn('positive_review', array_contains(col("wo_stop_words"),"good"))
booleanDF.show(5)

#Add a timestamp when the job is running as the insert_date column
dfTimestamp= booleanDF.withColumn('insert_date', lit(current_timestamp()))
dfTimestamp.show(5)

#Use the following logic to convert a boolean column to an integer: 
#reviews.positive_review = CASE
 #  		WHEN positive_review IS True THEN 1
 #  		ELSE 0
 # 		  END
isPositiveDF = dfTimestamp.withColumn('is_possitive', when(dfTimestamp.positive_review=='true', 1).when(dfTimestamp.positive_review=='false',0))
isPositiveDF.show(5)

#Store your results into a new file in the STAGE area (user_id(cid), positive_review(is_possitive) and review_id).
dropColumnsDF = isPositiveDF.drop('review_str').drop('review_token').drop('wo_stop_words').drop('insert_date').drop('positive_review')
classified_movie_review = dropColumnsDF.withColumnRenamed('cid', 'customer_id').withColumnRenamed('is_positive','positive_review').withColumnRenamed('id_review', 'review_id')
classified_movie_review.show(5)

#save dataframe as CSV file
classified_movie_review.write.option("header","true").csv("s3://manual-bucket-megc/stage-data/classified_movie_review")

job.commit()
