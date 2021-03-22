import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read("capstone_project.cfg")

#AWS_ACCESS_KEY_ID =config['AWS']['AWS_ACCESS_KEY_ID']
#AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
S3_BUCKET_INPUT = config['AWS']['S3_BUCKET_INPUT']
S3_BUCKET_OUTPUT = config['AWS']['S3_BUCKET_OUTPUT']
S3_BUCKET_EVENTS = config['AWS']['S3_BUCKET_EVENTS']


os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
       This function creates the SparkSession with the needed dependencies.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def check_tweet_id_columns(spark, data):
    """
        This file reads in the output tweet data of the etl_pyspark pipeline and
        checks for consistency: Do tweet and user id match the appropriate data
        format.
    """

    tweets = spark.read.parquet(os.path.join(data, "Tweets"))

    count_user_id = tweets.select(F.col("user_id").cast("int").isNull()).count()
    count_tweet_id = tweets.select(F.col("tweet_id").cast("int").isNull()).count()

    if count_user_id + count_tweet_id > 0:
        # Return false, user should check ID columns in tweets
        assert False
    else:
        assert True



def main():
    """
       This is the main function which defines the pathes to input and output datasets
       and calls the function to create the spark session and to process the tweet
       and and events data created above.
    """
    spark = create_spark_session()
    input_data = "s3a://" + S3_BUCKET_INPUT
    input_data_events = "s3a://" + S3_BUCKET_EVENTS
    output_data = "s3a://" + S3_BUCKET_OUTPUT


    check_tweet_id_columns(spark, output_data)



if __name__ == "__main__":
    main()
