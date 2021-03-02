import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
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




def process_tweets(spark, input_data, output_data):
    """
       This function takes a spark session object, and pathes to input and output data.
       It then retrieves the song data from the inputs and builds dimension tables
       which are stored as parquet files on AWS S3.
       Input:
       - spark: spark-session object
       - input_data: path to input data
       - output_data: path to store data
    """


    #tweet_schema = StructType([
    #StructField("user.id", IntegerType(), False),
    #StructField("user.created_at", StringType(), False), #"DateType"
    #StructField("user.screen_name", StringType(), False),
    #StructField("user.verified",  BooleanType(), False),
    #StructField("user.description", StringType(), True),
    #StructField("user.followers_count", IntegerType(), False),
    #StructField("user.friends_count", IntegerType(), False),

    #StructField("retweeted_status.user.id", IntegerType(), True),
    #StructField("retweeted_status.user.created_at", StringType(), True), #"DateType"
    #StructField("retweeted_status.user.screen_name", StringType(), True),
    #StructField("retweeted_status.user.verified",  BooleanType(), True),
    #StructField("retweeted_status.user.description", StringType(), True),
    #StructField("retweeted_status.user.followers_count", IntegerType(), True),
    #StructField("retweeted_status.user.friends_count", IntegerType(), True),
    #StructField("retweeted_status.text", StringType(), True),
    #StructField("retweeted_status.extended_tweet.full_text", StringType(), True),

    #StructField("quoted_status.user.id", IntegerType(), True),
    #StructField("quoted_status.user.created_at", StringType(), True), #"DateType"
    #StructField("quoted_status.user.screen_name", StringType(), True),
    #StructField("quoted_status.user.verified",  BooleanType(), True),
    #StructField("quoted_status.user.description", StringType(), True),
    #StructField("quoted_status.user.followers_count", IntegerType(), True),
    #StructField("quoted_status.user.friends_count", IntegerType(), True),
    #StructField("quoted_status.text", StringType(), True),
    #StructField("quoted_status.extended_tweet.full_text", StringType(), True),

    #StructField("id", IntegerType(), False),
    #StructField("created_at", StringType(), False),
    #StructField("lang", StringType(), True),

    #StructField("text", StringType(), False),
    #StructField("extended_tweet.full_text", StringType(), True),
    #])

    tweet_file = os.path.join(input_data + "/*.json")

    df = spark.read.json(tweet_file)
    #df = spark.read.json(tweet_file, schema = tweet_schema)

    # User table
    user_table = df.select(col("user.id").alias("user_id"),
              col("user.created_at").alias("user_created_at"),
              col("user.screen_name").alias("user_screen_name"),
              col("user.verified").alias("user_verified"),
              col("user.description").alias("user_description"),
              col("user.followers_count").alias("user_followers_count"),
              col("user.friends_count").alias("user_friends_count")).dropDuplicates(subset=["user_id"])

    retweeted_user_table = df.select(col("retweeted_status.user.id").alias("user_id"),
              col("retweeted_status.user.created_at").alias("user_created_at"),
              col("retweeted_status.user.screen_name").alias("user_screen_name"),
              col("retweeted_status.user.verified").alias("user_verified"),
              col("retweeted_status.user.description").alias("user_description"),
              col("retweeted_status.user.followers_count").alias("user_followers_count"),
              col("retweeted_status.user.friends_count").alias("user_friends_count")).dropDuplicates(subset=["user_id"])

    quoted_user_table = df.select(col("quoted_status.user.id").alias("user_id"),
                  col("quoted_status.user.created_at").alias("user_created_at"),
                  col("quoted_status.user.screen_name").alias("user_screen_name"),
                  col("quoted_status.user.verified").alias("user_verified"),
                  col("quoted_status.user.description").alias("user_description"),
                  col("quoted_status.user.followers_count").alias("user_followers_count"),
                  col("quoted_status.user.friends_count").alias("user_friends_count")).dropDuplicates(subset=["user_id"])

    user_table = user_table.unionByName(retweeted_user_table)
    user_table = user_table.unionByName(quoted_user_table)

    user_table = user_table.dropDuplicates(subset=["user_id"])

    # Write user table to output bucket
    user_table.write.parquet(os.path.join(output_data, "Users"), 'overwrite')

    # Create tweets table (main fact table):

    tweets_table = df.select(col("id").alias("tweet_id"),
    col("created_at").alias("tweet_created_at"),
    col("lang").alias("tweet_lang"),
    col("text").alias("tweet_text"),
    col("extended_tweet.full_text").alias("tweet_extended_text"),
    #col("retweeted_status.text").alias("reweeted_status_text"),
    #col("retweeted_status.extended_tweet.full_text").alias("reweeted_status_extended_text"),
    col("quoted_status.text").alias("quoted_status_text"),
    col("quoted_status.extended_tweet.full_text").alias("quoted_status_extended_text"),
    col("user.id").alias("user_id"),
    col("retweeted_status.user.id").alias("retweeted_user_id"),
    col("quoted_status.user.id").alias("quoted_user_id"))

    tweets_table.write.parquet(os.path.join(output_data,"Tweets"), 'overwrite')


def process_events(spark, input_data, output_data):
    """
       This function takes a spark session object, and pathes to input and output data.
       It then retrieves the song data from the inputs and builds dimension tables
       which are stored as parquet files on AWS S3.
       Input:
       - spark: spark-session object
       - input_data: path to input data
       - output_data: path to store data
    """

    event_file= os.path.join(input_data + "/ecb_event_data.csv")

    event_df = spark.read.csv(event_file)

    # Write user table to output bucket
    event_df.write.parquet(os.path.join(output_data, "Events"), 'overwrite')

def main():
    """
       This is the main function which defines the pathes to input and output datasets
       and calls the function to create the spark session and to process the songs
       and log data created above.
    """
    spark = create_spark_session()
    input_data = "s3a://" + S3_BUCKET_INPUT
    input_data_events = "s3a://" + S3_BUCKET_EVENTS
    output_data = "s3a://" + S3_BUCKET_OUTPUT


    process_tweets(spark, input_data, output_data)
    #process_events(spark, input_data_events, output_data)


if __name__ == "__main__":
    main()
