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


def check_col_not_in_df(df, col):
    """
       Simple helper function to check whether col is not in df
    """
    if col in df.columns:
        return 0
    else:
        return 1



def check_users(spark, data):
    """
        This file reads in the output users data of the etl_pyspark pipeline and
        checks for consistency: Does the data contain all the required columns?
    """

    users = spark.read.parquet(os.path.join(data, "Users"))

    user_cols_to_contain = ['user_id', 'user_created_at', 'user_screen_name',
    'user_verified', 'user_description', 'user_followers_count', 'user_friends_count']

    helper_users = 0
    for colname in user_cols_to_contain:
        helper_users += check_col_not_in_df(users, colname)
    if helper_users > 0:
        print("Users df did not pass data quality check")
        assert False
    if helper_users == 0:
        print("Users df did pass data quality check")
        assert True



def check_tweets(spark, data):
    """
        This file reads in the output tweet data of the etl_pyspark pipeline and
        checks for consistency: Does the data contain all the required columns?
    """

    tweets = spark.read.parquet(os.path.join(data, "Tweets"))

    tweets_cols_to_contain = ['tweet_id', 'tweet_created_at', 'tweet_lang',
    'tweet_text', 'tweet_extended_text', 'quoted_status_text',
    'quoted_status_extended_text', 'user_id', 'retweeted_user_id',
    'quoted_user_id']

    helper_tweets = 0
    for colname in tweets_cols_to_contain:
        helper_tweets += check_col_not_in_df(tweets, colname)
    if helper_tweets > 0:
        print("Tweets df did not pass data quality check")
        assert False
    if helper_tweets == 0:
        print("Tweets df passed data quality check")
        assert True

def check_events(spark, data):
    """
        This file reads in the output event data of the etl_pyspark pipeline and
        checks for consistency: Does the data contain all the required columns?
    """

    events = spark.read.parquet(os.path.join(data, "Events"))

    events_cols_to_contain = ["type", "date", "speakers"]

    helper_events = 0
    for colname in events_cols_to_contain:
        helper_events += check_col_not_in_df(events, colname)
    if helper_events > 0:
        print("Events df did not pass data quality check")
        assert False
    if helper_events == 0:
        print("Events df passed data quality check")
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


    check_users(spark, output_data)
    check_tweets(spark, output_data)
    check_events(spark, output_data)


if __name__ == "__main__":
    main()
