import configparser
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib
matplotlib.use('agg',warn=False, force=True)
from matplotlib import pyplot as plt



config = configparser.ConfigParser()
config.read("capstone_project.cfg")


S3_BUCKET_INPUT = config['AWS']['S3_BUCKET_INPUT']
S3_BUCKET_OUTPUT = config['AWS']['S3_BUCKET_OUTPUT']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']






def build_timeline_of_tweets(spark, input_data, output_data):
    """
    This function takes the spark object and the input and output s3 buckets.
    It then counts the number of (English) tweets per day, plots them in matplotlib and
    saves the plot to the "analysis folder" in the s3 bucket.
    """

    tweets = spark.read.parquet(os.path.join(input_data, "Tweets"))
    tweets = tweets.filter(tweets.tweet_lang == "en")
    tweets = tweets.select("tweet_created_at")

    # Convert to pandas
    df = tweets.toPandas()


    list_of_dates = df['tweet_created_at'].tolist()
    idx = pd.DatetimeIndex(list_of_dates)
    ones = np.ones(len(list_of_dates))

    my_series = pd.Series(ones, index=idx)
    per_day = my_series.resample('D').sum().fillna(0)

    fig, ax = plt.subplots()
    ax.grid = True
    ax.set_title("Tweet Frequencies")

    # Define time range
    datemin = datetime(2020, 10, 25, 0, 0)
    datemax = datetime(2021, 3, 15, 0, 0)

    ax.plot(per_day.index, per_day)


    plt.savefig(output_data, "tweet_time_series.png")


def create_spark_session():
    """
       This function creates the SparkSession with the needed dependencies.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def main():
    """
        This is the main function. It calls the functions which deliver and save
        the analysis.
    """
    spark = create_spark_session()
    input_data = "s3a://" + S3_BUCKET_OUTPUT

    output_data = "s3a://" + S3_BUCKET_OUTPUT


    build_timeline_of_tweets(spark, input_data, output_data)



if __name__ == "__main__":
    main()
