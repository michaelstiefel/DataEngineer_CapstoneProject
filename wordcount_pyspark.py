import configparser
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from nltk.corpus import stopwords
import string
import sys
#from importlib import reload
#reload(sys)
#sys.setdefaultencoding('utf-8')


config = configparser.ConfigParser()
config.read("capstone_project.cfg")

#AWS_ACCESS_KEY_ID =config['AWS']['AWS_ACCESS_KEY_ID']
#AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
S3_BUCKET_INPUT = config['AWS']['S3_BUCKET_INPUT']
S3_BUCKET_OUTPUT = config['AWS']['S3_BUCKET_OUTPUT']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


punct = list(string.punctuation)
stopword_list = stopwords.words('english') + punct + ['rt', 'via', 'ecb', 'lagarde', 'easycop', 'us', 'users']
    #'@lagarde', '@ecb',
    #'central', 'bank', '1x', '@easycopbots', '@easycopbots:', 'european', 'euro', 'christine', 'digital', '#ecb', 'president', 'bitcoin',
    #'#bitcoin', 'today', 'says',
    # '&amp;', 'ecb\'s', 'new']



def count_words(spark, input_data, output_data, stopwords = stopword_list):
    """
    This function takes the spark object and the input and output s3 buckets as well as a list of
    stopwords. It then creates a textfile with a list of the most common words in the
    tweets and saves this object to the "analysis folder" in the s3 bucket.
    """
    tweets = spark.read.parquet(os.path.join(input_data, "Tweets"))
    tweets = tweets.filter(tweets.tweet_lang == "en")
    tweets = tweets.withColumn("all_text", F.when(tweets['tweet_extended_text'].isNull(), tweets['tweet_text']) .otherwise(tweets['tweet_extended_text']))
    all_tweets = tweets.select('all_text').rdd
    split_rdd = all_tweets.flatMap(lambda x: x.all_text.split())
    split_rdd_clean = split_rdd.map(lambda x: x.lower())
    split_rdd_clean = split_rdd_clean.filter(lambda x: x not in stopword_list)
    split_rdd_clean = split_rdd_clean.map(lambda x: (x, 1))
    split_rdd_clean = split_rdd_clean.reduceByKey(lambda x, y: x+y)
    split_rdd_clean = split_rdd_clean.map(lambda x: (x[1], x[0]))
    split_rdd_clean = split_rdd_clean.sortByKey(ascending = False)
    split_rdd_clean = split_rdd_clean.map(lambda x: (x[1], x[0]))
    output = spark.sparkContext.parallelize(split_rdd_clean.take(200))
    output.coalesce(1).saveAsTextFile(os.path.join(output_data, "Analysis/wordcount"))


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


    count_words(spark, input_data, output_data)



if __name__ == "__main__":
    main()
