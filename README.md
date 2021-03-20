# Capstone Project: ETL Pipeline for streaming tweets

## Purpose

This Capstone project develops an ETL pipeline which loads raw .json files containing tweets from an S3 bucket, transforms and cleans them into a schema specified below and then stores them in another S3 bucket as parquet files.

Another data input source are monetary events. To this end, another data set is created containing

## Data

There are two main data sources: An S3 bucket containing up to date raw json files
with tweets from the Twitter API, and a file containing all monetary events. The code to generate those monetary events is in this repositry as well, so that events
can always be updated again.

## Overview of files

## How to use this repository

1. Add AWS access keys and secrets as well as the bucket information and e-mail to
the .cfg file
2. Run read_monetary_policy_decisions.py locally to scrape dates for the monetary policy
decision dates from the ecb website
3. Run create_monetary_event database which creates a data set from the monetary policy decision dates and speeches from ecb council members
4. Create EMR cluster on it, copy etl_pyspark.py and the cfg. file onto the cluster
5. Run etl_pyspark.py on cluster
