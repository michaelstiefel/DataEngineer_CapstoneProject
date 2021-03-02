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
