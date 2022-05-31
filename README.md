# About project

This project builds an ELT pipeline for the music streaming application Sparkify. Data will be extracted from an S3 bucket, transformed into fact and dimension tables using Apache Spark DataFrame API and loaded back to S3 in parquet format for efficient storage and retrieval.

## Files

+ `etl.py`  : This file reads song and log data from S3, transforms them into fact and dimension tables using spark and finally writes the tables back to S3 in parquet format.
+ `dl.cfg`  : Contains access key and secret access key to read/write data from/to S3

## How to run:

An S3 bucket with public access to objects must be created in us-west-2 region. Next, access key and secret access key id fields in the dl.cfg file
must be filled with a root IAM user. Finally, etl.py can be run in terminal.

## Author

[Arda Aras](https://www.linkedin.com/in/arda-aras/)
