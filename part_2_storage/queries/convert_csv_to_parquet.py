#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql import SparkSession


def csv_to_parquet(spark, input_file_path, output_file_path):
    df = spark.read.csv(input_file_path, header=True,
                        schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    df.write.parquet(output_file_path)


if __name__ == "__main__":
    # Create the spark session object
    spark = SparkSession.builder.appName('csv_to_parquet').getOrCreate()

    # Convert and store each CSV file as a Parquet file
    csv_to_parquet(spark, 'hdfs:/user/pw44_nyu_edu/peopleSmall.csv', 'hdfs:/user/jw5487_nyu_edu/people_small.parquet')
    csv_to_parquet(spark, 'hdfs:/user/pw44_nyu_edu/peopleModerate.csv', 'hdfs:/user/jw5487_nyu_edu/people_moderate.parquet')
    csv_to_parquet(spark, 'hdfs:/user/pw44_nyu_edu/peopleBig.csv', 'hdfs:/user/jw5487_nyu_edu/people_big.parquet')
