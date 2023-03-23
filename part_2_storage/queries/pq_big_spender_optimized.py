#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_big_spender.py <file_path>
'''

# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

def csv_to_parquet(spark, input_file_path, output_file_path):
    df = spark.read.csv(input_file_path, header=True,
                        schema='first_name STRING, last_name STRING, age INT, income FLOAT, zipcode INT, orders INT, loyalty BOOLEAN, rewards BOOLEAN')
    df.write.parquet(output_file_path)

def pq_big_spender(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns an uncomputed dataframe that
    will contain users with at least 100 orders but
    do not yet have a rewards card.

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet`

    Returns
    df_big_spender:
        Uncomputed dataframe of the maximum income grouped by last_name
    '''
    people = spark.read.parquet(file_path)

    people.createOrReplaceTempView('people')

    df_big_spender = spark.sql("SELECT * FROM people WHERE orders >= 100 AND rewards = 'false'")

    return df_big_spender

def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, pq_big_spender, file_path)

    print(f'Times to run pq_big_spender 25 times on {file_path}')
    print(times)
    print(f'Minimum Time taken: {min(times)}')
    print(f'Median Time taken: {sorted(times)[len(times)//2]}')
    print(f'Maximum Time taken: {max(times)}')

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2') \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "67108864") \
                .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
                .config("spark.sql.execution.arrow.enabled", "true") \
                .config("spark.sql.adaptive.skewedJoin.enabled", "true") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
