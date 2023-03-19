#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_brian.py <file_path>
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


def pq_brian(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will filters down to only include people with `first_name`
    of 'Brian' that are not yet in the loyalty program

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/peopleSmall.parquet`

    Returns
    df_brian:
        Uncomputed dataframe that only has people with 
        first_name of 'Brian' and not in the loyalty program
    '''
    people = spark.read.parquet(file_path)

    people.createOrReplaceTempView('people')

    df_brian = spark.sql("SELECT * FROM people WHERE first_name = 'Brian' AND loyalty = 'false'")

    return df_brian



def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    times = bench.benchmark(spark, 25, pq_brian, file_path)

    print(f'Times to run pq_brian 25 times on {file_path}')
    print(times)
    print(f'Minimum Time taken: {min(times)}')
    print(f'Median Time taken: {sorted(times)[len(times)//2]}')
    print(f'Maximum Time taken: {max(times)}')




# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
