#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_sum_orders.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def pq_sum_orders(spark, file_path):
    people = spark.read.parquet(file_path)
    df_sum_orders = people.groupBy("zipcode").sum("orders")
    return df_sum_orders

if __name__ == "__main__":
    # Create the spark session object
    spark = SparkSession.builder \
            .appName('pq_sum_orders_optimized') \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.cbo.enabled", "true") \
            .config("spark.sql.join.preferSortMergeJoin", "true") \
            .getOrCreate()

    file_path = 'hdfs:/user/jw5487_nyu_edu/people_big.parquet'
    df_sum_orders = pq_sum_orders(spark, file_path)

    times = bench.benchmark(spark, 25, pq_sum_orders, file_path)

    print(f'Times to run pq_sum_orders 25 times on {file_path}')
    print(times)
    print(f'Minimum Time taken to run pq_sum_orders 25 times on {file_path}: {min(times)}')
    print(f'Median Time taken to run pq_sum_orders 25 times on {file_path}: {sorted(times)[len(times)//2]}')
    print(f'Maximum Time taken to run pq_sum_orders 25 times on {file_path}: {max(times)}')






