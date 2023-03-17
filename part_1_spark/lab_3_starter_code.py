#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py
'''
import os


# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def main(spark, userID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    userID : string, userID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/jw5487_nyu_edu/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/jw5487_nyu_edu/sailors.json')
    reserves = spark.read.json(f'hdfs:/user/jw5487_nyu_edu/reserves.json')


    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    reserves.createOrReplaceTempView('reserves')

    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    #question_1_query = ....
    print('Question 1: ')
    spark.sql('SELECT sid, sname, age FROM sailors WHERE age > 40').show()

    #question_2_query = ....
    print('Question 2: ')
    from pyspark.sql.functions import count

    reserves = spark.read.json('reserves.json')
    res1 = reserves.filter(reserves.bid != 101).groupBy('sid').agg(count('bid').alias('count'))
    res1.show()



    #question_3_query = ....
    print('Question 3: ')
    res2 = spark.sql(' SELECT s.sid, s.sname, COUNT(DISTINCT r.bid) AS distinct_boats FROM sailors s JOIN reserves r ON s.sid = r.sid GROUP BY s.sid, s.sname').show()


    #question_4_query = ....
    print('Question 4: ')

    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
    from pyspark.sql.functions import percentile_approx, max, collect_list, count, avg

    # Define schema for artist_term.csv
    artist_term_schema = StructType([
        StructField("artistID", StringType(), True),
        StructField("term", StringType(), True),
    ])

    # Define schema for tracks.csv
    tracks_schema = StructType([
        StructField("trackID", StringType(), True),
        StructField("title", StringType(), True),
        StructField("release", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("duration", FloatType(), True),
        StructField("artistID", StringType(), True),
    ])

    # Load artist_term.csv and tracks.csv as DataFrames

    artist_term_df = spark.read.csv('hdfs:/user/jw5487_nyu_edu/artist_term.csv', header=False, schema=artist_term_schema)
    tracks_df = spark.read.csv('hdfs:/user/jw5487_nyu_edu/tracks.csv', header=False, schema=tracks_schema)


    # Compute median year of release, maximum track duration, and total number of artists for each term
    tracks_years_df = tracks_df.join(artist_term_df, "artistID")
    tracks_years_df = tracks_years_df.withColumn("year", tracks_years_df["year"].cast(IntegerType()))
    tracks_years_df.groupBy("term").agg(
        max("duration").alias("max(duration)"),
        collect_list("year").alias("years"),
        count("artistID").alias("count(artistID)"),
        percentile_approx("year", 0.5).alias("median_year")
    )

    # Compute average track duration for each term
    term_durations = tracks_years_df.groupBy("term").agg(avg("duration").alias("avg(duration)"))

    # Join the two DataFrames and sort by average track duration
    result_df = term_durations.join(tracks_years_df, "term").orderBy("avg(duration)").limit(10)

    # Show the results
    result_df.show()


    #question_5_query = ....
    print('Question 5: ')
    from pyspark.sql.functions import countDistinct

    tracks_by_term = tracks_df.join(artist_term_df, "artistID")\
                            .groupBy("term")\
                            .agg(countDistinct("trackID").alias("distinct_tracks"))\
                            .orderBy("distinct_tracks", ascending=False)

    top_terms = tracks_by_term.limit(10)

    top_terms.show()

    bottom_terms = tracks_by_term.orderBy("distinct_tracks", ascending=True).limit(10)

    bottom_terms.show()




# Only enter this block if we're in main
if __name__ == "__main__":

        # Create the spark session object
        spark = SparkSession.builder.appName('part1').getOrCreate()

        # Get user userID from the command line
        # We need this to access the user's folder in HDFS
        userID = os.environ['USER']

        # Call our main routine
        main(spark, userID)


