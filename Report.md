# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

     spark.sql('SELECT sid, sname, age FROM sailors WHERE age > 40').show()

```


Output:
```

Question 1: 
+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

    from pyspark.sql.functions import count

    reserves = spark.read.json('reserves.json')
    res1 = reserves.filter(reserves.bid != 101).groupBy('sid').agg(count('bid').alias('count'))
    res1.show()



```


Output:
```

Question 2: 
+---+-----+
|sid|count|
+---+-----+
| 22|    3|
| 31|    3|
| 74|    1|
| 64|    1|
+---+-----+

```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

    res2 = spark.sql(' SELECT s.sid, s.sname, COUNT(DISTINCT r.bid) AS distinct_boats FROM sailors s JOIN reserves r ON s.sid = r.sid GROUP BY s.sid, s.sname').show()

```


Output:
```

Question 3: 
+---+-------+--------------+
|sid|  sname|distinct_boats|
+---+-------+--------------+
| 64|horatio|             2|
| 22|dusting|             4|
| 31| lubber|             3|
| 74|horatio|             1|
+---+-------+--------------+

```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

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

```


Output:
```

Question 4: 
+----------------+------------------+------------------+------------------+--------------------+--------------------+----+--------+
|            term|     avg(duration)|          artistID|           trackID|               title|             release|year|duration|
+----------------+------------------+------------------+------------------+--------------------+--------------------+----+--------+
|       mope rock|13.661589622497559|ARLLYSS11F50C50D4C|TRBMWZO128F933F271|           Que faire|       Verite divine|   0|13.66159|
|      murder rap| 15.46403980255127|ARMKICE11F4C846A64|TRDDTYQ128F9334C26|Thug Disease (intro)|Spice 1 Presents....|   0|15.46404|
|experimental rap| 25.91301918029785|AR9PHUI1187B98C8B2|TRGYKKI12903C9538C|            21 to 35|      Lost For Words|2000|25.91302|
|    abstract rap| 25.91301918029785|AR9PHUI1187B98C8B2|TRGYKKI12903C9538C|            21 to 35|      Lost For Words|2000|25.91302|
|     ghetto rock|26.461589813232422|AR7NMRP1187B99879F|TRCETNH128F4267A42|  DWZ Message (Skit)|       The Infektion|   0|26.46159|
|  brutal rapcore|26.461589813232422|AR7NMRP1187B99879F|TRCETNH128F4267A42|  DWZ Message (Skit)|       The Infektion|   0|26.46159|
|     punk styles| 41.29914093017578|ARLE7OP1187FB39A50|TREJJBK128F425B992|           Polisstat|Network Of Friends 1|   0|41.29914|
|     turntablist| 43.32922387123108|ARDYKF31187FB3C18C|TRAHGDK128F422FBA9|             Pioneer|The Night Before ...|1993|34.40281|
|     turntablist| 43.32922387123108|ARDYKF31187FB3C18C|TRAZMKY128F422FBE1|                 Oki|The Night Before ...|1993|38.76526|
|     turntablist| 43.32922387123108|ARDYKF31187FB3C18C|TRGXXRB128F422FBB2|    Toyo Engineering|The Night Before ...|1993|62.14485|
+----------------+------------------+------------------+------------------+--------------------+--------------------+----+--------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

Code:
```
    from pyspark.sql.functions import countDistinct

    tracks_by_term = tracks_df.join(artist_term_df, "artistID")\
                            .groupBy("term")\
                            .agg(countDistinct("trackID").alias("distinct_tracks"))\
                            .orderBy("distinct_tracks", ascending=False)

    top_terms = tracks_by_term.limit(10)

    top_terms.show()

    bottom_terms = tracks_by_term.orderBy("distinct_tracks", ascending=True).limit(10)

    bottom_terms.show()

```

Output:
```
Question 5: 
+----------------+---------------+
|            term|distinct_tracks|
+----------------+---------------+
|            rock|          21796|
|      electronic|          17740|
|             pop|          17129|
|alternative rock|          11402|
|         hip hop|          10926|
|            jazz|          10714|
|   united states|          10345|
|        pop rock|           9236|
|     alternative|           9209|
|           indie|           8569|
+----------------+---------------+

+---------------+---------------+
|           term|distinct_tracks|
+---------------+---------------+
|    czech metal|              1|
|   power groove|              1|
|classic uk soul|              1|
|   pukkelpop 07|              1|
|    swedish rap|              1|
|indie argentina|              1|
| swedish artist|              1|
|toronto hip hop|              1|
|    cowboy rock|              1|
|classical tango|              1|
+---------------+---------------+

```
## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?

Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
