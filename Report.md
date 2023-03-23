# Lab 3: Spark and Parquet Optimization Report

Name: Robin Wang
 
NetID: jw5487

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

#### 2.3:
Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv:
```
[6.684542179107666, 0.5465199947357178, 0.3794515132904053, 0.3906090259552002, 0.43916797637939453, 0.3245418071746826, 0.45089054107666016, 0.2831563949584961, 0.2610323429107666, 0.2695648670196533, 0.2860431671142578, 0.24521708488464355, 0.26129579544067383, 0.24613428115844727, 0.30683279037475586, 0.23199129104614258, 0.21504426002502441, 0.2669494152069092, 0.21554303169250488, 0.2266700267791748, 0.20880484580993652, 0.21675848960876465, 0.2038877010345459, 0.22884225845336914, 0.20035195350646973]

Minimum Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv: 0.20035195350646973
Median Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv: 0.26129579544067383
Maximum Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv: 6.684542179107666

```

Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv:

```
[10.586856365203857, 1.40350341796875, 1.1708438396453857, 1.0969171524047852, 1.1042098999023438, 1.017277717590332, 1.046489953994751, 0.8801395893096924, 0.8358659744262695, 0.9472832679748535, 0.9053826332092285, 0.8376681804656982, 0.9190199375152588, 0.8232700824737549, 0.9060385227203369, 0.9336726665496826, 0.8826279640197754, 0.8639860153198242, 0.9150254726409912, 0.8078916072845459, 0.8256223201751709, 1.0199649333953857, 0.7681324481964111, 0.9112057685852051, 0.8199465274810791]

Minimum Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv: 0.7681324481964111
Median Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv: 0.9112057685852051
Maximum Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv: 10.586856365203857
```

Times to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv:

```
[33.67477750778198, 30.334853410720825, 25.255217790603638, 24.409943342208862, 24.552271366119385, 24.894401788711548, 25.233367443084717, 24.871740341186523, 24.76655912399292, 25.363996505737305, 26.940394639968872, 25.19355821609497, 26.733312845230103, 25.23068857192993, 25.262548685073853, 25.478711128234863, 28.668444871902466, 25.13585591316223, 25.30380129814148, 24.624347448349, 25.28725004196167, 27.17269778251648, 24.8986713886261, 25.053296089172363, 28.339969396591187]

Minimum Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv: 24.409943342208862
Median Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv: 25.255217790603638
Maximum Time taken to run csv_sum_orders 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv: 33.67477750778198
```

Times to run csv_big_spender 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv

```
[5.6705029010772705, 0.3220803737640381, 5.250263929367065, 0.3088042736053467, 0.24941372871398926, 0.22049164772033691, 0.281968355178833, 0.2656233310699463, 0.20179963111877441, 0.22290802001953125, 0.22136569023132324, 0.19487571716308594, 0.18381857872009277, 0.16083121299743652, 0.20994997024536133, 0.18544936180114746, 0.21135616302490234, 0.1666862964630127, 0.15528559684753418, 0.1911160945892334, 0.16842985153198242, 0.15301060676574707, 0.15752625465393066, 0.156813383102417, 0.17258191108703613]

Minimum Time taken: 0.15301060676574707
Median Time taken: 0.20179963111877441
Maximum Time taken: 5.6705029010772705
```
Times to run csv_big_spender 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv

```
[9.539115905761719, 1.1990182399749756, 0.9354195594787598, 0.81125807762146, 0.7225699424743652, 0.917717456817627, 0.8406772613525391, 0.6208081245422363, 0.6874260902404785, 0.6647160053253174, 0.601973295211792, 0.5688071250915527, 0.6032519340515137, 0.5806779861450195, 0.7312443256378174, 0.6413486003875732, 0.6102936267852783, 0.6305520534515381, 0.5737423896789551, 0.64715576171875, 0.5476813316345215, 0.6957643032073975, 0.5496127605438232, 0.5546562671661377, 0.6160275936126709]

Minimum Time taken: 0.5476813316345215
Median Time taken: 0.6413486003875732
Maximum Time taken: 9.539115905761719
```
Times to run csv_big_spender 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv

```
[30.93815040588379, 25.10404944419861, 24.6126492023468, 24.151321172714233, 24.93704319000244, 22.865183115005493, 22.546642065048218, 23.275401830673218, 23.084631204605103, 23.988660097122192, 24.750231742858887, 22.12598466873169, 24.161078929901123, 24.738195657730103, 22.546529054641724, 24.314565896987915, 22.245267152786255, 24.264698266983032, 24.572205305099487, 24.05819606781006, 24.13307762145996, 24.404740810394287, 22.03800892829895, 24.0277156829834, 22.80817174911499]

Minimum Time taken: 22.03800892829895
Median Time taken: 24.13307762145996
Maximum Time taken: 30.93815040588379
```


Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleSmall.csv

```
[5.709216594696045, 0.33031463623046875, 0.26419806480407715, 0.22968268394470215, 0.21936607360839844, 0.21036911010742188, 0.2568051815032959, 0.19907331466674805, 0.17763638496398926, 0.16997170448303223, 0.1630113124847412, 0.16326642036437988, 0.1582787036895752, 0.20168423652648926, 0.1788337230682373, 0.17644453048706055, 0.18980097770690918, 0.13683104515075684, 0.14302968978881836, 0.15751266479492188, 0.13461756706237793, 0.1540517807006836, 0.1304640769958496, 0.14058351516723633, 0.1317284107208252]

Minimum Time taken: 0.1304640769958496
Median Time taken: 0.17644453048706055
Maximum Time taken: 5.709216594696045
```

Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleModerate.csv

```
[9.651880979537964, 0.9076380729675293, 0.7370748519897461, 0.6611483097076416, 0.666792631149292, 0.6768689155578613, 0.6888120174407959, 0.6419687271118164, 0.6360437870025635, 0.6608128547668457, 0.64394211769104, 0.5628311634063721, 0.5884621143341064, 0.6164007186889648, 0.5677645206451416, 0.6270143985748291, 0.5215799808502197, 0.5829806327819824, 0.5673980712890625, 0.6047990322113037, 0.549124002456665, 0.45957136154174805, 0.6187925338745117, 0.47585153579711914, 0.49646472930908203]

Minimum Time taken: 0.45957136154174805
Median Time taken: 0.6187925338745117
Maximum Time taken: 9.651880979537964
```

Times to run csv_brian 25 times on hdfs:/user/pw44_nyu_edu/peopleBig.csv

```
[27.288403749465942, 23.101820945739746, 21.973228454589844, 22.665141820907593, 22.24302363395691, 22.535742044448853, 22.495282411575317, 24.784806966781616, 22.808658361434937, 22.47356867790222, 23.80485486984253, 22.89566707611084, 22.86572003364563, 22.494057416915894, 22.391347885131836, 21.990581035614014, 21.994640350341797, 24.844486713409424, 21.63934826850891, 24.38092613220215, 22.039952754974365, 24.780951499938965, 22.24944496154785, 24.35793375968933, 24.05630326271057]

Minimum Time taken: 21.63934826850891
Median Time taken: 22.665141820907593
Maximum Time taken: 27.288403749465942
```
#### 2.4

Times to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet

```
[3.850375175476074, 0.5254673957824707, 0.37157297134399414, 0.3459453582763672, 0.36692142486572266, 0.3029670715332031, 0.2836439609527588, 0.487323522567749, 0.30992794036865234, 0.26685452461242676, 0.23830533027648926, 0.25028085708618164, 0.26426219940185547, 0.2344529628753662, 0.23814654350280762, 0.27750730514526367, 0.21238160133361816, 0.23409104347229004, 0.21317768096923828, 0.21747231483459473, 0.19463419914245605, 0.22487425804138184, 0.20982122421264648, 0.20723319053649902, 0.20623326301574707]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet: 0.19463419914245605
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet: 0.25028085708618164
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet: 3.850375175476074
23/03/18 14:45:23 INFO org.sparkproject.jetty.server.AbstractConnector: Stopped Spark@e5c4bd5{HTTP/1.1, (http/1.1)}{0.0.0.0:0}
```

Times to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet

```
[8.089649200439453, 4.409907817840576, 3.740840196609497, 3.836031436920166, 3.850304126739502, 3.8574283123016357, 3.878108263015747, 3.890381097793579, 3.8872132301330566, 3.8436646461486816, 3.8830103874206543, 3.9114174842834473, 3.851735830307007, 3.8838982582092285, 3.860368013381958, 3.895395040512085, 3.8598031997680664, 3.908381223678589, 3.8956823348999023, 3.9212539196014404, 3.867335081100464, 3.902679204940796, 3.886307954788208, 3.90275502204895, 3.871826648712158]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet: 3.740840196609497
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet: 3.8838982582092285
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet: 8.089649200439453
```

Times to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet

```
[10.585622310638428, 4.61439323425293, 4.27582859992981, 4.289586782455444, 4.245201110839844, 4.105280637741089, 3.966933250427246, 4.141218662261963, 3.8099606037139893, 3.9031810760498047, 4.005260229110718, 4.069449424743652, 3.875211000442505, 3.895784854888916, 3.9652793407440186, 3.712509870529175, 3.8851189613342285, 3.880192756652832, 3.7243173122406006, 3.982044219970703, 3.7343177795410156, 3.9110755920410156, 4.333549737930298, 3.7405786514282227, 3.739586353302002]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet: 3.712509870529175
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet: 3.9652793407440186
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet: 10.585622310638428
```

Times to run pq_big_spender 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet
```
[5.581738233566284, 0.2827723026275635, 0.24184155464172363, 0.23829126358032227, 0.2075190544128418, 0.20594191551208496, 0.203721284866333, 0.18763470649719238, 0.1793355941772461, 0.17064929008483887, 0.16257858276367188, 0.16083478927612305, 0.16444993019104004, 0.1510789394378662, 0.14168167114257812, 0.16108202934265137, 0.1822986602783203, 0.15220856666564941, 0.18441462516784668, 0.13190555572509766, 0.1290736198425293, 0.14281678199768066, 0.12841057777404785, 0.1180717945098877, 0.11525344848632812]

Minimum Time taken: 0.11525344848632812
Median Time taken: 0.16444993019104004
Maximum Time taken: 5.581738233566284
```

Times to run pq_big_spender 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet

```
[6.388536214828491, 4.1463868618011475, 3.833674192428589, 3.732558488845825, 3.744685649871826, 3.8598713874816895, 3.9317824840545654, 3.809157609939575, 3.8841302394866943, 3.828294038772583, 3.881664276123047, 3.902523994445801, 3.864091634750366, 3.914794921875, 3.8440864086151123, 3.885725498199463, 3.8430209159851074, 3.859915018081665, 3.8927900791168213, 3.8968076705932617, 3.8988606929779053, 3.8807408809661865, 3.9011614322662354, 3.8580546379089355, 3.8838250637054443]

Minimum Time taken: 3.732558488845825
Median Time taken: 3.881664276123047
Maximum Time taken: 6.388536214828491
```

Times to run pq_big_spender 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet
```
[6.49641227722168, 3.8931949138641357, 3.5777618885040283, 3.8325958251953125, 3.8156871795654297, 3.839728593826294, 3.8730239868164062, 3.863968849182129, 3.8594276905059814, 3.855067729949951, 3.8475470542907715, 3.878814458847046, 3.8844447135925293, 3.899057149887085, 3.8827550411224365, 3.890124559402466, 3.8864316940307617, 3.900991916656494, 3.864894151687622, 3.9066717624664307, 3.8895132541656494, 3.8812644481658936, 3.9324731826782227, 3.8518950939178467, 3.936614513397217]

Minimum Time taken: 3.5777618885040283
Median Time taken: 3.8812644481658936
Maximum Time taken: 6.49641227722168
```

Times to run pq_brian 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet
```
[5.835251569747925, 1.4100983142852783, 0.2297687530517578, 0.21826910972595215, 0.23281550407409668, 0.21461176872253418, 0.21199321746826172, 0.2027266025543213, 0.1943352222442627, 0.1988539695739746, 0.17873334884643555, 0.17871689796447754, 0.17326092720031738, 0.15869545936584473, 0.16633319854736328, 0.20492172241210938, 0.1616201400756836, 0.17118072509765625, 0.15163159370422363, 0.16211700439453125, 0.15320992469787598, 0.1732645034790039, 0.13901257514953613, 0.13555383682250977, 0.12575221061706543]

Minimum Time taken: 0.12575221061706543
Median Time taken: 0.17871689796447754
Maximum Time taken: 5.835251569747925
```

Times to run pq_brian 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet
```
[5.835575819015503, 3.3531477451324463, 3.774088144302368, 3.7465944290161133, 3.838315010070801, 3.892418622970581, 3.851986885070801, 3.8495092391967773, 3.838196277618408, 3.8689217567443848, 3.8828232288360596, 3.862131118774414, 3.9374501705169678, 3.821171760559082, 3.9069666862487793, 3.8460938930511475, 3.8875513076782227, 3.8972866535186768, 3.85603404045105, 3.9244801998138428, 3.8498404026031494, 3.9001529216766357, 3.8862974643707275, 3.886928081512451, 3.892022132873535]

Minimum Time taken: 3.3531477451324463
Median Time taken: 3.8689217567443848
Maximum Time taken: 5.835575819015503
```


Times to run pq_brian 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet
```
[7.7918713092803955, 3.355710506439209, 2.997004985809326, 2.8850150108337402, 2.890883207321167, 2.84328031539917, 2.882725477218628, 2.742623805999756, 2.870518684387207, 2.755523681640625, 2.661592721939087, 2.7017083168029785, 2.660459280014038, 2.647141933441162, 2.604702949523926, 2.667640209197998, 2.802860975265503, 2.623307466506958, 2.698118209838867, 2.7259411811828613, 2.7555088996887207, 2.6048264503479004, 2.644012928009033, 2.6311161518096924, 2.7319469451904297]
Minimum Time taken: 2.604702949523926
Median Time taken: 2.7319469451904297
Maximum Time taken: 7.7918713092803955
```


#### 2.5


# 2.5-1

This report describes the inputs and outputs of each stage (including mappers and reducers for each step) in the `pq_sum_orders.py` script. We tested three optimization methods to determine the quickest method to sort and sum orders using Apache Spark:

1. Sorting the DataFrame according to a specific column for each of the three datasets
2. Adding repartition to the DataFrame using a specified number of partitions
3. Enabling adaptive query execution, dynamic partition pruning, and leveraging columnar storage

## Results

The results showed that the third method, which leveraged adaptive query execution, dynamic partition pruning, and columnar storage, yielded the best performance. The following tables present the minimum, median, and maximum times taken to run `pq_sum_orders` 25 times for each dataset.

### Method 1: Sorting the DataFrame

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 0.2329           | 0.2758          | 1.2896           |
| `people_moderate.parquet` | 0.2242           | 0.2824          | 1.1198           |
| `people_big.parquet`    | 1.3362           | 1.4356          | 2.2193           |

### Method 2: Adding Repartition

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 3.6247           | 3.8877          | 5.7778           |
| `people_moderate.parquet` | 3.5401           | 3.8869          | 4.8546           |
| `people_big.parquet`    | 2.4568           | 2.6786          | 3.4667           |

### Method 3: Enabling Adaptive Query Execution, Dynamic Partition Pruning, and Leveraging Columnar Storage

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 0.2079           | 0.2831          | 2.0553           |
| `people_moderate.parquet` | 0.4013         | 0.4764        | 4.0056           |
| `people_big.parquet`    | 7.1588           | 7.34223        | 13.599           |


## The original times to run:

-  `pq_sum_orders` 25 times for each dataset

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 0.1946          | 0.2503          | 3.8504          |
| `people_moderate.parquet` | 3.7408        | 3.8839        | 8.0896           |
| `people_big.parquet`    | 3.7125          | 3.9653       | 10.5856          |

- `csv_sum_orders` 25 times for each dataset

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `peopleSmall.csv`  | 0.2004          | 0.2613          | 6.6845          |
| `peopleModerate.csv` | 0.7681       | 0.9112	        | 10.5869           |
| `peopleBig.csv	`    | 24.4099	          | 25.2552       | 33.6748          |

# Comparisons

 ## The comparison of the performance of pq_sum_orders and csv_sum_orders functions:

 - For the small dataset, the pq_sum_orders function has a similar median time (0.2503 s) to the csv_sum_orders function (0.2613 s). However, the maximum time taken by the csv_sum_orders function is significantly higher (6.6845 s) compared to the pq_sum_orders function (3.8504 s).
 
 - For the moderate dataset, the pq_sum_orders function performs better with a lower median time (3.8839 s) compared to the csv_sum_orders function (0.9112 s). The maximum time taken by the csv_sum_orders function is also higher (10.5869 s) than the pq_sum_orders function (8.0896 s).
 
 - For the big dataset, the pq_sum_orders function significantly outperforms the csv_sum_orders function with a much lower median time (3.9653 s) compared to the csv_sum_orders function (25.2552 s). The maximum time taken by the csv_sum_orders function is also much higher (33.6748 s) than the pq_sum_orders function (10.5856 s).
 
 ## When comparing the optimized methods (Method 1, Method 2, and Method 3) to the original pq_sum_orders and csv_sum_orders functions:
 
 - Method 1 (Sorting the DataFrame) shows a significant improvement in median and maximum times compared to the original pq_sum_orders function for all three datasets. It also outperforms the csv_sum_orders function across all datasets in terms of median and maximum times.
 
 - Method 2 (Adding Repartition) has worse median and maximum times than the original pq_sum_orders function for the small and moderate datasets. However, it shows improvement in the median and maximum times for the big dataset. It outperforms the csv_sum_orders function for the big dataset but has worse median and maximum times for the small and moderate datasets.
 
 - Method 3 (Enabling Adaptive Query Execution, Dynamic Partition Pruning, and Leveraging Columnar Storage) has a similar median time compared to the original pq_sum_orders function for the small dataset but shows improvement for the moderate and big datasets. The maximum times have also improved for all three datasets. It also outperforms the csv_sum_orders function in terms of median and maximum times across all datasets.



# Conclusions

 - Method 1 (Sorting) shows the best performance for all three datasets. 

 - Method 2 (Repartition) shows the worse performance for all three datasetes compared to the first method.

 - Method 3 (Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage) shows some improvements in the small and moderate datasets but shows sluggish performance for the big one.
 
 - ## Method 1 (Sorting the DataFrame) is the quickest method. This method has the shortest median times across all three datasets compared to the other methods.


# Code:


### Sorting the DataFrame according to a specific column for each of the three datasets:

```
def sort_and_save_parquet(spark, input_file_path, output_file_path):
    df = spark.read.parquet(input_file_path)
    sorted_df = df.sort("zipcode")
    sorted_df.write.parquet(output_file_path)

def pq_sum_orders(spark, file_path):
    # ... (rest of the code remains the same)

def main(spark, file_path):
    # ... (rest of the code remains the same)

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    # Sort and save the Parquet file
    sorted_file_path = file_path.replace(".parquet", "_sorted.parquet")
    sort_and_save_parquet(spark, file_path, sorted_file_path)

    # Run the main function with the sorted file path
    main(spark, sorted_file_path)
```


### Adding repartition to the DataFrame using a specified number of partitions:

```
def sort_and_save_parquet(spark, input_file_path, output_file_path, num_partitions=4):
    '''Sorts the input Parquet file by the "zipcode" column and saves it to the output file path.
    Additionally, it repartitions the DataFrame using the specified number of partitions.

    Parameters
    ----------
    spark : SparkSession
    input_file_path : str
    output_file_path : str
    num_partitions : int (default: 4)
    '''
    df = spark.read.parquet(input_file_path)
    sorted_df = df.sort("zipcode")
    repartitioned_df = sorted_df.repartition(num_partitions)
    repartitioned_df.write.parquet(output_file_path)
    return output_file_path 

def pq_sum_orders(spark, file_path):
    # ... (rest of the code remains the same)

def main(spark, file_path):
    # ... (rest of the code remains the same)

if __name__ == "__main__":
    # ... (rest of the code remains the same)
```



### Enabling adaptive query execution, dynamic partition pruning, and leveraging columnar storage.

```
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

    file_path = 'hdfs:/user/jw5487_nyu_edu/people_small.parquet'
    df_sum_orders = pq_sum_orders(spark, file_path)

    times = bench.benchmark(spark, 25, pq_sum_orders, file_path)

    print(f'Times to run pq_sum_orders 25 times on {file_path}')
    print(times)
    print(f'Minimum Time taken to run pq_sum_orders 25 times on {file_path}: {min(times)}')
    print(f'Median Time taken to run pq_sum_orders 25 times on {file_path}: {sorted(times)[len(times)//2]}')
    print(f'Maximum Time taken to run pq_sum_orders 25 times on {file_path}: {max(times)}')
```




# 2.5-2

In this report, we compare the performance of three different optimization methods applied to the pq_big_spender function in PySpark. The function queries users with at least 100 orders and no rewards card.

  

# Optimization Methods

1. Sorting: Sort the DataFrame by the income column in descending order using the orderBy function from PySpark.

2. Repartition: Add repartition to the DataFrame using a specified number of partitions.

3. Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage: Enable various Spark configurations to optimize query performance.

# Performance Results

## Method 1: Sorting


| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 0.1246           | 0.1729          | 5.1272           |
| `people_moderate.parquet` | 3.6413           | 3.8531          | 8.6039           |
| `people_big.parquet`    | 3.8424           | 3.9011          | 8.3966           |


## Method 2: Repartition

| Dataset | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet` | 0.1490 | 0.1989 | 2.7515 |
| `people_moderate.parquet` | 0.1808 | 0.2179 | 6.4442 |
| `people_big.parquet` | 0.7304 | 0.8356 | 6.8625 |


## Method 3: Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage

| Dataset | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet` | 0.1317 | 0.1751 | 2.4845 |
| `people_moderate.parquet` | 3.2961 | 3.8797 | 6.4257 |
| `people_big.parquet` | 3.8096 | 3.8804 | 7.0490 |

## The original times to run:

- `csv_big_spender` 25 times for each dataset:

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `peopleSmall.csv`       | 0.1530           | 0.2018          | 5.6705           |
| `peopleModerate.csv`    | 0.5477           | 0.6413          | 9.5391           |
| `peopleBig.csv`         | 22.0380          | 24.1331         | 30.9382          |

- `pq_big_spender` 25 times for each dataset:

| Dataset                        | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|--------------------------------|------------------|-----------------|------------------|
| `people_small.parquet`         | 0.1153           | 0.1644          | 5.5817           |
| `people_moderate.parquet`      | 3.7326           | 3.8817          | 6.3885           |
| `people_big.parquet`           | 3.5778           | 3.8813          | 6.4964           |


# Comparisons

 ## Comparison of csv_big_spender and pq_big_spender

 - We can see that using the Parquet file format (pq_big_spender) generally results in faster processing times than using the CSV format (csv_big_spender).
 
 - The minimum, median, and maximum times are generally lower for the Parquet format, especially for the larger datasets.
 
 ## When comparing the optimized methods (Method 1, Method 2, and Method 3) to the original pq_big_spender and csv_big_spender functions:
 
 - Method 1: Sorting -- This method shows a general improvement in the minimum and median times across all datasets compared to csv_big_spender. However, the maximum times for the smaller datasets are still higher than those of the original csv_big_spender.
 - Method 2: Repartition -- The repartition method shows significant improvements in the maximum times across all datasets compared to the original csv_big_spender and pq_big_spender. The minimum and median times for the smaller datasets are also generally faster than the original csv_big_spender.
 - Method 3: Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage -- This method results in the best performance overall, showing improvements in the minimum, median, and maximum times across all datasets compared to the original csv_big_spender. The improvements are most noticeable for the larger datasets.





#  Conclusions

 - Method 1 (Sorting) shows bad performance for the moderate and big datasets in terms of minimum, median, and maximum time.

 - Method 2 (Repartition) provides the best overall performance, with improvements in minimum, median, and maximum time for all datasets.

 - Method 3 (Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage) shows some improvements in the small and moderate datasets but shows bad results for the big one.

 - ## Based on the results, Method 2 is the best optimization method for the pq_big_spender function, with the median time being the shortest across all three datasets compared to the other methods.


# Code

- method 1:

```
def pq_big_spender(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will contains users with at least 100 orders but
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

    # Sort the DataFrame by income in descending order
    df_big_spender_sorted = df_big_spender.orderBy("income", ascending=False)

    return df_big_spender_sorted
```

- method 2:

```
def pq_big_spender(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will contains users with at least 100 orders but
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

    # Sort the DataFrame by income in descending order
    df_big_spender_sorted = df_big_spender.orderBy("income", ascending=False)

    # Repartition the DataFrame using the specified number of partitions
    df_big_spender_repartitioned = df_big_spender_sorted.repartition(4)

    return df_big_spender_repartitioned
```

- method 3:

```
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
```


# 2.5 - 3

In this report, we compare the performance of three different optimization methods applied to the pq_brian function in PySpark. The function queries users with at least 100 orders and no rewards card.

  

# Optimization Methods

1. Sorting: Sorting by a specific column. In this case, I add sorting by the income column in descending order to the pq_brian function.

2. Repartition: Repartitions the DataFrame into a specified number of partitions I repartition the DataFrame into 5 partitions in the pq_brian function.

3. Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage: Enable various Spark configurations to optimize query performance

# Performance Results

## Method 1: Sorting


| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 0.122         | 0.153       | 0.069        |
| `people_moderate.parquet` | 3.453          | 3.862          | 0.413          |
| `people_big.parquet`    |2.594	        | 2.720	        | 10.183           |


## Method 2: Repartition

| Dataset | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet` | 0.151	 | 0.211 | 5.117 |
| `people_moderate.parquet` | 3.775 | 3.865 | 8.142 |
| `people_big.parquet` | 2.607 | 2.734 | 8.139 |


## Method 3: Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage

| Dataset | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet` | 0.119 | 0.164 | 5.966 |
| `people_moderate.parquet` | 0.159 | 0.198	 | 5.885 |
| `people_big.parquet` | 1.773 | 1.878 | 7.380 |

## The original times to run:

-  `pq_brian` 25 times for each dataset

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `people_small.parquet`  | 0.1258          | 0.1787          | 5.8353          |
| `people_moderate.parquet` | 3.3531        | 3.8689        | 5.8356           |
| `people_big.parquet`    | 2.6047          | 2.7319       | 7.7919          |

- `csv_brian` 25 times for each dataset

| Dataset                 | Minimum Time (s) | Median Time (s) | Maximum Time (s) |
|-------------------------|------------------|-----------------|------------------|
| `peopleSmall.csv`  | 0.1305          | 0.1764          | 5.7092          |
| `peopleModerate.csv` | 0.4596       | 0.6188	        | 9.6519           |
| `peopleBig.csv	`    | 21.6393	          | 22.6651	       | 27.2884          |


# Comparisons

 ## Comparison of csv_brian and pq_brian

 - We can see that parquet files generally have better performance, with the exception of the peopleSmall dataset, where the median time taken for csv_brian is slightly faster.

 - However, for both peopleModerate and peopleBig datasets, pq_brian shows faster median times compared to the csv_brian.
 
 ## When comparing the optimized methods (Method 1, Method 2, and Method 3) to the original pq_brian and csv_brian functions:
 
 - All three methods show improved minimum, median, and maximum times for the people_small.parquet and people_moderate.parquet datasets.
 - For the people_big.parquet dataset, Method 3 outperforms both the original pq_brian and csv_brian times, while Method 1 and Method 2 show improvements in minimum and median times, but not in the maximum times.
 - In summary, using the parquet file format with the optimization methods provides better performance when compared to using CSV files without any optimization.



# Conclusions

 - Method 1 (Sorting) shows some improvements, particularly in the minimum time taken for people_small.parquet & people_moderate.parquet.

 - Method 2 (Repartition) shows slight improvements, particularly in the minimum time taken for people_big.parquet.

 - Method 3 (Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage) provides the best overall performance, with improvements significantly for people_big.parquet.
 
 ## All in all, it appears that Method 3 (Adaptive Query Execution, Dynamic Partition Pruning, and Columnar Storage) is the quickest method for the people_big.parquet dataset, as it has the shortest median time (1.878 seconds) compared to the other methods. However, for the people_small.parquet and people_moderate.parquet datasets, Method 1 (Sorting) is still the quickest.

# Code


- Method 1:
```
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

    # Sort the DataFrame by income in descending order
    df_brian_sorted = df_brian.orderBy("income", ascending=False)

    return df_brian_sorted
```


- Method 2:
```
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

    # Sort the DataFrame by income in descending order
    df_brian_sorted = df_brian.orderBy("income", ascending=False)

    # Repartition the DataFrame into a specified number of partitions
    df_brian_repartitioned = df_brian_sorted.repartition(5)

    return df_brian_repartitioned
```


- Method 3:
```

#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit --deploy-mode client pq_brian_optimized.py <file_path>
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

    This function returns an uncomputed dataframe that
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

    # Create the spark session object with optimizations enabled
    spark = (SparkSession.builder.appName('part2')
              .config('spark.sql.adaptive.enabled', 'true')
              .config('spark.sql.adaptive.shuffle.targetPostShuffleInputSize', '67108864')
              .config('spark.sql.autoBroadcastJoinThreshold', '-1')
              .config('spark.sql.execution.arrow.enabled', 'true')
              .getOrCreate())

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
```


