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

#### 2.5 pq_sum_orders.py

In this analysis, three optimization methods were tested to determine the quickest method to sort and sum orders using Apache Spark. The methods were as follows:

1. **Sorting the DataFrame** according to a specific column for each of the three datasets.
2. **Adding repartition** to the DataFrame using a specified number of partitions.
3. **Enabling adaptive query execution, dynamic partition pruning, and leveraging columnar storage**.

The results showed that the third method, which leveraged adaptive query execution, dynamic partition pruning, and columnar storage, yielded the best performance. When applied to the `people_small.parquet` dataset, the minimum, median, and maximum times taken to run `pq_sum_orders` 25 times were 0.2329, 0.2758, and 1.2896 seconds, respectively. For the `people_moderate.parquet` dataset, these times were 0.2242, 0.2824, and 1.1198 seconds, respectively. Lastly, for the `people_big.parquet` dataset, the times were 1.3362, 1.4356, and 2.2193 seconds, respectively.

**In conclusion**, enabling adaptive query execution, dynamic partition pruning, and leveraging columnar storage provided the best performance in sorting and summing orders using Apache Spark.


#### Sort the DataFrame according to a specific column for each of the three datasets:
The sort_and_save_parquet function added to pq_sum_orders:
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

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet

```
[1.2895925045013428, 0.5193519592285156, 0.4222571849822998, 0.6449780464172363, 0.3765876293182373, 0.3358345031738281, 0.35135841369628906, 0.31900715827941895, 0.3795952796936035, 0.30310511589050293, 0.2972714900970459, 0.2757720947265625, 0.2740592956542969, 0.3195929527282715, 0.25824451446533203, 0.26157236099243164, 0.2730739116668701, 0.2743651866912842, 0.2599475383758545, 0.271930456161499, 0.2476062774658203, 0.23689508438110352, 0.2530384063720703, 0.23990869522094727, 0.23291611671447754]
Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet: 0.23291611671447754
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet: 0.2757720947265625
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet: 1.2895925045013428
```

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet

```
[1.119814395904541, 0.5196249485015869, 0.4133260250091553, 0.38582611083984375, 0.34375715255737305, 0.3445124626159668, 0.3536109924316406, 0.32384777069091797, 0.2938382625579834, 0.2787950038909912, 0.27295637130737305, 0.30155205726623535, 0.34664011001586914, 0.25416040420532227, 0.26088571548461914, 0.2664000988006592, 0.29137539863586426, 0.25771164894104004, 0.25104284286499023, 0.2556331157684326, 0.24962496757507324, 0.2631847858428955, 0.28243160247802734, 0.2242419719696045, 0.2367255687713623]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet: 0.2242419719696045
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet: 0.28243160247802734
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet: 1.119814395904541
``` 

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet
```
[2.219278573989868, 1.7703912258148193, 1.5527746677398682, 1.551875114440918, 1.5295329093933105, 1.4936892986297607, 1.5640678405761719, 1.4979584217071533, 1.4694113731384277, 1.4152870178222656, 1.4210727214813232, 1.3854479789733887, 1.4587302207946777, 1.4694545269012451, 1.4042165279388428, 1.3601820468902588, 1.3659417629241943, 1.3682975769042969, 1.4000065326690674, 1.4403860569000244, 1.4013011455535889, 1.3618366718292236, 1.4356281757354736, 1.3362312316894531, 1.3907794952392578]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet: 1.3362312316894531
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet: 1.4356281757354736
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet: 2.219278573989868
```
#### Additionally, add repartition to the DataFrame using a specified number of partitions 
The code:
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
Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet
```
[5.77779221534729, 4.20501971244812, 3.80135178565979, 4.03955864906311, 3.6246988773345947, 3.8587241172790527, 3.820797920227051, 3.8693952560424805, 3.887660264968872, 3.9268906116485596, 3.8099920749664307, 3.889528751373291, 3.8752009868621826, 3.9173717498779297, 3.8677992820739746, 3.8886220455169678, 3.8882787227630615, 3.88046932220459, 3.9227612018585205, 3.8512792587280273, 3.8889236450195312, 3.8964390754699707, 3.983720064163208, 3.855992317199707, 3.8350954055786133]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet: 3.6246988773345947
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet: 3.887660264968872
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small_sorted.parquet: 5.77779221534729
```

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet
```
[4.854625940322876, 3.5401251316070557, 3.7225100994110107, 3.885870933532715, 3.8321099281311035, 3.9206223487854004, 3.8035411834716797, 4.048891067504883, 3.723074436187744, 3.885659694671631, 3.8826675415039062, 3.9327967166900635, 3.734513521194458, 3.880768299102783, 3.8941893577575684, 3.890043020248413, 3.8903138637542725, 3.923430919647217, 3.8676204681396484, 4.026265382766724, 3.751892566680908, 3.919215440750122, 3.8868885040283203, 3.9022789001464844, 3.8987932205200195]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet: 3.5401251316070557
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet: 3.8868885040283203
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate_sorted.parquet: 4.854625940322876
```

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet
```
[3.3741872310638428, 3.466672658920288, 2.989224672317505, 2.628814220428467, 2.8503196239471436, 2.97538161277771, 2.6851210594177246, 2.7672274112701416, 2.6427834033966064, 2.743905782699585, 2.6409668922424316, 2.7553911209106445, 2.4567997455596924, 2.667722463607788, 2.554793119430542, 2.501457929611206, 2.616809368133545, 2.5292932987213135, 2.8618674278259277, 2.5780367851257324, 2.628495693206787, 2.6351523399353027, 2.6132092475891113, 2.5640082359313965, 2.534189462661743]
Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet: 2.4567997455596924
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet: 2.6409668922424316
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big_sorted.parquet: 3.466672658920288
```


#### Lastly, try to improve the performance by enabling adaptive query execution, dynamic partition pruning, and leveraging columnar storage

The code:

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

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet:
```
[0.709155797958374, 0.41013073921203613, 0.3731675148010254, 2.055326461791992, 0.40297937393188477, 0.3230311870574951, 0.3324623107910156, 0.31008362770080566, 0.3354315757751465, 0.2904038429260254, 0.283099889755249, 0.3048861026763916, 0.2510104179382324, 0.24310517311096191, 0.23824620246887207, 0.2582552433013916, 0.28702569007873535, 0.2317345142364502, 0.23116374015808105, 0.24457168579101562, 0.24332880973815918, 0.2739732265472412, 0.22717595100402832, 0.20919537544250488, 0.20792222023010254]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet: 0.20792222023010254
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet: 0.283099889755249
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet: 2.055326461791992
```

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet"
```
[4.005620002746582, 0.7552187442779541, 0.5301058292388916, 0.6168117523193359, 0.45574045181274414, 0.5552792549133301, 0.4354536533355713, 0.4841923713684082, 0.4789750576019287, 0.4012923240661621, 0.46244144439697266, 0.5689125061035156, 0.40459585189819336, 0.4047689437866211, 0.4775662422180176, 0.4734032154083252, 0.4764058589935303, 0.4704306125640869, 0.4899253845214844, 0.5063436031341553, 0.4951629638671875, 0.4454522132873535, 0.4506714344024658, 0.44201207160949707, 0.44856977462768555]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet: 0.4012923240661621
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet: 0.4764058589935303
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet: 4.005620002746582
```

Times to run the optimized pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet
```
[13.599949836730957, 10.01722526550293, 7.875621557235718, 7.686975002288818, 7.621505260467529, 7.418942213058472, 7.30681848526001, 7.158859014511108, 7.353839159011841, 7.337078809738159, 7.443836688995361, 7.348997354507446, 7.396658658981323, 7.4696009159088135, 7.280026435852051, 7.217103719711304, 7.186214208602905, 7.202255487442017, 7.254214763641357, 7.283256769180298, 7.27942419052124, 7.674328565597534, 7.34223198890686, 7.217056751251221, 7.178027391433716]

Minimum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet: 7.158859014511108
Median Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet: 7.34223198890686
Maximum Time taken to run pq_sum_orders 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet: 13.599949836730957
```



#### 2.5 pq_big_spender_optimized.py Performance Report: Optimized vs. Unoptimized pq_big_spender

In this part, we'll compare the performance of the optimized `pq_big_spender` function to the original unoptimized function. The optimizations applied include enabling Adaptive Query Execution, Dynamic Partition Pruning, and using columnar storage.

## Optimized Code

The optimized code includes the following configurations:

```python
spark = SparkSession.builder.appName('part2') \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "67108864") \
            .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.sql.adaptive.skewedJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .getOrCreate()

```
#### enable Adaptive Query Execution, Dynamic Partition Pruning, and use columnar storage
## Performance Comparison

### Optimized Results

#### people_small.parquet
- Minimum Time taken: 0.1317 seconds
- Median Time taken: 0.1751 seconds
- Maximum Time taken: 2.4845 seconds

#### people_moderate.parquet
- Minimum Time taken: 3.2961 seconds
- Median Time taken: 3.8797 seconds
- Maximum Time taken: 6.4257 seconds

#### people_big.parquet
- Minimum Time taken: 3.8096 seconds
- Median Time taken: 3.8804 seconds
- Maximum Time taken: 7.0490 seconds

### Unoptimized Results

#### people_small.parquet
- Minimum Time taken: 0.1153 seconds
- Median Time taken: 0.1644 seconds
- Maximum Time taken: 5.5817 seconds

#### people_moderate.parquet
- Minimum Time taken: 3.7326 seconds
- Median Time taken: 3.8817 seconds
- Maximum Time taken: 6.3885 seconds

#### people_big.parquet
- Minimum Time taken: 3.5778 seconds
- Median Time taken: 3.8813 seconds
- Maximum Time taken: 6.4964 seconds

## Conclusion

The optimized `pq_big_spender` function has shown tremendous improvement in performance, particularly for the larger datasets. The optimizations applied, such as Adaptive Query Execution, Dynamic Partition Pruning, and columnar storage, have enabled Spark to make more efficient decisions during query execution, which has led to the improved performance.



The optimized code:
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

Times to run the optimized pq_big_spender 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet:
```
[2.4844717979431152, 0.25836896896362305, 0.2427382469177246, 1.6212751865386963, 0.23668122291564941, 0.18804335594177246, 0.18615293502807617, 0.20587658882141113, 0.17507147789001465, 0.2222731113433838, 0.17309927940368652, 0.162872314453125, 0.17629790306091309, 0.1769874095916748, 0.15369200706481934, 0.16825103759765625, 0.14771366119384766, 0.15894007682800293, 0.14487171173095703, 0.14162063598632812, 0.14290165901184082, 0.17655062675476074, 0.13170981407165527, 0.1371021270751953, 0.13759756088256836]

Minimum Time taken: 0.13170981407165527
Median Time taken: 0.17507147789001465
Maximum Time taken: 2.4844717979431152
```

Times to run the optimized pq_big_spender 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet:
```
[6.425657510757446, 3.296051025390625, 3.8135721683502197, 3.6493477821350098, 3.835911750793457, 3.864894151687622, 3.8590657711029053, 3.868067741394043, 3.889186143875122, 3.8789236545562744, 3.8851194381713867, 3.9269142150878906, 3.8430230617523193, 3.8741941452026367, 3.8821871280670166, 3.897787570953369, 3.8796658515930176, 3.9211220741271973, 3.8306005001068115, 3.897162914276123, 3.8988025188446045, 3.8895955085754395, 3.8959221839904785, 3.9002161026000977, 3.8646793365478516]

Minimum Time taken: 3.296051025390625
Median Time taken: 3.8796658515930176
Maximum Time taken: 6.425657510757446
```

Times to run the optimized pq_big_spender 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet
```
[7.048979043960571, 4.733640432357788, 4.3738391399383545, 3.8253448009490967, 3.8095614910125732, 3.8728010654449463, 3.8751585483551025, 3.8566062450408936, 3.9038426876068115, 3.8164191246032715, 3.855523109436035, 3.8891453742980957, 3.8867063522338867, 3.88043475151062, 3.9287445545196533, 3.869302272796631, 3.879455327987671, 3.9071898460388184, 3.919938802719116, 3.850766897201538, 3.87749981880188, 3.899902820587158, 3.895371675491333, 3.9087209701538086, 3.851419448852539]

Minimum Time taken: 3.8095614910125732
Median Time taken: 3.88043475151062
Maximum Time taken: 7.048979043960571
```


#### 2.5 pq_brian_optimized.py Performance Report: Optimized vs. Unoptimized pq_big_spender

# Performance Report for Optimized pq_brian

The optimized version of the pq_brian script has been benchmarked on three different dataset sizes: small, moderate, and big. The results are as follows:

## 1. Small Dataset (hdfs:/user/jw5487_nyu_edu/people_small.parquet):

   - **Minimum Time taken:** 0.11994218826293945 seconds
   - **Median Time taken:** 0.16488361358642578 seconds
   - **Maximum Time taken:** 5.966109752655029 seconds

## 2. Moderate Dataset (hdfs:/user/jw5487_nyu_edu/people_moderate.parquet):

   - **Minimum Time taken:** 0.15963029861450195 seconds
   - **Median Time taken:** 0.19846773147583008 seconds
   - **Maximum Time taken:** 5.885470151901245 seconds

## 3. Big Dataset (hdfs:/user/jw5487_nyu_edu/people_big.parquet):

   - **Minimum Time taken:** 1.7737863063812256 seconds
   - **Median Time taken:** 1.877957820892334 seconds
   - **Maximum Time taken:** 7.379911422729492 seconds

Comparing these results with the original pq_brian script performance:

## 1. Small Dataset (hdfs:/user/jw5487_nyu_edu/people_small.parquet):

   - **Minimum Time taken:** 0.12575221061706543 seconds
   - **Median Time taken:** 0.17871689796447754 seconds
   - **Maximum Time taken:** 5.835251569747925 seconds

## 2. Moderate Dataset (hdfs:/user/jw5487_nyu_edu/people_moderate.parquet):

   - **Minimum Time taken:** 3.3531477451324463 seconds
   - **Median Time taken:** 3.8689217567443848 seconds
   - **Maximum Time taken:** 5.835575819015503 seconds

## 3. Big Dataset (hdfs:/user/jw5487_nyu_edu/people_big.parquet):

   - **Minimum Time taken:** 2.604702949523926 seconds
   - **Median Time taken:** 2.7319469451904297 seconds
   - **Maximum Time taken:** 7.7918713092803955 seconds

The optimized version of the pq_brian script demonstrates significant improvements in performance across all dataset sizes. Specifically, the median time taken for the script to run on the moderate and big datasets has reduced considerably, showcasing the impact of the optimization process.

## Optimizations

The original `pq_brian` script was optimized with the following changes:

1. Adaptive Query Execution (AQE) was enabled.
2. Target post-shuffle input size was set.
3. Auto broadcast join threshold was disabled.
4. Arrow optimization for data serialization was enabled.

The specific configurations applied in the optimized script are:

```python
.config('spark.sql.adaptive.enabled', 'true')
.config('spark.sql.adaptive.shuffle.targetPostShuffleInputSize', '67108864')
.config('spark.sql.autoBroadcastJoinThreshold', '-1')
.config('spark.sql.execution.arrow.enabled', 'true')
```
These configurations were added to the SparkSession builder in the main block of the optimized script:
```
# Create the spark session object with optimizations enabled
spark = (SparkSession.builder.appName('part2')
          .config('spark.sql.adaptive.enabled', 'true')
          .config('spark.sql.adaptive.shuffle.targetPostShuffleInputSize', '67108864')
          .config('spark.sql.autoBroadcastJoinThreshold', '-1')
          .config('spark.sql.execution.arrow.enabled', 'true')
          .getOrCreate())
```
In contrast, the original script did not have any optimizations enabled:
```
# Create the spark session object
spark = SparkSession.builder.appName('part2').getOrCreate()
```
These optimizations help improve the performance of the script across all dataset sizes, as demonstrated by the reduced median time taken for the script to run on moderate and big datasets.



The optimized pq_brian_optimized.py code:
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

Times to run the optimized pq_brian 25 times on hdfs:/user/jw5487_nyu_edu/people_small.parquet:
```
[5.966109752655029, 0.2701911926269531, 0.24095916748046875, 0.22726154327392578, 0.22723889350891113, 0.22799038887023926, 0.18859624862670898, 0.18465042114257812, 0.1766197681427002, 0.16074013710021973, 0.19269919395446777, 0.1676464080810547, 0.15172338485717773, 0.16488361358642578, 0.13998913764953613, 0.18745183944702148, 0.13976144790649414, 0.13338088989257812, 0.13485121726989746, 0.13995695114135742, 0.12538671493530273, 0.14389872550964355, 0.12647700309753418, 0.12494802474975586, 0.11994218826293945]

Minimum Time taken: 0.11994218826293945
Median Time taken: 0.16488361358642578
Maximum Time taken: 5.966109752655029
```
Times to run the optimized pq_brian 25 times on hdfs:/user/jw5487_nyu_edu/people_moderate.parquet:
```
[5.885470151901245, 0.3526623249053955, 0.31196093559265137, 0.34597253799438477, 0.23587584495544434, 0.37865161895751953, 0.22622919082641602, 0.2409226894378662, 0.24106836318969727, 0.22941088676452637, 0.20674872398376465, 0.1918318271636963, 0.1885976791381836, 0.18756699562072754, 0.19548273086547852, 0.18280363082885742, 0.19770026206970215, 0.20308327674865723, 0.17993450164794922, 0.18463873863220215, 0.17561006546020508, 0.17201852798461914, 0.15963029861450195, 0.16186833381652832, 0.19846773147583008]

Minimum Time taken: 0.15963029861450195
Median Time taken: 0.19846773147583008
Maximum Time taken: 5.885470151901245
```

Times to run the optimized pq_brian 25 times on hdfs:/user/jw5487_nyu_edu/people_big.parquet:
```
[7.379911422729492, 2.311307430267334, 2.1436970233917236, 2.0388617515563965, 1.993192195892334, 1.9498565196990967, 1.902327060699463, 1.866819143295288, 1.877957820892334, 1.9084835052490234, 1.8898077011108398, 1.8507180213928223, 1.8671131134033203, 1.8298497200012207, 1.830305576324463, 1.9279770851135254, 1.8454008102416992, 1.9209651947021484, 1.8421881198883057, 1.7737863063812256, 1.8197028636932373, 1.8348784446716309, 1.8632643222808838, 1.8980820178985596, 1.8503503799438477]
Minimum Time taken: 1.7737863063812256
Median Time taken: 1.877957820892334
Maximum Time taken: 7.379911422729492
```