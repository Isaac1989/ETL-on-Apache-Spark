import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, dayofweek
from pyspark.sql.types import IntegerType


os.environ['AWS_ACCESS_KEY_ID']='AKIA5NERC5RN3JOPBNL4'
os.environ['AWS_SECRET_ACCESS_KEY']='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'

spark = SparkSession \
.builder \
.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
.config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
.config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
.config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
.getOrCreate()

input_data = "s3a://udacity-dend/"
output_data = ""

# get filepath to song data file
song_data = input_data + 'song_data/*/*/*/*.json'

# read song data file
df = spark.read.json(song_data)

df.columns

# extract columns to create songs table
songs_table = df.select(
'song_id',
'title',
'artist_id',
'year',
'duration'
).dropDuplicates()

songs_table.show(2)

songs_table\
.write\
.format('parquet')\
.mode('overwrite')\
.partitionBy('year', 'artist_id')\
.save(os.path.join(output_data, 'songs/songs.parquet'))

artists_table = df.select(
'artist_id',
col('artist_name').alias('name'),
col('artist_location').alias("location"),
col('artist_latitude').alias('latitude'),
col('artist_longitude').alias('longitude')
).dropDuplicates()

artists_table.show(2)

artists_table\
.write\
.format('parquet')\
.mode('overwrite')\
.save(os.path.join(output_data, 'artists/artists.parquet'))

# get filepath to log data file
log_data = input_data + "log-data/*/*/*.json"

# read log data file
df = spark\
    .read\
    .format('json')\
    .load(log_data)

df.columns

# filter by actions for song plays
df = df.where(col('page') == 'NextSong')
df.show(2)

# extract columns for users table    
users_table = df.select(
col('userId').alias('user_id'),
col('firstName').alias('first_name'),
col('lastName').alias('last_name'),
col('gender'),
col('level')
).dropDuplicates()

users_table.show(2)

# write users table to parquet files
users_table\
.write\
.format('parquet')\
.mode('overwrite')\
.save(os.path.join(output_data, 'users/users.parquet'))

# create datetime column from original timestamp column
get_timestamp = udf(lambda x: str(int(int(x)/1000)))
df = df.withColumn(
    'ts',get_timestamp('ts')
).withColumn('ts', to_timestamp(col('ts').cast(IntegerType())).alias('ts') )

# extract columns to create time table
time_table = df.select(col('ts').alias('start_time'))\
.withColumn('hour', hour(col('start_time')))\
.withColumn('day',dayofmonth(col('start_time')))\
.withColumn('week', weekofyear(col('start_time')))\
.withColumn('month',month(col('start_time')))\
.withColumn('year',year(col('start_time')))\
.withColumn('weekday',dayofweek(col('start_time')))\
.dropDuplicates()

# write time table to parquet files partitioned by year and month
time_table\
.write\
.format('parquet')\
.mode('overwrite')\
.partitionBy('year','month')\
.save(os.path.join(output_data, 'time_table/time_table.parquet'))

# read in song data to use for songplays table
song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
          
          

song_df.createOrReplaceTempView('songs_table')
df.createOrReplaceTempView('log_table')

song_df.printSchema()

df.printSchema()

# write songplays table to parquet files partitioned by year and month
joinExpr1 = df['artist'] == song_df['artist_name']
joinExpr2 = df['length'] == song_df['duration']
joinExpr3 = df['song'] == song_df['title']
songplays_table= df.join(
    song_df, joinExpr1 & joinExpr2 & joinExpr3, 'left_outer')\
    .where(col('userId').isNotNull())\
    .where(col('song_id').isNotNull())\
    .where(col('page') == 'NextSong')\
    .select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        year(col('ts')).alias('year'),
        month(col('ts')).alias('month'))\
    .withColumn('songplay_id', monotonically_increasing_id())


songplays_table\
          .write\
          .format('parquet')\
          .mode('overwrite')\
          .partitionBy('year','month')\
          .save(os.path.join(output_data, 'songplays/songplays.parquet'))
