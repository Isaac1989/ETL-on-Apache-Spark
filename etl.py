import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp, dayofweek
from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIAL']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIAL']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    The function creates and returns a spark Session object 
    """
#     spark = SparkSession \
#         .builder \
#         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#         .getOrCreate()
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
    .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This functions is responsible for the following:
    1. Loads up the song_data from AWS s3
    2. Creates the songs_table and the artist_table
    3. Loads data from song_data into tables created in 2.
    4. Writes both song_table and artist_table to parquet files in AWS S3
    
    inputs:
    ------
        spark: spark session object
        input_data: source of input data from S3
        output_data: destination to write files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
    'song_id',
    'title',
    'artist_id',
    'year',
    'duration'
    ).dropDuplicates()
    
    songs_table.createOrReplaceTempView('songs_table')

    
    # write songs table to parquet files partitioned by year and artist
    songs_table\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .partitionBy('year', 'artist_id')\
    .save(os.path.join(output_data, 'songs/songs.parquet'))

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'),
    col('artist_name').alias('name'),
    col('artist_location').alias("location"),
    col('artist_latitude').alias('latitude'),
    col('artist_longitude').alias('longitude')
    ).dropDuplicates() 
          
    artists_table.createOrReplaceTempView('artists_table')
    
    # write artists table to parquet files
    artists_table\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save(os.path.join(output_data, 'artists/artists.parquet'))


def process_log_data(spark, input_data, output_data):
    """
    This functions is responsible for the following:
    1. Loads up the log_data from AWS s3
    2. Creates the `users_table`, `time_table` and the `songplays_table`.
    3. Loads data from log_data into tables created in 2.
    4. Writes to all tables created in step 2 to parquet files in AWS S3
    
    inputs:
    ------
        spark: spark session object
        input_data: source of input data from S3
        output_data: destination to write files
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark\
    .read\
    .format('json')\
    .load(log_data)
    
    # filter by actions for song plays
    df = df.where(col('page') == 'NextSong')


    # extract columns for users table    
    users_table = df.select(
    col('userId').alias('user_id'),
    col('firstName').alias('first_name'),
    col('lastName').alias('last_name'),
    col('gender'),
    col('level')
    ).dropDuplicates()
          
    users_table.createOrReplaceTempView('users_table')
    
    # write users table to parquet files
    users_table\
    .write\
    .format('parquet')\
    .mode('overwrite')\
    .save(os.path.join(output_data, 'users/users.parquet'))

    # create timestamp column from original timestamp column
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('ts',get_timestamp('ts'))\
           .withColumn('ts', to_timestamp(col('ts').cast(IntegerType())).alias('ts'))
    
    # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
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
    song_df = spark\
          .read\
          .format('parquet')\
    # extract columns from joined song and log datasets to create songplays table 
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


    # write songplays table to parquet files partitioned by year and month
    songplays_table\
          .write\
          .format('parquet')\
          .mode('overwrite')\
          .partitionBy('year','month')\
          .save(os.path.join(output_data, 'songplays/songplays.parquet'))


def main():
    """
    The starts the etl process by creating spark session object and the data sources need
    by the functions process_song_data and process_log_data
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

