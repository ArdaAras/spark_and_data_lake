import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates a spark session and returns it
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Reads song data using the given input S3 bucket and transforms them into songs and artists tables.
    Finally, writes the tables to given output S3 bucket in parquet format
    Parameters:
            spark       (SparkSession): Spark session object
            input_data           (str): Path of the S3 bucket where data resides
            output_data          (str): Path of the S3 bucket where songs and artists tables will be written to
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude'])
    artists_table_new_names = artists_table.toDF('artist_id','name','location','latitude','longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table_new_names.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    '''
    Reads song and log data using the given input S3 bucket and transforms them into users, time and songplays tables.
    Finally, writes the tables to given output S3 bucket in parquet format
    Parameters:
            spark       (SparkSession): Spark session object
            input_data           (str): Path of the S3 bucket where data resides
            output_data          (str): Path of the S3 bucket where users, time and songplays tables will be written to
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    users_table_new_names = users_table.toDF('user_id','first_name','last_name','gender','level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df = df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
   
    # extract columns to create time table
    time_table = df.select('start_time').dropDuplicates()
    time_table = time_table.withColumn('hour', F.hour('start_time'))
    time_table = time_table.withColumn('day', F.dayofmonth('start_time'))
    time_table = time_table.withColumn('week', F.weekofyear('start_time'))
    time_table = time_table.withColumn('month', F.month('start_time'))
    time_table = time_table.withColumn('year', F.year('start_time'))
    time_table = time_table.withColumn('weekday', F.dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = song_df.join(df, song_df.artist_name == df.artist, 'inner')
    joined_df = joined_df.withColumn('songplay_id', F.monotonically_increasing_id())
    
    songplays_table = joined_df.select(['songplay_id','start_time','userId','level','song_id','artist_id','sessionId','location','userAgent','year']).dropDuplicates()
    songplays_table = songplays_table.withColumn('month', F.month('start_time'))
    songplays_table = songplays_table.withColumnRenamed('userId','user_id')
    songplays_table = songplays_table.withColumnRenamed('sessionId','session_id')
    songplays_table = songplays_table.withColumnRenamed('userAgent','user_agent')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    '''
    Creates spark session and reads song and log data from given input S3 bucket.
    Transforms the read data into fact and dimension analytical tables.
    Finally, writes the transformed tables to given output S3 bucket in parquet format
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" #insert your S3 here
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
