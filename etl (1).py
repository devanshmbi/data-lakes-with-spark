import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('dl.cfg')

""" Reading AWS scredentials """
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')


""" Creating sparksession with necessary jars """
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

""" This function will reading songs json files and will write output to parquet file """
def process_song_data(spark, input_data, output_data):
    
    """ Creating input path for songs data """
    song_data_input_path = input_data + 'song_data/*/*/*/*.json'

    """ Reading songs data into a dataframe """
    df_staging_songs = spark.read.json(path=song_data_input_path)

    """ Extract columns from staging_songs dataframe for songs table """
    songs_table = df_staging_songs.selectExpr("artist_id", "title", "year", "duration"). \
        withColumn("song_id", monotonically_increasing_id()).drop_duplicates()

    """ Writing songs dataframe as parquet partitioned by year and artist """
    print("Printing sample data from songs_table dataframe")
    songs_table.show(10)
    songs_table.coalesce(1).write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+"songs_table/")

    """ Extract columns from staging_songs dataframe for artists table """
    artists_table = df_staging_songs.selectExpr("artist_id", "artist_name as name", \
                                                "artist_location as location","artist_latitude as latitude",\
                                                "artist_longitude as longitude").drop_duplicates()

    """ Writing artists dataframe as parquet to S3 """
    print("Printing sample data from artists_table dataframe")
    artists_table.show(10)
    artists_table.coalesce(1).write.mode('overwrite').parquet(output_data+"artists_table/")

""" This function will reading log json files and will write output to parquet file """
def process_log_data(spark, input_data, output_data):
    
    """ Creating input path for logs data """
    log_data_input_path = input_data + 'log_data/*.json'

    """ Reading log data into a dataframe """
    df_log_data = spark.read.json(path=log_data_input_path)

    """ Filtering dataframe where pade is equal to NextSong """
    df_log_data_filtered = df_log_data.filter(df_log_data.page == 'NextSong')

    """ Extract columns from staging events dataframe for users table """
    users_table = df_log_data_filtered.selectExpr("userId as user_id", \
                                                  "firstName as first_name",\
                                                  "lastName as last_name", \
                                                  "gender", "level").drop_duplicates()

    """ Writing users dataframe as parquet """
    print("Printing sample data from users_table dataframe")
    users_table.show(10)
    users_table.coalesce(1).write.mode('overwrite').parquet(output_data+"users_table/")

    """ Creating spark UDF to convert bigint to timestamp """
    def convert_date(ts):
        ts = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        return ts

    sp_udf = udf(lambda x: convert_date(x), StringType())

    """ Extract columns from staging events dataframe for time table """
    df_time = df_log_data_filtered.select(sp_udf("ts").alias("start_time").cast("timestamp")).drop_duplicates()
    time_table = df_time.select("start_time", \
                                (hour('start_time').alias("hour")), \
                                (dayofmonth('start_time').alias("day")), \
                                (weekofyear(date_format("start_Time", 'yyyy-MM-dd')).alias("week")), \
                                (month('start_time').alias("month")), \
                                (year('start_time').alias("year")), \
                                (dayofweek(date_format("start_Time", 'yyyy-MM-dd')).alias("weekday")))

    """ Writing time_table dataframe as parquet partitioned by year and artist """
    print("Printing sample data from time_table dataframe")
    time_table.show(10)
    time_table.coalesce(1).write.mode('overwrite').parquet(output_data+"time_table/")

    """ Reading songs data into dataframe for creating songsplay table """
    song_data_input_path = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(path=song_data_input_path)

    """ Creating register temp table for both tables """
    df_log_data_filtered.registerTempTable("log_data")
    song_df.registerTempTable("songs_data")
    
    """ Extract columns from songsplay table by joining staging events table and staging songs """
    songplays_table=spark.sql(""" SELECT monotonically_increasing_id() as songplay_id,
                         to_timestamp(log_data.ts/1000) as start_time,
                         year(to_timestamp(log_data.ts/1000)) as year,
                         month(to_timestamp(log_data.ts/1000)) as month,
                         log_data.userId as user_id,
                         log_data.level as level,
                         songs_data.song_id as song_id,
                         songs_data.artist_id as artist_id,
                         log_data.sessionId as session_id,
                         log_data.location as location,
                         log_data.userAgent as user_agent
                         FROM log_data
                         JOIN songs_data songs_data on log_data.artist = songs_data.artist_name
                         and log_data.song = songs_data.title """)
    
    print("Printing sample data from songsplays_table dataframe")
    songplays_table.show(10)
    
    """ Writing songplays_Table dataframe as parquet partitioned by year and artist """
    songplays_table.coalesce(1).write.mode('overwrite').partitionBy('year','month').parquet(output_data+"songplays_table/")

""" Writing main function to process and write data to S3 """
def main():
    spark = create_spark_session()
    input_data = "/home/workspace/raw_data/"
    output_data = "/home/workspace/landing/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()