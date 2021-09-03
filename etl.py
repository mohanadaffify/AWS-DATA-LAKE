import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description : This function creates the spark session. 
    
    Parameters  : None 
    
    Returns     : None
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
    and then again load back processed data to S3
        
    Parameters:
      spark       : this is the Spark Session .
      input_data  : the location of song_data from where the file is load to process
      output_data : the location where after processing the results will be stored
      
    Returns: 
     None
            
    """
    # get filepath to song data file
    song_data =os.path.join(input_data,'song_data','*','*','*')
    
    # read song data file
    df = spark.read.json(song_data)
    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.parquet(os.path.join(output_data, 'songs'),
                                            partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artist_table=df.select(['artist_id',
                            'artist_latitude',
                            'artist_longitude',
                            'artist_location',
                             'artist_name']) 
    
    
    
    artist_table= artist_table.selectExpr("artist_id as artist_id", 
                                          "artist_latitude as latitude",
                                          "artist_longitude as longitude",
                                          "artist_location as location",
                                          "artist_name as name")
    
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table=artists_table.write.parquet(os.path.join(output_data,'artists'))


def process_log_data(spark, input_data, output_data):
    
    """
    Description: This function loads log data from S3 and processes it by extracting the songs and artist tables
    and then again load back processed data to S3
        
    Parameters:
      spark       : this is the Spark Session .
      input_data  : the location of song_data from where the file is load to process
      output_data : the location where after processing the results will be stored
      
    Returns: 
     None
     
    """ 
    
    # get filepath to log data file
    log_data =os.path.join(input_data,'slog_data','*','*')

    # read log data file
    df =spark.read.json(log_data)  
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table
    users_table =df.select(['firstName','gender','lastName','level','user_id'])
    users_table=users_table.selectExpr( "firstName as first_name",
                                       "gender as gender",
                                       "lastName as last_name",
                                       "level as level",
                                       "user_id as user_id" )   
    
    users_table = users_table.drop_duplicates(subset=['user_id'])
    
    # write artist table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users'))
                            
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df =df.withColumn("timestamp",get_timestamp(ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts:to_date(ts))
    df =df.withColumn("datetime",get_datetime(ts)) 
    
    # extract columns to create time table
    time_table =df.select(col("timestamp"))
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))                        
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.parquet(os.path.join(output_data, 'time'),
                                          partitionBy=['year','month'])

    # read in song data to use for songplays table
    song_df =spark.read.json(input_data,'song_data','*','*','*') 

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')
    songplays_table = spark.sql("""
        SELECT
            DISTNICT l.songplay_id,
            l.start_time,
            l.user_id,
            l.level,
            l.song_id,
            l.artist_id,
            l.sessionId as session_id,
            l.location,
            l.userAgent as user_agent,
            year(l.start_time) as year,
            month(l.start_time) as month
        FROM logs l
        LEFT JOIN songs s ON
            l.song = s.title AND
            l.artist = s.artist_name AND
            ABS(l.length - s.duration) < 4
    """)                        

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), 
                                  partitionBy=['year', 'month'])

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
