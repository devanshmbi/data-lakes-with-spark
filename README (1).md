Data Lakes with Spark

Sparkify Analytics

As the trend of custom recommended playlist increases. We are startup group who wants to fullfil their customers needs to provide enhanced and best recommended music playlist according to their mood and category for our customers via music streaming app.

The process started when we started to analyze our users activity, to know them better what kind of songs they listen and construct a real time ETL Workflow, which will parse, analyze, aggregate and Insert data into Postgres Database which will help us in categorizing and enhancing our recommendation models for our customer.

Project Description:

For this project, we have hosted our enviorment on cloud using AWS EMR. We have stored our log_data and event_data files on S3 at "s3a://udacity-dend/"
The ETL pipeline is build as follows:
1) Fetch data from S3 and store it as dataframe.
2) Create artists, users, songs and time dataframe and write it as parquet files


Source Data:

The log_data directory contains events files in JSON format.

{"artist":"Sydney Youngblood","auth":"Logged In","firstName":"Jacob","gender":"M","itemInSession":53,"lastName":"Klein","length":238.07955,"level":"paid","location":"Tampa-St. Petersburg-Clearwater, FL","method":"PUT","page":"NextSong","registration":1540558108796.0,"sessionId":954,"song":"Ain't No Sunshine","status":200,"ts":1543449657796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.78.2 (KHTML, like Gecko) Version\/7.0.6 Safari\/537.78.2\"","userId":"73"}


The songs_data directory contains log files in JSON format.

{"num_songs": 1, "artist_id": "AREDBBQ1187B98AFF5", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Eddie Calvert", "song_id": "SOBBXLX12A58A79DDA", "title": "Erica (2005 Digital Remaster)", "duration": 138.63138, "year": 0}


We have distributed our tables in form of STAR schema, for better analysis. The list of tables and their fields are as follows:

Fact Table, also known as songplays table will contain records from log data as well as songs data associated with the songs such as:

    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL sortkey distkey,
    user_id     INTEGER,
    level       VARCHAR,
    song_id     VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    session_id  INTEGER,
    location    VARCHAR,
    user_agent  VARCHAR

Dimension Tables design have been categorized as follows:

1) Users Table - To get all the users from music streaming app, the respective schema for the table is attached below
    
    user_id     INTEGER NOT NULL PRIMARY KEY sortkey,
    first_name  VARCHAR NOT NULL,
    last_name   VARCHAR NOT NULL,
    gender      VARCHAR NOT NULL,
    level       VARCHAR NOT NULL
    
2) Songs Table - Collections of songs from music database, the respective schema for the table is attached below
    
    song_id    VARCHAR PRIMARY KEY sortkey,
    title      VARCHAR NOT NULL,
    artist_id  VARCHAR NOT NULL,
    year       INTEGER NOT NULL,
    duration   FLOAT

3) Artist Table - Collections of artists from music database, the respective schema for the table is attached below
    
    artist_id VARCHAR PRIMARY KEY sortkey,
    name VARCHAR,
    location VARCHAR,
    lattitude FLOAT,
    longitude FLOAT

4) Time Table - Timestamps of song records played by the user which partitioned by year, month,.etc, the respective schema for the table is attached below
    
    start_time TIMESTAMP PRIMARY KEY distkey sortkey,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
    

The Project Structure is as follows:

data: For this project the data is uploaded on AWS S3. The directory for the where song_data and log_data JSON file resides.
etl.py - This file contains python code to insert data from the staging tables into different tables and finally populating the songplays table.
dl.cfg - It contains access key credentails to login to enviorment


To run this project in local mode, first open dl.cfg and insert your AWS credentials.
Run the command python etl.py to write files as parquet.

Author: Devansh Modi