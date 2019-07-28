import configparser

# CONFIG
from config import get_config

#CREATE THE dist SCHEMA for DISTRIBUTION STRATEGY
create_dist_schema = "CREATE SCHEMA IF NOT EXISTS dist;"
set_search_path = "SET search_path TO dist;"
    
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS dist.staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS dist.staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS dist.songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS dist.users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS dist.songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS dist.artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS dist.time CASCADE"
dist_schema_drop = "DROP SCHEMA IF EXISTS dist CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS dist.staging_events
    (
    artist          VARCHAR(MAX),
    auth            VARCHAR(MAX),
    firstName       VARCHAR(MAX),
    gender          VARCHAR(MAX),
    itemInSession   INTEGER,
    lastName        VARCHAR(MAX),
    lenght          FLOAT,
    level           VARCHAR(MAX),
    location        VARCHAR(MAX),
    method          VARCHAR(MAX),
    page            VARCHAR(MAX),
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR(MAX),
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR(MAX),
    userId          INTEGER
    )
    diststyle all;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS dist.staging_songs 
    (
    song_id          VARCHAR(MAX) PRIMARY KEY,
    title            VARCHAR(MAX),
    duration         FLOAT,
    year             INTEGER,
    num_songs        INTEGER,
    artist_id        VARCHAR(MAX),
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR(MAX),
    artist_name      VARCHAR(MAX) 
    )
    diststyle all;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS dist.songplays
    (
    songplay_id  INTEGER      IDENTITY(0,1) PRIMARY KEY,
    start_time   VARCHAR(MAX) NOT NULL REFERENCES dist.time(start_time) SORTKEY,
    user_id      INTEGER      NOT NULL REFERENCES dist.users(user_id),
    level        VARCHAR(MAX) NOT NULL,
    song_id      VARCHAR(MAX) NOT NULL REFERENCES dist.songs(song_id),
    artist_id    VARCHAR(MAX) NOT NULL REFERENCES dist.artists(artist_id),
    session_id   INTEGER      NOT NULL,
    location     VARCHAR(MAX) NOT NULL,
    user_agent   VARCHAR(MAX) NOT NULL
    )
    diststyle all;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dist.users
    (
    user_id     INTEGER     NOT NULL   PRIMARY KEY SORTKEY,
    first_name  VARCHAR(MAX) NOT NULL,
    last_name   VARCHAR(MAX) NOT NULL,
    gender      VARCHAR(MAX) NOT NULL,
    level       VARCHAR(MAX) NOT NULL
    )
    DISTSTYLE all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dist.songs
    (
    song_id     VARCHAR(MAX) NOT NULL  PRIMARY KEY SORTKEY,
    title       VARCHAR(MAX) NOT NULL,
    artist_id   VARCHAR(MAX) NOT NULL REFERENCES artists(artist_id),
    year        INTEGER      NOT NULL,
    duration    FLOAT     NOT NULL
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dist.artists
    (
    artist_id    VARCHAR(MAX) PRIMARY KEY SORTKEY,
    name         VARCHAR(MAX),
    location     VARCHAR(MAX),
    latitude     FLOAT,
    longitude    FLOAT
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dist.time
    (
    start_time  VARCHAR(MAX)  PRIMARY KEY SORTKEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy dist.staging_events 
    from {}
    iam_role {}
    json {};
""").format(get_config('S3','LOG_DATA'),get_config('IAM_ROLE','ARN'),get_config('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy dist.staging_songs 
    from {}
    iam_role {}
    json 'auto'
""").format(get_config('S3', 'SONG_DATA'), get_config('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO dist.songplays (start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
    SELECT DISTINCT dist.staging_events.ts as start_time,
                    dist.staging_events.userId as user_id,
                    dist.staging_events.level as level,
                    dist.staging_songs.song_id as song_id,
                    dist.staging_songs.artist_id as artist_id,
                    dist.staging_events.sessionId as session_id,
                    dist.staging_events.location as location,
                    dist.staging_events.userAgent as user_agent
    FROM dist.staging_events, dist.staging_songs
    WHERE dist.staging_events.page = 'NextSong'
    AND dist.staging_events.song = dist.staging_songs.title
""")

user_table_insert = ("""
    INSERT INTO dist.users(user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT DISTINCT userid,
                    firstName,
                    lastName,
                    gender, 
                    level
    FROM dist.staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO dist.songs (song_id,
                      title,
                      artist_id,
                      year,
                      duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM dist.staging_songs
""")

artist_table_insert = ("""
    INSERT INTO dist.artists (artist_id,
                 name,
                 location,
                 latitude,
                 longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM dist.staging_songs
""")

time_table_insert = ("""
    INSERT INTO dist.time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
    SELECT start_time,
           date_part(hour, date_time) AS hour,
           date_part(day, date_time) AS day,
           date_part(week, date_time) AS week,
           date_part(month, date_time) AS month,
           date_part(year, date_time) AS year,
           date_part(weekday, date_time) AS weekday
  FROM (SELECT ts AS start_time,
               '1970-01-01'::date + ts/1000 * interval '1 second' AS date_time
          FROM dist.staging_events
          GROUP BY ts) as temp
  ORDER BY start_time;
""")

# QUERY LISTS

create_table_queries = [create_dist_schema,set_search_path,staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#copy_table_queries = [staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
