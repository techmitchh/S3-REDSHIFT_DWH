import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
SONG_DATA       = config.get('S3', 'SONG_DATA')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
REGION          = config.get('S3', 'REGION')


# DROP TABLES

staging_events_table_drop   = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop    = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop         = "DROP TABLE IF EXISTS songplays;"
user_table_drop             = "DROP TABLE IF EXISTS users;"
song_table_drop             = "DROP TABLE IF EXISTS songs;"
artist_table_drop           = "DROP TABLE IF EXISTS artists;"
time_table_drop             = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                                    artist        VARCHAR(256),
                                    auth          VARCHAR(256),
                                    firstName     VARCHAR(256),
                                    gender        VARCHAR(256),
                                    itemInSession INTEGER,
                                    lastName      VARCHAR(256),
                                    length        FLOAT,
                                    level         VARCHAR(256),
                                    location      VARCHAR(256),
                                    method        VARCHAR(256),
                                    page          VARCHAR(256),
                                    registration  VARCHAR(256),
                                    sessionId     INTEGER,
                                    song          VARCHAR(256),
                                    status        VARCHAR(256),
                                    ts            BIGINT,
                                    userAgent     VARCHAR(256),
                                    userId        INTEGER
                              );
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
                                        artist_id         VARCHAR(256),        
                                        artist_latitude   FLOAT,         
                                        artist_location   VARCHAR(256),        
                                        artist_longitude  FLOAT,         
                                        artist_name       VARCHAR(256),        
                                        duration          FLOAT,               
                                        num_songs         INTEGER,             
                                        song_id           VARCHAR(256),         
                                        title             VARCHAR(256),        
                                        year              INTEGER
                                    );
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id   INT IDENTITY(0, 1)  NOT NULL,
                                start_time    TIMESTAMP           NOT NULL,
                                user_id       INTEGER             NOT NULL, 
                                level         VARCHAR(256),
                                song_id       VARCHAR(256),
                                artist_id     VARCHAR(256),
                                session_id    INTEGER     ,
                                location      VARCHAR(256),
                                user_agent    VARCHAR(256),
                                PRIMARY KEY(songplay_id)
                                );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                            user_id    INTEGER      NOT NULL, 
                            first_name VARCHAR(256) NOT NULL, 
                            last_name  VARCHAR(256) NOT NULL, 
                            gender     VARCHAR(256), 
                            level      VARCHAR(256),
                            PRIMARY KEY(user_id)
                            );
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                            song_id   VARCHAR(256) NOT NULL SORTKEY, 
                            title     VARCHAR(256) NOT NULL, 
                            artist_id VARCHAR(256) NOT NULL, 
                            year      INTEGER, 
                            duration  FLOAT,
                            PRIMARY KEY(song_id)
                            );
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                              artist_id         VARCHAR(256) NOT NULL SORTKEY, 
                              artist_name       VARCHAR(256) NOT NULL, 
                              artist_location   VARCHAR(256), 
                              artist_latitude   FLOAT, 
                              artist_longitude  FLOAT,
                              PRIMARY KEY(artist_id)
                              );
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                            start_time  TIMESTAMP NOT NULL,
                            hour        INTEGER   NOT NULL, 
                            day         INTEGER   NOT NULL,
                            week        INTEGER   NOT NULL,
                            month       INTEGER   NOT NULL,
                            year        INTEGER   NOT NULL,
                            weekday     INTEGER   NOT NULL,
                            PRIMARY KEY(start_time)
                            );
""")
                            


# STAGING TABLES

staging_events_copy = ("""
                        COPY staging_events 
                        FROM {} 
                        IAM_ROLE {}
                        JSON {}
                        COMPUPDATE off
                        REGION {}
""").format(LOG_DATA, ARN, LOG_JSONPATH, REGION)

staging_songs_copy = ("""
                        COPY staging_songs
                        FROM {}
                        IAM_ROLE {}
                        FORMAT AS JSON 'auto' REGION {}
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            (SELECT TIMESTAMP 'epoch' + staging_events.ts/1000 * INTERVAL '1 second' AS start_time,
                                    staging_events.userId,
                                    staging_events.level,
                                    staging_songs.song_id,
                                    staging_songs.artist_id,
                                    staging_events.sessionId,
                                    staging_events.location,
                                    staging_events.userAgent
                               FROM staging_events
                               LEFT JOIN staging_songs
                                 ON staging_events.artist = staging_songs.artist_name
                              WHERE staging_events.page = 'NextSong'
                                AND song_id IS NOT NULL
                                AND artist_id IS NOT NULL);
""")

user_table_insert = ("""
                        INSERT INTO users
                        (SELECT userId,
                                firstName,
                                lastName,
                                gender,
                                level
                           FROM staging_events
                          WHERE userid IS NOT NULL);
""")

song_table_insert = ("""
                        INSERT INTO songs
                        (SELECT song_id,
                                title,
                                artist_id,
                                duration
                           FROM staging_songs);
""")

artist_table_insert = (""" INSERT INTO artists 
                        (SELECT DISTINCT artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude                
                          FROM staging_songs
                      GROUP BY 1,2,3,4,5
                  HAVING COUNT (*) < 2);
""")

time_table_insert = (""" INSERT INTO time
                          (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                                  EXTRACT (hour from start_time) AS hour,
                                  EXTRACT (day from start_time) AS day,
                                  EXTRACT (week from start_time) AS week,
                                  EXTRACT (month from start_time) AS month,
                                  EXTRACT (year from start_time) AS year,
                                  EXTRACT (dow from start_time) AS weekday
                             FROM staging_events);                     
""")
            
# QUERY LISTS

create_table_queries    = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries      = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries      = [staging_events_copy, staging_songs_copy]
insert_table_queries    = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
