import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

# staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
# staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                                    artist VARCHAR,
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender VARCHAR,
                                    itemInSession INT,
                                    lastName VARCAHR,
                                    length VARCHAR,
                                    level VARCHAR,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId INT,
                                    song VARCHAR,
                                    status VARCHAR,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userId INT
                              )
""")

# staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs
# """)

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id INT PRIMARY KEY,
                                start_time TIMESTAMP NOT NULL, 
                                userId INT NOT NULL, 
                                level VARCHAR, 
                                song_id VARCHAR, 
                                artist_id VARCHAR, 
                                session_id INT, 
                                location VARCHAR, 
                                userAgent VARCHAR
                                );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                            userId INT PRIMARY KEY, 
                            firstName VARCHAR NOT NULL, 
                            lastName VARCHAR NOT NULL, 
                            gender VARCHAR, 
                            level VARCHAR
                            );
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                            song_id VARCHAR PRIMARY KEY, 
                            title VARCHAR NOT NULL, 
                            artist_id VARCHAR NOT NULL, 
                            year INT, 
                            duration FLOAT
                            );
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                              artist_id VARCHAR PRIMARY KEY, 
                              artist_name VARCHAR NOT NULL, 
                              artist_location VARCHAR, 
                              artist_latitude FLOAT, 
                              artist_longitude FLOAT
                              );
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                            start_time TIMESTAMP PRIMARY KEY, 
                            hour INT NOT NULL, 
                            day INT NOT NULL, 
                            weekofyear INT NOT NULL, 
                            month INT NOT NULL, 
                            year INT NOT NULL, 
                            weekday INT NOT NULL
                            );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
