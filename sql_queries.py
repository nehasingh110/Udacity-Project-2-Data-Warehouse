import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stg_events (artist varchar, auth varchar, firstName \
                                varchar, gender char, itemInSession numeric, lastName varchar, length decimal,\
                                level varchar, location varchar, method varchar, page varchar, registration \
                                decimal, sessionId numeric, song varchar, status numeric, ts BIGINT, userAgent\
                                varchar, userId numeric) """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stg_songs (num_songs numeric, artist_id varchar,\
                                artist_latitude varchar, artist_longitude varchar, artist_location varchar, \
                                artist_name varchar, song_id varchar, title varchar, duration decimal,\
                                year numeric); """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id int IDENTITY(0,1) PRIMARY KEY, \
                            start_time BIGINT,user_id numeric, level varchar, song_id varchar, \
                            artist_id varchar, session_id numeric, location varchar,user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id numeric PRIMARY KEY, first_name varchar,\
                        last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id varchar PRIMARY KEY, title varchar, \
                        artist_id varchar, year numeric, duration numeric);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists  (artist_id varchar PRIMARY KEY, name varchar, \
                            location varchar, lattitude numeric, longitude numeric);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time  (start_time BIGINT PRIMARY KEY, hour numeric, day numeric,\
                        week numeric, month numeric, year numeric, weekday numeric);""")



# STAGING TABLES

staging_events_copy = ("""COPY stg_events FROM {} iam_role {} FORMAT AS JSON {}\
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""COPY stg_songs FROM {} iam_role {} FORMAT AS JSON 'auto'\
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, \
                        session_id, location, user_agent) \
                        select e.ts as start_time, e.userId as user_id, e.level, s.song_id, s.artist_id,\
                        e.sessionId as session_id, e.location, e.userAgent as user_agent from stg_events e \
                        left join stg_songs s on e.song = s.title and e.artist = s.artist_name;""")

user_table_insert = ("""INSERT INTO users ( user_id, first_name, last_name, gender, level) \
                        select distinct userId as user_id, firstName as first_name, \
                        lastName as last_name, gender, max(level) aslevel from stg_events where \
                        userId is not null group by 1,2,3,4;""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration) \
                        select distinct song_id, title, artist_id, year, duration from \
                        stg_songs where song_id is not null; """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) \
                        select distinct artist_id, artist_name as name, artist_location as location,\
                        artist_latitude as lattitude, artist_longitude as longitude from stg_songs \
                        where artist_id is not null;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)\
                        select distinct ts as start_time, \
                        EXTRACT(h from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as hour,\
                        EXTRACT(d from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as day, \
                        EXTRACT(w from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as week, \
                        EXTRACT(mon from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as month, \
                        EXTRACT(y from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as year, \
                        EXTRACT(dow from TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as weekday \
                        from stg_events where ts is not null;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]