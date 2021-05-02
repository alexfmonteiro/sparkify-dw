import configparser

# CONFIG FILE
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES
# 1. STAGING TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
"""

# 2. FACT TABLE
songplay_table_create = """
CREATE TABLE IF NOT EXISTS public.songplays (
	playid varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	songid varchar(256),
	artistid varchar(256),
	sessionid int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
);
"""

# 3. DIMENSION TABLES
user_table_create = """
CREATE TABLE IF NOT EXISTS public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
);  
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
);    
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
);
"""

# LOAD DATA FROM S3 BUCKETS INTO STAGING TABLES VIA COPY COMMANDS
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
AWS_KEY = config.get("AWS", "KEY")
AWS_SECRET = config.get("AWS", "SECRET")

staging_events_copy = ("""
    COPY public.staging_events FROM {}
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}' 
    TIMEFORMAT AS 'epochmillisecs'
    REGION 'us-west-2' 
    JSON {}
""").format(LOG_DATA, AWS_KEY, AWS_SECRET, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY public.staging_songs FROM {}
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    TIMEFORMAT AS 'epochmillisecs'
    REGION 'us-west-2'
    JSON 'auto'
""").format(SONG_DATA, AWS_KEY, AWS_SECRET)

# LOAD DATA FROM STAGING TABLES INTO FINAL TABLES
songplay_table_insert = """
INSERT INTO public.songplays (
    SELECT md5(events.sessionid || events.start_time) songplay_id,
           events.start_time,
           events.userid,
           events.level,
           songs.song_id,
           songs.artist_id,
           events.sessionid,
           events.location,
           events.useragent
    FROM (SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time, *
          FROM public.staging_events se
          WHERE page='NextSong') events
             LEFT JOIN public.staging_songs songs
                       ON events.song = songs.title
                           AND events.artist = songs.artist_name
                           AND events.length = songs.duration
);
"""

user_table_insert = """
INSERT INTO public.users (
    SELECT distinct userid, firstname, lastname, gender, level
    FROM public.staging_events
    WHERE page = 'NextSong'
);
"""

song_table_insert = """
INSERT INTO public.songs (
    SELECT distinct song_id, title, artist_id, year, duration
    FROM public.staging_songs
);
"""

artist_table_insert = """
INSERT INTO public.artists (
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM public.staging_songs
);
"""

time_table_insert = """
INSERT INTO public.time (
    SELECT start_time,
           extract(hour from start_time),
           extract(day from start_time),
           extract(week from start_time),
           extract(month from start_time),
           extract(year from start_time),
           extract(dayofweek from start_time)
    FROM public.songplays
);
"""

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
