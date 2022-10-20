class SqlQueries:
    # Drop tables

    drop_table_staging_events = 'DROP TABLE IF EXISTS staging_events;'
    drop_table_staging_songs = 'DROP TABLE IF EXISTS staging_songs;'
    drop_table_songplays = 'DROP TABLE IF EXISTS songplays;'
    drop_table_users = 'DROP TABLE IF EXISTS users;'
    drop_table_songs = 'DROP TABLE IF EXISTS songs;'
    drop_table_artists = 'DROP TABLE IF EXISTS artists;'
    drop_table_time = 'DROP TABLE IF EXISTS time;'

    # Create tables

    create_table_staging_events = ("""
        CREATE TABLE staging_events (
            userId              TEXT,
            firstName           TEXT,
            lastName            TEXT,
            gender              TEXT,
            level               TEXT,
            artist              TEXT,
            song                TEXT,     
            length              FLOAT,
            sessionId           SMALLINT,
            auth                TEXT,
            itemInSession       SMALLINT, 
            location            TEXT,
            registration        DECIMAL(13,0),
            ts                  BIGINT,
            page                TEXT,
            userAgent           TEXT,
            status              SMALLINT,
            method              TEXT
        ) diststyle auto;
    """)

    create_table_staging_songs = ("""
        CREATE TABLE staging_songs (
            song_id             TEXT,
            title               TEXT,
            duration            FLOAT,
            year                INTEGER,
            num_songs           INTEGER,
            artist_id           TEXT,
            artist_name         TEXT,
            artist_location     TEXT,
            artist_latitude     DOUBLE PRECISION,
            artist_longitude    DOUBLE PRECISION
        ) diststyle auto;
    """)

    create_table_songplays = ("""
        CREATE TABLE songplays (
            songplay_id         TEXT                NOT NULL                   ,
            level               TEXT                NOT NULL                   ,
            location            TEXT                NOT NULL                   ,
            user_agent          TEXT                NOT NULL                   ,
            session_id          SMALLINT            NOT NULL                   ,
            user_id             INTEGER             NOT NULL                   ,
            song_id             TEXT                                           ,
            artist_id           TEXT                                           ,
            start_time          TIMESTAMP           NOT NULL    sortkey distkey,
            PRIMARY KEY (songplay_id)
        );
    """)

    create_table_users = ("""
        CREATE TABLE users (
            user_id             INTEGER             NOT NULL    sortkey,
            first_name          TEXT                NOT NULL           ,
            last_name           TEXT                NOT NULL           ,
            gender              TEXT                NOT NULL           ,
            level               TEXT                NOT NULL           ,
            PRIMARY KEY (user_id)
        ) diststyle auto;
    """)

    create_table_songs = ("""
        CREATE TABLE songs (
            song_id             TEXT                NOT NULL           ,
            title               TEXT                NOT NULL    sortkey,
            year                INTEGER             NOT NULL           ,
            duration            FLOAT               NOT NULL           ,
            artist_id           TEXT                NOT NULL           ,
            PRIMARY KEY (song_id)
        ) diststyle auto;
    """)

    create_table_artists = ("""
        CREATE TABLE artists (
            artist_id           TEXT                NOT NULL           ,
            name                TEXT                NOT NULL    sortkey,
            location            TEXT                NOT NULL           ,
            latitude            DOUBLE PRECISION                       ,
            longitude           DOUBLE PRECISION                       ,
            PRIMARY KEY (artist_id)
        ) diststyle auto;
    """)

    create_table_time = ("""
        CREATE TABLE time (
            start_time          TIMESTAMP           NOT NULL    sortkey distkey,
            hour                NUMERIC(2)          NOT NULL                   ,
            day                 NUMERIC(2)          NOT NULL                   ,
            week                NUMERIC(2)          NOT NULL                   ,       
            month               NUMERIC(2)          NOT NULL                   ,
            year                INTEGER             NOT NULL                   ,
            weekday             TEXT                NOT NULL                   ,
            PRIMARY KEY (start_time)
        );
    """)

    # Staging tables

    copy_staging_events = f"""
    COPY staging_events
    FROM '{config.get('S3', 'LOG_DATA')}'
    CREDENTIALS 'aws_iam_role={config.get('IAM', 'REDSHIFT_ROLE_ARN')}'
    FORMAT AS JSON 'auto ignorecase'
    TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
    region '{config.get('AWS', 'REGION')}';
    """

    copy_staging_songs = f"""
    COPY staging_songs
    FROM '{config.get('S3', 'SONG_DATA')}'
    CREDENTIALS 'aws_iam_role={config.get('IAM', 'REDSHIFT_ROLE_ARN')}'
    FORMAT AS JSON 'auto ignorecase'
    region '{config.get('AWS', 'REGION')}';
    """

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
