class SqlQueries:
    # Create tables

    table_staging_events_create = ("""
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

    table_staging_songs_create = ("""
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

    table_songplays_create = ("""
        CREATE TABLE songplays (
            songplay_id         TEXT                NOT NULL                   ,
            session_id          SMALLINT            NOT NULL                   ,
            user_agent          TEXT                NOT NULL                   ,
            level               TEXT                NOT NULL                   ,
            location            TEXT                NOT NULL                   ,
            start_time          TIMESTAMP           NOT NULL    sortkey distkey,
            user_id             INTEGER             NOT NULL                   ,
            song_id             TEXT                                           ,
            artist_id           TEXT                                           ,
            PRIMARY KEY (songplay_id)
        );
    """)

    table_users_create = ("""
        CREATE TABLE users (
            user_id             INTEGER             NOT NULL    sortkey,
            first_name          TEXT                NOT NULL           ,
            last_name           TEXT                NOT NULL           ,
            gender              TEXT                NOT NULL           ,
            level               TEXT                NOT NULL           ,
            PRIMARY KEY (user_id)
        ) diststyle auto;
    """)

    table_songs_create = ("""
        CREATE TABLE songs (
            song_id             TEXT                NOT NULL           ,
            title               TEXT                NOT NULL    sortkey,
            year                INTEGER             NOT NULL           ,
            duration            FLOAT               NOT NULL           ,
            artist_id           TEXT                NOT NULL           ,
            PRIMARY KEY (song_id)
        ) diststyle auto;
    """)

    table_artists_create = ("""
        CREATE TABLE artists (
            artist_id           TEXT                NOT NULL           ,
            name                TEXT                NOT NULL    sortkey,
            location            TEXT                                   ,
            latitude            DOUBLE PRECISION                       ,
            longitude           DOUBLE PRECISION                       ,
            PRIMARY KEY (artist_id)
        ) diststyle auto;
    """)

    table_time_create = ("""
        CREATE TABLE time (
            start_time          TIMESTAMP           NOT NULL    sortkey distkey,
            hour                NUMERIC(2)          NOT NULL                   ,
            day                 NUMERIC(2)          NOT NULL                   ,
            week                NUMERIC(2)          NOT NULL                   ,       
            month               NUMERIC(2)          NOT NULL                   ,
            year                INTEGER             NOT NULL                   ,
            weekday             NUMERIC(1)          NOT NULL                   ,
            PRIMARY KEY (start_time)
        );
    """)

    # Staging tables

    table_staging_events_copy = """
    COPY staging_events
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON 'auto ignorecase'
    TIMEFORMAT 'YYYY-MM-DD HH:MI:SS'
    region '{}';
    """

    table_staging_songs_copy = """
    COPY staging_songs
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON 'auto ignorecase'
    region '{}';
    """

    table_songplays_insert = ("""
        INSERT INTO songplays ( 
        SELECT
            md5(events.sessionId || events.start_time) songplay_id,
            events.sessionId AS session_id,
            events.userAgent AS user_agent,  
            events.level,  
            events.location, 
            events.start_time,
            CAST (events.userId AS INTEGER) AS user_id,
            songs.song_id, 
            songs.artist_id
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') AS events
        LEFT JOIN staging_songs AS songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
        )
    """)

    table_users_insert = ("""
        INSERT INTO users (
            SELECT distinct
                CAST (userId AS INTEGER) AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
            FROM staging_events
            WHERE page='NextSong'
        )
    """)

    table_songs_insert = ("""
        INSERT INTO songs (
            SELECT distinct song_id, title, year, duration, artist_id
            FROM staging_songs
        )
    """)

    table_artists_insert = ("""
        INSERT INTO artists (
            SELECT distinct
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
            FROM staging_songs
        )
    """)

    table_time_insert = ("""
        INSERT INTO time (
            SELECT
                start_time,
                extract(hour from start_time),
                extract(day from start_time),
                extract(week from start_time), 
                extract(month from start_time),
                extract(year from start_time),
                extract(dayofweek from start_time) AS weekday
            FROM songplays
        )
    """)
