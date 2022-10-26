-- Songplays
select count (*) from songplays; --6820
SELECT sum(regexp_count(songplay_id, '[a-f0-9]+', 1, 'p')) FROM songplays; --6820 -> 9b85446ad246ae38b131189aad2af37a
SELECT sum(regexp_count(session_id, '[0-9]+')) FROM songplays; --6820 -> 345
SELECT sum(regexp_count(level, '(free)|(paid)')) FROM songplays; --6820 -> free or paid
SELECT sum(regexp_count(location, '[[:word:][:digit:][:space:][:punct:]\.\-]+', 1, 'p')) FROM songplays; --6820 -> Phoenix-Mesa-Scottsdale, AZ
SELECT sum(regexp_count(extract(epoch from start_time), '[[:digit:]]{10}', 1, 'p')) FROM songplays; --6820 -> 2018-11-01 21:01:46.000000 - 1541106496
SELECT sum(regexp_count(user_id, '[[:digit:]]+', 1, 'p')) FROM songplays; --6820 -> 345
SELECT sum(regexp_count(song_id, '[[:alpha:][:digit:]]+', 1, 'p')) FROM songplays; --319 -> SOEIQUY12AF72A086A
SELECT count(*) FROM songplays WHERE song_id IS NULL; --6501
SELECT sum(regexp_count(artist_id, '[[:alpha:][:digit:]]+', 1, 'p')) FROM songplays; --319 -> ARHUC691187B9AD27F
SELECT count(*) FROM songplays WHERE artist_id IS NULL; --6501

-- Users
select count (*) from users; --104
SELECT sum(regexp_count(user_id, '[[:digit:]]+', 1, 'p')) FROM users; --104 -> 66
SELECT sum(regexp_count(first_name, '[[:word:]]+', 1, 'p')) FROM users; --104 -> Elijah
SELECT sum(regexp_count(last_name, '[[:word:]]+', 1, 'p')) FROM users; --104 -> Gonzalez
SELECT sum(regexp_count(gender, '(F)|(M)', 1, 'p')) FROM users; --104 -> F or M
SELECT sum(regexp_count(level, '(free)|(paid)')) from users; --104 -> free or paid

-- Songs
select count (*) from songs; --14896
SELECT sum(regexp_count(song_id, '[[:alpha:][:digit:]]+')) from songs; --14896 -> SOEIQUY12AF72A086A
SELECT sum(regexp_count(title, '.+')) from songs; --14896 -> 5 Minutes Alone (LP Version)
SELECT sum(regexp_count(year, '([[:digit:]]{4})|0')) from songs; --14896 -> 2001 or 0
SELECT sum(regexp_count(duration, '[[:digit:]]+\.[[:digit:]]+')) from songs; --14896 -> 267.41506
SELECT sum(regexp_count(artist_id, '[[:alpha:][:digit:]]+', 1, 'p')) FROM songs; --14896 -> ARHUC691187B9AD27F

-- Artists
select count (*) from artists; --10025
SELECT sum(regexp_count(artist_id, '[[:alpha:][:digit:]]+', 1, 'p')) FROM artists; --10025 -> ARHUC691187B9AD27F
SELECT sum(regexp_count(name, '.+', 1, 'p')) FROM artists; --10025 -> 101 Strings Orchestra
SELECT sum(regexp_count(latitude, '[-[:digit:]]+\.[[:digit:]]+', 1, 'p')) FROM artists; --3438 -> 38.8991
SELECT count(*) FROM artists WHERE latitude IS NULL; --6587
SELECT sum(regexp_count(longitude, '[-[:digit:]]+\.[[:digit:]]+', 1, 'p')) FROM artists; --3438 -> -77.029
SELECT count(*) FROM artists WHERE longitude IS NULL; --6587

-- Time
select count (*) from time; --6820
SELECT sum(regexp_count(extract(epoch from start_time), '[[:digit:]]{10}', 1, 'p')) FROM time; --6820 -> 2018-11-01 21:01:46.000000 - 1541106496
SELECT sum(regexp_count(hour, '[[:digit:]]{1,2}', 1, 'p')) FROM time;
SELECT sum(regexp_count(day, '[[:digit:]]{1,2}', 1, 'p')) FROM time;
SELECT sum(regexp_count(week, '[[:digit:]]{1,2}', 1, 'p')) FROM time;
SELECT sum(regexp_count(month, '[[:digit:]]{1,2}', 1, 'p')) FROM time;
SELECT sum(regexp_count(year, '[[:digit:]]{4}', 1, 'p')) FROM time;
SELECT sum(regexp_count(weekday, '[[:digit:]]{1}', 1, 'p')) FROM time;