import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Receive the cursor and filepath to read the songs files
    and write results to songs and artists tables.
    
    Each file has only one record, so we index to [0] to 
    avoid processing individualy every field to the INSERT statement.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist() 
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Receive the cursor and filepath to read the logs files
    and write results to time, users and songplay tables.
    
    UserId is important for our tables, so when we don't have
    this field populated (with empty string) is better to remove
    these ocurrencis rather than keep these files into the analytical 
    tables.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df[df.userId.astype(str) != '']

    # filter by NextSong action
    t = df[(df.page == 'NextSong')].copy()

    # convert timestamp column to datetime
    t['ts'] = pd.to_datetime(t['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.ts, t.ts.dt.hour, t.ts.dt.day, t.ts.dt.week, t.ts.dt.month, t.ts.dt.year, t.ts.dt.weekday) 
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # check if there is string content to user lower()
        if row.song is None or row.artist is None:
            songid, artistid = None, None
            
        else:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song.lower(), row.artist.lower(), row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Receive the cursor, connection, filepath and function to process the
    files depending on the kind of file.
    
    We commit the changes after the hole file is transformed.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()