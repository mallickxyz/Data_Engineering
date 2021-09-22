import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *




def process_song_file(cur, filepath):
    """
    - This function take psycopg2 database cursor object and filepath of the single song dataset as input.
    
    - It will convert the json file into pandas dataframe and insert the data to songs and artists table.
    """
    
    df = pd.read_json(filepath, lines=True)

    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)
    
    
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    
    


def process_log_file(cur, filepath):
    """
    - This function take psycopg2 database cursor object and filepath of the single log dataset as input.
    
    - From the log file according to the transformation it will insert the data into time,users dimensiona tables and song_play fact table.
    """
    
    df = pd.read_json(filepath,lines=True)

    
    df = df[df.page == 'NextSong']

    
    df['ts'] = pd.to_datetime(df['ts'],unit='ms').to_frame()
    t = df['ts'].to_frame()
    
    
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = t.rename(columns={'ts':'start_time'})[['start_time']]

    time_df[column_labels[1]] =  time_df['start_time'].apply(lambda x: x.hour)
    time_df[column_labels[2]] =  time_df['start_time'].apply(lambda x: x.day)
    time_df[column_labels[3]] =  time_df['start_time'].apply(lambda x: x.week)
    time_df[column_labels[4]] =  time_df['start_time'].apply(lambda x: x.month)
    time_df[column_labels[5]] =  time_df['start_time'].apply(lambda x: x.year)
    time_df[column_labels[6]] =  time_df['start_time'].apply(lambda x: x.weekday())

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    
    user_df = df[['userId','firstName','lastName','gender','level']]

    
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    
    for index, row in df.iterrows():
        
        
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        
        songplay_data = (row.ts,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - As parameters these function will take psycopg2 connection,cursor object, filepath of the speficied file directory and the appropriate processing function reference. Ex- 'data/song_data',''data/log_data''
    
    -It will fetch all the files in the specified filepath in the directory and call process_song_file/process_log_file function to insert the data.
    """
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Main function to call etl functions.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()