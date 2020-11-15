import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
import datetime
import sqlite3


DATABASE_LOCATION = 'sqlite:///my_played_tracks.sqlite'
USER_ID = 
TOKEN = 

#def check_if_valid_data(df: pd.DataFrame) -> bool:
#    # Check if dataframe is empty
#    if df.empty:
#        print('No songs downloaded. Finishing execution')
#        return False 
#
#    # Primary Key Check
#    if pd.Series(df['played_at']).is_unique:
#        pass
#    else:
#        raise Exception('Primary Key check is violated')
#
#    # Check for nulls
#    if df.isnull().values.any():
#        raise Exception('Null values found')
#
#    # Check that all timestamps are of yesterday's date
#    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
#    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
#
#    timestamps = df['timestamp'].tolist()
#    for timestamp in timestamps:
#        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
#            raise Exception('At least one of the returned songs does not have a yesterdays timestamp')
#
#    return True


# Extract part of the ETL process
def extract_the_data():
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    # Convert time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=2)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time = yesterday_unix_timestamp), headers = headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = [] # The date and time the track was played (timestamp format)
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at']) #played at = play history object key 
        timestamps.append(song['played_at'][0:10]) #slicing the first 10 numbers of the 'played at' output 

    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)

    # Validate
    #if check_if_valid_data(song_df):
    #    print("Data valid, proceed to Load stage")

    #Load

    #creating the engine, if it doesn't exists it will be created automatically
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)

    #initiating the connection to our new database
    connection = sqlite3.connect('my_played_tracks.sqlite')

    #creating cursor which is a pointer which allows us to refer to specific rows in a database
    cursor = connection.cursor()

    sql_query = '''
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(100),
        artist_name VARCHAR(100),
        played_at VARCHAR(100),
        timestamp VARCHAR(100),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    '''


    cursor.execute(sql_query)
    print('Created the database sucessfully')

    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append') # index = False because we dont want the number rank of pandas included in our df
    except:
        print('Data already exists')
    
    #closing the connection
    connection.close
    print('Closed the database sucessfully')
   

extract_the_data()