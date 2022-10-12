import requests
import json
import pandas as pd
import datetime
import sqlalchemy

# Importing DB information form database.py
from database import DB_LOCATION, DB_NAME

# Spotify credentials
USER_ID = ""
token = ""

# API route
URL = "https://api.spotify.com/v1/me/player/recently-played"


def is_valid_data(df: pd.DataFrame) -> bool :
    """
    Checks if data from spotify is valid.

    Params:
    -------
        df: pd.DataFrame - Data from spotify

    Returns:
    --------
        bool - test result
    """

    if df.empty:
        return False

    if df.isnull().values.any():
        raise Exception("Null values found")

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("Data contains wrong Timestamp")
    
    return True


def get_streaming_data() -> pd.DataFrame :
    """
    Retrieves Data from spotify API.

    Returns:
    --------
        pd.DataFrame - streaming data
    """
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    params = {
        "after": yesterday_unix_timestamp
    }

    r = requests.get(URL, params=params, headers = headers)

    data = r.json()

    songs = []
    artists = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        songs.append(song["track"]["name"])
        artists.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    dict = {
        "song" : songs,
        "artist": artists,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    data_df = pd.DataFrame.from_dict(dict)

    return data_df


def run_etl_process() :
    # Getting data
    streaming_df = get_streaming_data()
    
    # checking if data is valid
    if not is_valid_data(streaming_df):
        print("Something is wrong with the collected data !")

    # Storing data
    engine = sqlalchemy.create_engine(DB_LOCATION)

    
    nb_added_rows = streaming_df.to_sql("STREAMING_HISTORY", engine, index=False, if_exists='append')

    if nb_added_rows == None and streaming_df.shape[0] != 0:
        raise Exception("Error adding data to database !")
