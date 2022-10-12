import sqlalchemy
import sqlite3


DB_NAME = "my_spotify_data.db"
DB_LOCATION = "sqlite:///my_spotify_data.db"


def create_sqlite_database(db_location: str, db_name: str) -> None:
    """
    Creates database and streaming history table if not exist.

    Params:
    -------
        db_location: str - location of the database.
        db_name    : str - name of the database.

    Returns:
    --------
        None.
    """

    engine = sqlalchemy.create_engine(db_location)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = """
    CREATE TABLE IF NOT EXISTS STREAMING_HISTORY(
        song VARCHAR(128),
        artist VARCHAR(128),
        played_at VARCHAR(128),
        timestamp VARCHAR(128),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(query)
    conn.close()


if __name__ == "__main__" :
    create_sqlite_database(DB_LOCATION, DB_NAME)
