import sqlite3
from pathlib import Path

DB_PATH = Path("C:/Users/User/OneDrive/flight-delay-pipeline/data/flight_delays.db")

def load_to_sqlite(df, table_name):
    """
    Loads a DataFrame into SQLite, replacing existing tables.
    """
    conn = sqlite3.connect(DB_PATH)

    try:
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
        print(f"Loaded {table_name} into SQLite")
    finally:
        conn.close()

