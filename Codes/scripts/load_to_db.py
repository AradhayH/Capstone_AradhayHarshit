import pandas as pd
import sqlite3
from pathlib import Path

DB_PATH = "data/mobility.db"
CLEAN_DATA_PATH = "data/cleaned/yellow_tripdata_cleaned.csv"
TABLE_NAME = "trips"

def main():
    Path("data").mkdir(exist_ok=True)

    print("Loading cleaned CSV...")
    df = pd.read_csv(CLEAN_DATA_PATH)

    print("Connecting to SQLite...")
    conn = sqlite3.connect(DB_PATH)

    print("Writing to database...")
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    conn.close()
    print(f"Data loaded into {DB_PATH} as table '{TABLE_NAME}'")

if __name__ == "__main__":
    main()
