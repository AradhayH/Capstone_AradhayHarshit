import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = "data/mobility.db"
SQL_FILE = "Codes/sql_queries/analytical_queries.sql"
OUTPUT_DIR = Path("./outputs/sqloutputs")

# print(OUTPUT_DIR.resolve())

def parse_queries(sql_text):
    blocks = sql_text.split("--")
    queries = {}

    for block in blocks:
        block = block.strip()
        if not block:
            continue
        lines = block.splitlines()
        name = lines[0].strip()
        query = "\n".join(lines[1:]).strip()
        queries[name] = query

    return queries

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(SQL_FILE, "r") as f:
        sql_text = f.read()

    queries = parse_queries(sql_text)
    conn = sqlite3.connect(DB_PATH)

    for name, query in queries.items():
        print(f"Running: {name}")
        df = pd.read_sql_query(query, conn)
        out_path = OUTPUT_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        print(name)
        print(df)
        print("\n\n")

    conn.close()
    print("All SQL analytics exported.")

if __name__ == "__main__":
    main()
