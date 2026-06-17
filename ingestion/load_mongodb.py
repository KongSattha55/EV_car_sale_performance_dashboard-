"""
Load data/raw_dataset.csv into MongoDB Atlas.

Reads connection details from .env (MONGODB_URI, MONGODB_DB, MONGODB_COLLECTION),
inserts the dataset as JSON documents, and verifies the result.
"""

import os

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGODB_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ.get("MONGODB_DB", "final_project")
MONGODB_COLLECTION = os.environ.get("MONGODB_COLLECTION", "loans")
INPUT_CSV = "data/raw_dataset.csv"


def main():
    df = pd.read_csv(INPUT_CSV)
    print(f"Read {len(df):,} rows x {len(df.columns)} cols from {INPUT_CSV}")

    records = df.to_dict(orient="records")

    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]

    # Idempotent reload
    collection.delete_many({})
    result = collection.insert_many(records)

    print(f"Inserted {len(result.inserted_ids):,} documents into "
          f"{MONGODB_DB}.{MONGODB_COLLECTION}")
    print(f"Collection count: {collection.count_documents({}):,}")
    print("Sample document:")
    print(collection.find_one())
    print(f"Collections in {MONGODB_DB}: {db.list_collection_names()}")

    client.close()


if __name__ == "__main__":
    main()
