"""
Verify PySpark can connect to MongoDB Atlas and read the `loans` collection.

Uses mongo-spark-connector 10.x (Spark 3.5 / Mongo Spark Connector v10 API),
which uses format("mongodb") and the spark.mongodb.read.connection.uri config
(the older format("mongo") + connector 3.0.1 API targets Spark 2.4/3.0).
"""

import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

# Ensure Spark workers use the same Python interpreter as the driver (this venv).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

MONGODB_URI = os.environ["MONGODB_URI"]
MONGODB_DB = os.environ.get("MONGODB_DB", "final_project")
MONGODB_COLLECTION = os.environ.get("MONGODB_COLLECTION", "loans")

MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"


def get_spark():
    return (
        SparkSession.builder.appName("FinalProject-MongoConnect")
        .config("spark.jars.packages", MONGO_CONNECTOR_PACKAGE)
        .config("spark.mongodb.read.connection.uri", MONGODB_URI)
        .config("spark.mongodb.write.connection.uri", MONGODB_URI)
        .getOrCreate()
    )


def main():
    spark = get_spark()

    df = (
        spark.read.format("mongodb")
        .option("database", MONGODB_DB)
        .option("collection", MONGODB_COLLECTION)
        .load()
    )

    print(f"Connected to {MONGODB_DB}.{MONGODB_COLLECTION}")
    print(f"Row count: {df.count():,}")
    df.printSchema()
    df.show(5)

    spark.stop()


if __name__ == "__main__":
    main()
