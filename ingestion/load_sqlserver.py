import os
import sys
import pymssql
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

# Set Hadoop environment for Windows compatibility
if "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "hadoop"))
if os.path.exists(os.environ["HADOOP_HOME"]):
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Retrieve variables from .env with fallback defaults
HOST = os.environ.get("SQLSERVER_HOST", "localhost")
PORT = os.environ.get("SQLSERVER_PORT", "1433")
USER = os.environ.get("SQLSERVER_USER", "sa")
PASSWORD = os.environ.get("SQLSERVER_PASSWORD", "YourStrongPassword123!")
DB_NAME = os.environ.get("SQLSERVER_DB", "final_project")

SQLSERVER_JDBC = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"
JDBC_URL_DB = f"jdbc:sqlserver://{HOST}:{PORT};databaseName={DB_NAME};encrypt=true;trustServerCertificate=true"

def main():
    # 1. Create the database if it does not exist using pymssql
    print(f"Checking and creating SQL Server database '{DB_NAME}' if not exists...")
    try:
        conn = pymssql.connect(server=HOST, user=USER, password=PASSWORD, port=int(PORT))
        conn.autocommit(True)
        cursor = conn.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{DB_NAME}') CREATE DATABASE {DB_NAME}")
        cursor.close()
        conn.close()
        print(f"Database '{DB_NAME}' is ready.")
    except Exception as e:
        print(f"Error checking/creating database via pymssql: {e}")
        sys.exit(1)

    # 2. Initialize Spark Session with SQL Server JDBC driver
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("SQLServer-Ingestion") \
        .config("spark.jars.packages", SQLSERVER_JDBC) \
        .getOrCreate()
    
    # 3. Load the CSV file
    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw_dataset.csv"))
    print(f"Reading CSV from: {csv_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print(f"Loaded {df.count():,} rows from CSV.")

    # 4. Write data to SQL Server
    print(f"Writing raw loans data to SQL Server table ({DB_NAME}.dbo.loans)...")
    df.write.format("jdbc") \
        .option("url", JDBC_URL_DB) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "loans") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .mode("overwrite") \
        .save()
    
    print("Ingestion completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
