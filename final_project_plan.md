# Final Project Plan: PySpark/Kafka/Hadoop → SQL Server/MongoDB → Power BI

**Course:** Big Data / Database Systems  
**Presentation Date:** June 19, 2026  
**Student:** Kong Sattha

---

## Project Overview

A 4-layer data pipeline that ingests a dataset into SQL Server and MongoDB, processes and analyzes it using PySpark (with optional Kafka/Hadoop), and visualizes results in Power BI or Tableau.

### Minimum Requirements Checklist

- [ ] Input data into SQL Server and/or MongoDB
- [ ] Use PySpark to connect to SQL Server / MongoDB
- [ ] Apply data analysis using PySpark / Kafka / Hadoop
- [ ] Visualize results in Power BI / Tableau from SQL Server / MongoDB

---

## Architecture

```
[Raw Data Source]
   CSV / JSON / API
        │
        ▼
┌───────────────────────────────┐
│         INGEST LAYER          │
│  SQL Server (AWS RDS Free)    │◄──── also ────►  MongoDB Atlas (M0 Free)
└───────────────────────────────┘
        │                                                │
        ▼ (optional)                                     ▼ (optional)
  ┌──────────┐          ┌─────────────┐
  │  Kafka   │          │ Hadoop HDFS │   ← pick one for bonus points
  └──────────┘          └─────────────┘
        │                      │
        └──────────┬────────────┘
                   ▼
        ┌─────────────────────┐
        │       PySpark       │
        │  EDA · Aggregations │
        │  Transformations    │
        │  Basic ML (optional)│
        └─────────────────────┘
                   │
                   ▼
        ┌─────────────────────┐
        │  Power BI / Tableau │
        │  (connects to DB)   │
        └─────────────────────┘
```

---

## Recommended Stack (Mac-Friendly)

| Layer | Tool | Notes |
|-------|------|-------|
| SQL Server | AWS RDS Free Tier (`db.t3.micro`) | Free 12 months, accessible from Mac |
| MongoDB | MongoDB Atlas M0 (Free Cluster) | No local install, cloud-hosted |
| Processing | PySpark (local) | JDBC for SQL Server, `mongo-spark-connector` for MongoDB |
| Optional streaming | Apache Kafka | Docker Compose setup |
| Optional storage | Hadoop HDFS | Docker Compose setup |
| Visualization | Power BI Desktop / Tableau Public | Power BI via ODBC on Mac (Parallels) or Power BI Service online |

---

## Dataset Options

Pick one — small but meaningful for analysis (10k+ rows recommended):

- **Loan/credit data** — reuse cleaned LendingClub data from Loan Default Predictor project
- **Economic indicators** — reuse World Bank/IMF data from EconFlow Pipeline project
- Any public CSV with numeric columns for aggregation, trends, and basic modeling

---

## Week-by-Week Timeline

### Now → June 13 — Setup & Data Ingestion

**Goals:** Environment ready, data loaded into both databases, PySpark connection verified.

**Tasks:**
- [ ] Spin up AWS RDS SQL Server (Free Tier) OR MongoDB Atlas M0 cluster
- [ ] Write Python ingestion script to load dataset into SQL Server and MongoDB
- [ ] Confirm PySpark can read from both:
  - SQL Server: `spark.read.jdbc(url, table, properties)`
  - MongoDB: `spark.read.format("mongo").option("uri", ...).load()`
- [ ] (Optional) Set up Docker Compose with Kafka or Hadoop HDFS

**Key code — PySpark + SQL Server:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FinalProject") \
    .config("spark.jars", "mssql-jdbc-12.4.2.jre11.jar") \
    .getOrCreate()

jdbc_url = "jdbc:sqlserver://<RDS_ENDPOINT>:1433;databaseName=<DB_NAME>"
properties = {
    "user": "<USERNAME>",
    "password": "<PASSWORD>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbc_url, table="your_table", properties=properties)
df.show(5)
```

**Key code — PySpark + MongoDB:**
```python
spark = SparkSession.builder \
    .appName("FinalProject") \
    .config("spark.mongodb.input.uri", "mongodb+srv://<user>:<pass>@cluster.mongodb.net/db.collection") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.format("mongo").load()
df.show(5)
```

---

### June 14–15 — PySpark Analysis

**Goals:** At least 3 meaningful analyses completed, results written back to the database.

**Minimum analyses to include:**
1. **Summary statistics** — `df.describe().show()`
2. **Groupby aggregation** — e.g. average loan amount by grade, or GDP by region
3. **Correlation / trend analysis** — `df.stat.corr("col_a", "col_b")` or time-series grouping

**Optional (bonus):**
- Train a simple `pyspark.ml` classifier or regressor (e.g. LogisticRegression, RandomForestClassifier)
- Kafka: add a producer that streams rows into the database in real time (even 30 seconds of live streaming is impressive)
- Hadoop: read raw CSV from HDFS first, then write processed output to SQL Server/MongoDB

**Write results back to DB:**
```python
# Write processed/gold table back to SQL Server
df_result.write.jdbc(url=jdbc_url, table="processed_results", mode="overwrite", properties=properties)
```

---

### June 16–17 — Power BI / Tableau Dashboard

**Goals:** 3–4 polished visuals connected to your live database.

**Recommended visuals:**
1. **KPI card** — total records, average value, or model accuracy
2. **Bar or column chart** — aggregated metric by category
3. **Line chart** — trend over time
4. **Distribution / histogram** — key numeric variable

**Power BI connection (Mac via Parallels or Power BI Service):**
- Use ODBC driver for SQL Server: `Server=<RDS_ENDPOINT>,1433; Database=<DB>; UID=...; PWD=...`
- Import the `processed_results` table (written by PySpark in the previous step)

**Tableau Public (no Parallels needed):**
- Connect to MongoDB Atlas via the MongoDB Connector for BI (mongosql)
- Or export PySpark results to CSV and connect from Tableau directly

---

### June 18 — Slides & Rehearsal

**Presentation structure (10–15 min):**

1. **Problem statement** (1 min) — what dataset, what questions
2. **Architecture diagram** (1 min) — show the pipeline overview
3. **Demo: data ingestion** (2 min) — show Python script inserting rows, confirm in DB
4. **Demo: PySpark connection & analysis** (3–4 min) — run `spark.read.jdbc(...)`, show aggregation outputs
5. **Demo: Power BI dashboard** (2–3 min) — live visuals connected to the database
6. **Conclusions** (1–2 min) — findings, what you would improve, lessons learned

**Tips:**
- Use a small subset of data (5k–10k rows) so queries run fast during live demo
- Have fallback screenshots in case of network issues on the day
- Briefly mention Kafka or Hadoop even if optional — shows you understand the full stack

---

### June 19 — Presentation Day 🎯

---

## Quick Wins to Impress

- Show `spark.read.jdbc(...)` or `spark.read.format("mongo")` **live** — directly satisfies requirement 2
- Add a SHAP plot or feature importance from a PySpark MLlib model — ties to your Loan Default Predictor
- If using Kafka: show consumer lag or a live message counter — even a 30-second stream demo is memorable
- Frame your pipeline using the **medallion architecture** you used in the Finnhub project:
  - **Bronze** → raw data in SQL Server
  - **Silver** → PySpark-cleaned table written back to SQL Server/MongoDB
  - **Gold** → aggregated results consumed by Power BI

---

## Project File Structure

```
final-project/
├── data/
│   └── raw_dataset.csv
├── ingestion/
│   ├── load_sqlserver.py        # insert data into SQL Server
│   └── load_mongodb.py          # insert data into MongoDB
├── spark/
│   ├── connect_sqlserver.py     # PySpark + JDBC read/write
│   ├── connect_mongodb.py       # PySpark + Mongo connector
│   └── analysis.py              # EDA, aggregations, optional ML
├── kafka/                       # optional
│   ├── producer.py
│   └── consumer.py
├── powerbi/
│   └── dashboard.pbix           # Power BI report file
├── docker-compose.yml           # optional: Kafka + Hadoop setup
├── requirements.txt
└── README.md
```

---

## Key Dependencies

```txt
pyspark==3.5.1
pymongo==4.7.2
pyodbc==5.1.0
pandas==2.2.2
scikit-learn==1.5.0        # optional, for preprocessing
kafka-python==2.0.2        # optional
python-dotenv==1.0.1
```

---

## Notes

- **Mac users:** SQL Server runs on AWS RDS Free Tier — no local Windows VM required for the database itself. Power BI Desktop requires Windows (use Parallels) or use Power BI Service in the browser.
- **MongoDB Atlas** M0 free cluster requires no credit card and is accessible from any IP.
- **JDBC driver** for SQL Server: download `mssql-jdbc-12.x.jre11.jar` from Microsoft and pass it to `spark.jars`.
- Keep credentials in a `.env` file — never commit passwords to GitHub.
