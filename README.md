# Final Project — SQL Server & PySpark Loan Default Prediction Pipeline

This repository covers the complete final project data pipeline: ingesting the lending dataset into Microsoft SQL Server, performing feature engineering and predictive machine learning using PySpark, exporting results/predictions back to the database, and loading them live into Power BI.

---

## Architecture

```
Raw Dataset (CSV)
       │
PySpark Ingestion (load_sqlserver.py)
       │
Microsoft SQL Server (Docker Container on Port 1433)
       │
PySpark Notebook Pipeline (model_prediction.ipynb)
       ├── Aggregations & ML Modeling
       └── Saves back tables: 'processed_results' & 'loans_predictions'
       │
Power BI Desktop (Direct connection via native SQL Server connector)
```

---

## Dataset

* **`data/raw_dataset.csv`**: A 10,000-row stratified sample of the LendingClub loan dataset. Contains borrower metrics (income, FICO, DTI, revolving balance) and a `default` label (where 1 indicates default).

---

## Setup & Prerequisites

### 1. Python Environment (Windows)
Set up the python virtual environment using `uv` (recommended) or standard Python 3.11:

**Using `uv`:**
```bash
# Create virtual environment
uv venv --python 3.11

# Install requirements (use trusted-host if behind an SSL intercepting firewall)
.venv\Scripts\pip install -r requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org
```

### 2. SQL Server Setup (Docker)
We run Microsoft SQL Server 2022 inside a lightweight Docker container for the database layer:
```bash
# Start SQL Server
docker compose up -d
```
* **Server Address:** `127.0.0.1:1433` (or `localhost:1433`)
* **Admin User:** `sa`
* **Admin Password:** `YourStrongPassword123!`

### 3. Configure Environment (`.env`)
Create a `.env` file in the project root containing your SQL Server credentials:
```env
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=YourStrongPassword123!
SQLSERVER_DB=final_project
```

---

## Running the Pipeline

### Step 1: Ingest Data into SQL Server
Run the ingestion script. It connects to SQL Server, creates the `final_project` database, and loads the CSV data into the `loans` table:
```bash
& .venv\Scripts\python.exe ingestion/load_sqlserver.py
```

### Step 2: Execute Notebook Pipeline
Open [model_prediction.ipynb](spark/model_prediction.ipynb) in VS Code (select the `.venv` Python kernel) and run all cells:
* Computes summary statistics and correlation matrices.
* Performs aggregations (count, average interest rates, and average defaults by credit grade) and writes them to the `processed_results` table in SQL Server.
* Features engineering (fico average, ratios, ordinal credit sub-grade mapping `sub_grade_num`).
* Trains an optimized ElasticNet **Logistic Regression** classifier, achieving a **Test Accuracy of 80.53%**.
* Generates default predictions and risk probabilities for all 10,000 records, and writes them to the `loans_predictions` table in SQL Server.

---

## Visualizing in Power BI

Because SQL Server and Power BI are both Microsoft products, they integrate natively without any ODBC DSN configuration or SQL proxy:

1. In Power BI Desktop, click **Get Data** -> **SQL Server database**.
2. Enter the connection settings:
   * **Server:** `127.0.0.1` (or `localhost`)
   * **Database:** `final_project`
   * **Connectivity mode:** Select **Import** or **DirectQuery** (choose DirectQuery for real-time live queries).
3. Under the **Database** credentials tab, authenticate with:
   * **User name:** `sa`
   * **Password:** `YourStrongPassword123!`
4. In the **Navigator** window, check the tables:
   * `loans` (raw data)
   * `processed_results` (grade summary table)
   * `loans_predictions` (predictions and risk probabilities table)
5. Click **Load** to begin building your visual reports immediately!
