🎵 Spotify End-to-End Azure Data Pipeline
A production-style data engineering project built on Azure Data Factory, Databricks, and Delta Live Tables — implementing a full Bronze → Silver → Gold Medallion Architecture with incremental ingestion, SCD Type 2 dimension tracking, and Jinja-powered dynamic SQL queries.

🏗️ Architecture Overview
Azure SQL Database (Source)
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│                    Azure Data Factory                        │
│                                                              │
│   incremental_loop (Parent Pipeline)                         │
│   ┌──────────────────────────────────────────────────────┐  │
│   │  ForEach (5 tables)                                  │  │
│   │  ┌────────────────────────────────────────────────┐  │  │
│   │  │  last_cdc (Lookup)                             │  │  │
│   │  │  current  (Set Variable → utcNow)              │  │  │
│   │  │  AzureSQLtoLake (Copy Data — filtered by CDC)  │  │  │
│   │  │       │                                        │  │  │
│   │  │  Ifincrementaldata (If Condition)              │  │  │
│   │  │    ├── True  → max_cdc → update_last_cdc       │  │  │
│   │  │    └── False → DeleteEmptyFile                 │  │  │
│   │  └────────────────────────────────────────────────┘  │  │
│   └──────────────────────────────────────────────────────┘  │
│            │                                                  │
│         Alerts (Logic App — Pass/Fail webhook)               │
└──────────────────────────────────────────────────────────────┘
        │
        ▼
ADLS Gen2 — Bronze Layer
  /bronze/{table_name}/{table_name}_{timestamp}.parquet
        │
        ▼
Databricks — Silver Layer (Cleaned & Conformed)
        │
        ▼
Databricks Delta Live Tables — Gold Layer (SCD Type 2 Dimensions)

📦 Tech Stack
ComponentTool / ServiceOrchestrationAzure Data FactorySourceAzure SQL DatabaseStorageADLS Gen2 (Bronze → Silver → Gold)File FormatParquet (Snappy compressed)ProcessingDatabricks (PySpark, Spark SQL)CDC StrategyWatermark-based (last updated timestamp)Dimension ModelSCD Type 2 via Delta Live TablesDynamic SQLJinja2 templatingAlertingAzure Logic App (HTTP webhook)CatalogUnity Catalog (spotify_cata)InfrastructureAzure Free Tier

📂 Tables Ingested
SchemaTableCDC ColumndboDimUserupdated_atdboDimTrackupdated_atdboDimDatedatedboDimArtistupdated_atdboFactStreamstream_timestamp

⚙️ Pipeline Design
Layer 1 — Bronze (Azure Data Factory)
incremental_loop — Parent Pipeline
Accepts an array of table configs and loops through each using ForEach orchestration.
Inside each loop iteration:

last_cdc (Lookup) — Reads the last watermark timestamp from cdc.json stored in ADLS for that table.
current (Set Variable) — Captures utcNow() as the current run timestamp for file naming.
AzureSQLtoLake (Copy Data) — Runs a dynamic SQL query filtering rows where cdc_col > last_cdc. Writes to bronze as a timestamped Parquet file.
Ifincrementaldata (If Condition) — Checks if any data was actually copied (dataRead > 0).

True → max_cdc (Script) fetches the new max CDC value → update_last_cdc overwrites cdc.json.
False → DeleteEmptyFile removes the empty Parquet file to keep storage clean.


Alerts (Web Activity) — Posts pipeline status to an Azure Logic App on both success and failure.

incremental_ingestion — Child Pipeline
Same logic as above, designed to run for a single table with parameters passed directly. Used for targeted reruns or backfills.

Layer 2 — Silver (Databricks)
Raw Bronze Parquet data is read, cleaned, and conformed in Databricks notebooks using PySpark.

Schema enforcement and deduplication
Data type casting and standardization
Written as Delta tables under spotify_cata.silver.*


Layer 3 — Gold (Databricks Delta Live Tables)
Dimension tables are built in the Gold layer using Delta Live Tables (DLT) with apply_changes (SCD Type 2) — automatically tracking historical changes to dimension records.
DimUser — with Data Quality Expectations
pythonimport dlt

expectation = {"Rule_1": "user_id IS NOT NULL"}

@dlt.table
@dlt.expect_all_or_drop(expectation)
def dimuser_stg():
    return spark.readStream.table("spotify_cata.silver.dimuser")

dlt.create_streaming_table("dimuser", expect_all_or_drop=expectation)

dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
DimTrack
pythonimport dlt

@dlt.table
def dimtrack_stg():
    return spark.readStream.table("spotify_cata.silver.dimtrack")

dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrack_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
DimDate
pythonimport dlt

@dlt.table
def dimdate_stg():
    return spark.readStream.table("spotify_cata.silver.dimdate")

dlt.create_streaming_table("dimdate")

dlt.create_auto_cdc_flow(
    target="dimdate",
    source="dimdate_stg",
    keys=["date_key"],
    sequence_by="date",
    stored_as_scd_type=2
)

Jinja2 Dynamic SQL (Query Generation)
A Jinja2-powered SQL builder dynamically constructs multi-table JOIN queries from a Python config — eliminating hardcoded SQL and enabling metadata-driven query generation.
pythonparameters = [
    {
        "table": "spotify_cata.silver.dimtrack",
        "alias": "dimtrack",
        "cols": ["dimtrack.track_id", "dimtrack.track_name"]
    },
    {
        "table": "spotify_cata.silver.dimartist",
        "alias": "dimartist",
        "cols": ["dimartist.artist_name", "dimartist.genre"],
        "condition": "dimtrack.artist_id = dimartist.artist_id"
    }
]
The Jinja template loops over the config and builds the complete SELECT ... FROM ... LEFT JOIN ... query dynamically, which is then executed via spark.sql(query).

🗂️ Dynamic Datasets (ADF)
parquet_dynamic
Parameterized Parquet dataset pointing to ADLS Gen2.
ParameterDescriptioncontainerADLS container name (e.g., bronze)folderTable name used as folder pathfileFilename = {table}_{timestamp}.parquet
Compression: Snappy
azure_sql
Linked dataset pointing to Azure SQL with no hardcoded table — table is injected dynamically via the SQL query in the Copy activity.

🔗 Linked Services
NameTypeTargetdatalakeAzureBlobFSjunaidazureproject.dfs.core.windows.netazure_sqlAzureSqlDatabaseazureprojectserverjun.database.windows.net / azureprojectdbjun
Authentication: SQL auth with encrypted credentials stored in ADF.

🚀 How to Run
Full Load — All Tables
Trigger incremental_loop with the default loop_input parameter array. No changes needed — defaults are pre-configured.
Backfill a Specific Table
Run incremental_ingestion with parameters:
json{
  "schema": "dbo",
  "table": "FactStream",
  "cdc": "stream_timestamp",
  "from_date": "2024-01-01T00:00:00Z"
}
Setting from_date overrides the watermark from cdc.json and pulls all data from that date forward.
Run Gold DLT Pipeline
Deploy the gold_pipeline in Databricks under Workflows → Delta Live Tables. The pipeline runs all three dimension flows (DimDate, DimTrack, DimUser) in a single DAG.

📁 Project Structure
spotify-azure-end-to-end/
│
├── pipelines/
│   ├── incremental_loop.json          # Parent ADF pipeline
│   └── incremental_ingestion.json     # Child ADF pipeline (single table)
│
├── datasets/
│   ├── parquet_dynamic.json
│   └── azure_sql.json
│
├── linkedServices/
│   ├── datalake.json
│   └── azure_sql.json
│
├── databricks/
│   ├── silver/
│   │   └── silver_dimensions.ipynb    # Silver layer transformation notebook
│   ├── gold/
│   │   ├── DimDate.py                 # DLT pipeline — DimDate SCD2
│   │   ├── DimTrack.py                # DLT pipeline — DimTrack SCD2
│   │   └── DimUser.py                 # DLT pipeline — DimUser SCD2 + expectations
│   └── utils/
│       └── Jinja_notebook.ipynb       # Dynamic SQL generation using Jinja2
│
├── loop_input.txt                     # Table config array for ForEach
└── README.md

🧠 Key Design Decisions
DecisionRationaleWatermark-based CDCOnly new/updated rows are pulled each run — keeps costs and runtime low vs. full loadDynamic ADF datasetsOne Parquet dataset handles all 5 tables via parameters — no duplicationEmpty file cleanupIf no new data exists, the empty Parquet is deleted — storage stays cleanTimestamp in filenameEach run produces a unique file (Table_2024-04-08T10:00:00Z.parquet) — enables audit and reprocessingSCD Type 2 via DLTHistorical tracking of dimension changes without custom MERGE logicDLT data quality expectationsexpect_all_or_drop on DimUser enforces user_id IS NOT NULL at pipeline levelJinja2 SQL generationMetadata-driven query construction — avoids hardcoded SQL for multi-table JOINsAlert on success and failureLogic App webhook fires regardless of outcome — full pipeline observability

📸 Pipeline Screenshots
ADF — incremental_loop (ForEach orchestration)
ADF — incremental_ingestion (If Condition branch)
Databricks — gold_pipeline DLT run (Completed)
Azure Resource Group — Full infrastructure overview

👤 Author
Junaid Khan — Data Engineer
📍 Hyderabad, India
🔗 linkedin.com/in/junaid-khan07
🐙 github.com/iamjunaidatgit
