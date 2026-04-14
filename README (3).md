# 🎵 Spotify End-to-End Azure Data Pipeline

A production-style data engineering project built on **Azure Data Factory**, **Databricks**, and **Delta Live Tables** — implementing a full Bronze → Silver → Gold Medallion Architecture with incremental ingestion, SCD Type 2 dimension tracking, and Jinja-powered dynamic SQL queries.

---

## 🏗️ Architecture Overview

```
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
```

---

## 📦 Tech Stack

| Component        | Tool / Service                        |
|------------------|---------------------------------------|
| Orchestration    | Azure Data Factory                    |
| Source           | Azure SQL Database                    |
| Storage          | ADLS Gen2 (Bronze → Silver → Gold)    |
| File Format      | Parquet (Snappy compressed)           |
| Processing       | Databricks (PySpark, Spark SQL)       |
| CDC Strategy     | Watermark-based (last updated timestamp) |
| Dimension Model  | SCD Type 2 via Delta Live Tables      |
| Dynamic SQL      | Jinja2 templating                     |
| Alerting         | Azure Logic App (HTTP webhook)        |
| Catalog          | Unity Catalog (`spotify_cata`)        |
| Infrastructure   | Azure Free Tier                       |

---

## 📂 Tables Ingested

| Schema | Table       | CDC Column         |
|--------|-------------|--------------------|
| dbo    | DimUser     | updated_at         |
| dbo    | DimTrack    | updated_at         |
| dbo    | DimDate     | date               |
| dbo    | DimArtist   | updated_at         |
| dbo    | FactStream  | stream_timestamp   |

---

## ⚙️ Pipeline Design

### Layer 1 — Bronze (Azure Data Factory)

#### `incremental_loop` — Parent Pipeline
Accepts an array of table configs and loops through each using **ForEach** orchestration.

**Inside each loop iteration:**

1. **`last_cdc` (Lookup)** — Reads the last watermark timestamp from `cdc.json` stored in ADLS for that table.
2. **`current` (Set Variable)** — Captures `utcNow()` as the current run timestamp for file naming.
3. **`AzureSQLtoLake` (Copy Data)** — Runs a dynamic SQL query filtering rows where `cdc_col > last_cdc`. Writes to bronze as a timestamped Parquet file.
4. **`Ifincrementaldata` (If Condition)** — Checks if any data was actually copied (`dataRead > 0`).
   - **True** → `max_cdc` (Script) fetches the new max CDC value → `update_last_cdc` overwrites `cdc.json`.
   - **False** → `DeleteEmptyFile` removes the empty Parquet file to keep storage clean.
5. **`Alerts` (Web Activity)** — Posts pipeline status to an Azure Logic App on both success and failure.

#### `incremental_ingestion` — Child Pipeline
Same logic as above, designed to run for a **single table** with parameters passed directly. Used for targeted reruns or backfills.

---

### Layer 2 — Silver (Databricks)

Raw Bronze Parquet data is read, cleaned, and conformed in Databricks notebooks using PySpark.

- Schema enforcement and deduplication
- Data type casting and standardization
- Written as Delta tables under `spotify_cata.silver.*`

---

### Layer 3 — Gold (Databricks Delta Live Tables)

Dimension tables are built in the Gold layer using **Delta Live Tables (DLT)** with `apply_changes` (SCD Type 2) — automatically tracking historical changes to dimension records.

#### `DimUser` — with Data Quality Expectations
```python
import dlt

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
```

#### `DimTrack`
```python
import dlt

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
```

#### `DimDate`
```python
import dlt

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
```

---

### Jinja2 Dynamic SQL (Query Generation)

A **Jinja2-powered SQL builder** dynamically constructs multi-table JOIN queries from a Python config — eliminating hardcoded SQL and enabling metadata-driven query generation.

```python
parameters = [
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
```

The Jinja template loops over the config and builds the complete `SELECT ... FROM ... LEFT JOIN ...` query dynamically, which is then executed via `spark.sql(query)`.

---

## 🗂️ Dynamic Datasets (ADF)

### `parquet_dynamic`
Parameterized Parquet dataset pointing to ADLS Gen2.

| Parameter   | Description                                       |
|-------------|---------------------------------------------------|
| `container` | ADLS container name (e.g., `bronze`)              |
| `folder`    | Table name used as folder path                    |
| `file`      | Filename = `{table}_{timestamp}.parquet`          |

Compression: **Snappy**

### `azure_sql`
Linked dataset pointing to Azure SQL with no hardcoded table — table is injected dynamically via the SQL query in the Copy activity.

---

## 🔗 Linked Services

| Name        | Type          | Target                                                        |
|-------------|---------------|---------------------------------------------------------------|
| `datalake`  | AzureBlobFS   | `junaidazureproject.dfs.core.windows.net`                     |
| `azure_sql` | AzureSqlDatabase | `azureprojectserverjun.database.windows.net / azureprojectdbjun` |

Authentication: SQL auth with encrypted credentials stored in ADF.

---

## 🚀 How to Run

### Full Load — All Tables
Trigger `incremental_loop` with the default `loop_input` parameter array. No changes needed — defaults are pre-configured.

### Backfill a Specific Table
Run `incremental_ingestion` with parameters:

```json
{
  "schema": "dbo",
  "table": "FactStream",
  "cdc": "stream_timestamp",
  "from_date": "2024-01-01T00:00:00Z"
}
```

Setting `from_date` overrides the watermark from `cdc.json` and pulls all data from that date forward.

### Run Gold DLT Pipeline
Deploy the `gold_pipeline` in Databricks under **Workflows → Delta Live Tables**. The pipeline runs all three dimension flows (`DimDate`, `DimTrack`, `DimUser`) in a single DAG.

---

## 📁 Project Structure

```
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
```

---

## 🧠 Key Design Decisions

| Decision | Rationale |
|---|---|
| **Watermark-based CDC** | Only new/updated rows are pulled each run — keeps costs and runtime low vs. full load |
| **Dynamic ADF datasets** | One Parquet dataset handles all 5 tables via parameters — no duplication |
| **Empty file cleanup** | If no new data exists, the empty Parquet is deleted — storage stays clean |
| **Timestamp in filename** | Each run produces a unique file (`Table_2024-04-08T10:00:00Z.parquet`) — enables audit and reprocessing |
| **SCD Type 2 via DLT** | Historical tracking of dimension changes without custom MERGE logic |
| **DLT data quality expectations** | `expect_all_or_drop` on `DimUser` enforces `user_id IS NOT NULL` at pipeline level |
| **Jinja2 SQL generation** | Metadata-driven query construction — avoids hardcoded SQL for multi-table JOINs |
| **Alert on success and failure** | Logic App webhook fires regardless of outcome — full pipeline observability |

---

## 📸 Pipeline Screenshots

**ADF — incremental_loop (ForEach orchestration)**
<img width="1187" height="545" alt="image" src="https://github.com/user-attachments/assets/06509656-5c2e-4620-af98-32eec73009e0" />


**ADF — incremental_ingestion (If Condition branch)**
<img width="436" height="491" alt="image" src="https://github.com/user-attachments/assets/ae5f40b6-3511-4eb5-906a-7d89fed53e5b" />

**Databricks — gold_pipeline DLT run (Completed)**
<img width="1107" height="748" alt="image" src="https://github.com/user-attachments/assets/4ac111e4-0bbc-4ae5-8ab2-5efc46daafc2" />


**Azure Resource Group — Full infrastructure overview**
<img width="823" height="585" alt="image" src="https://github.com/user-attachments/assets/a197c397-213b-4e77-a59f-ee9c5a8a8442" />

---

## 👤 Author

**Junaid Khan** — Data Engineer  
📍 Hyderabad, India  
🔗 [linkedin.com/in/junaid-khan07](https://linkedin.com/in/junaid-khan07)  
🐙 [github.com/iamjunaidatgit](https://github.com/iamjunaidatgit)
