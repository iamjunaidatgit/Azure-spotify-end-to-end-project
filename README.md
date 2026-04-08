# 🎵 Spotify Incremental Data Pipeline — Azure Data Factory

A production-style incremental ingestion pipeline built on **Azure Data Factory**, loading data from **Azure SQL Database** into **ADLS Gen2 (Bronze layer)** using Change Data Capture (CDC) logic, dynamic datasets, and ForEach orchestration.

---

## 🏗️ Architecture

```
Azure SQL Database (Source)
        │
        ▼
┌──────────────────────────────────────────────┐
│           Azure Data Factory                 │
│                                              │
│  incremental_loop (Parent Pipeline)          │
│  ┌──────────────────────────────────────┐   │
│  │  ForEach (5 tables)                  │   │
│  │  ┌────────────────────────────────┐  │   │
│  │  │  last_cdc  ──┐                 │  │   │
│  │  │  current   ──┴──► Copy Data    │  │   │
│  │  │                   (AzureSQLtoLake)│  │
│  │  │                        │        │  │   │
│  │  │               If Incremental?   │  │   │
│  │  │              /                \ │  │   │
│  │  │           True              False│  │   │
│  │  │     max_cdc → update_cdc  DeleteEmptyFile│
│  │  └────────────────────────────────┘  │   │
│  └──────────────────────────────────────┘   │
│           │                                  │
│        Alerts (Logic App — Pass/Fail)        │
└──────────────────────────────────────────────┘
        │
        ▼
ADLS Gen2 — Bronze Layer
  /bronze/{table_name}/{table_name}_{timestamp}.parquet
```

---

## 📦 Tech Stack

| Component | Tool |
|---|---|
| Orchestration | Azure Data Factory |
| Source | Azure SQL Database |
| Sink | ADLS Gen2 (Bronze) |
| File Format | Parquet (Snappy compressed) |
| CDC Strategy | Watermark-based (last updated timestamp) |
| Alerting | Azure Logic App (HTTP webhook) |
| Infrastructure | Azure Free Tier |

---

## 📂 Tables Ingested

| Schema | Table | CDC Column |
|---|---|---|
| dbo | DimUser | updated_at |
| dbo | DimTrack | updated_at |
| dbo | DimDate | date |
| dbo | DimArtist | updated_at |
| dbo | FactStream | stream_timestamp |

---

## ⚙️ Pipeline Design

### `incremental_loop` (Parent Pipeline)
Accepts an array of table configs and loops through each using a **ForEach** activity.

**Inside each loop iteration:**

1. **`last_cdc` (Lookup)** — Reads the last watermark timestamp from a `cdc.json` file stored in ADLS for that table.
2. **`current` (Set Variable)** — Captures `utcNow()` as the current run timestamp for file naming.
3. **`AzureSQLtoLake` (Copy Data)** — Runs a dynamic SQL query filtering rows where `cdc_col > last_cdc`. Writes to bronze as a timestamped Parquet file.
4. **`Ifincrementaldata` (If Condition)** — Checks if any data was actually copied (`dataRead > 0`).
   - **True** → `max_cdc` (Script) fetches the new max CDC value → `update_last_cdc` (Copy) overwrites `cdc.json`.
   - **False** → `DeleteEmptyFile` (Delete) removes the empty Parquet file.
5. **`Alerts` (Web Activity)** — Posts pipeline status (name + run ID) to an Azure Logic App regardless of success or failure.

### `incremental_ingestion` (Child Pipeline)
Same logic as above but designed to run for a **single table** with parameters passed directly. Used for targeted reruns or backfills.

---

## 🗂️ Dynamic Datasets

### `parquet_dynamic`
Parameterized Parquet dataset pointing to ADLS Gen2.

| Parameter | Description |
|---|---|
| `container` | ADLS container name (e.g., `bronze`) |
| `folder` | Table name used as folder path |
| `file` | Filename = `{table}_{timestamp}.parquet` |

Compression: **Snappy**

### `azure_sql`
Linked dataset pointing to Azure SQL with no hard-coded table — table is injected dynamically via the SQL query in the Copy activity.

---

## 🔗 Linked Services

| Name | Type | Target |
|---|---|---|
| `datalake` | AzureBlobFS | `junaidazureproject.dfs.core.windows.net` |
| `azure_sql` | AzureSqlDatabase | `azureprojectserverjun.database.windows.net` / `azureprojectdbjun` |

Authentication: SQL auth with encrypted credentials stored in ADF.

---

## 🚀 How to Run

### Full Load (All Tables)
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

Setting `from_date` overrides the watermark from `cdc.json` and pulls all data from that date.

---

## 📁 Project Structure (GitHub)

```
spotify-adf-pipeline/
│
├── pipelines/
│   ├── incremental_loop.json
│   └── incremental_ingestion.json
│
├── datasets/
│   ├── parquet_dynamic.json
│   └── azure_sql.json
│
├── linkedServices/
│   ├── datalake.json
│   └── azure_sql.json
│
├── loop_input.txt          # Table config array for ForEach
└── README.md
```

---

## 🧠 Key Design Decisions

- **Watermark-based CDC** instead of full load — only new/updated rows are pulled each run, keeping costs and run time low.
- **Dynamic datasets** — one Parquet dataset handles all tables via parameters, no dataset duplication.
- **Empty file cleanup** — if no new data exists for a table, the empty Parquet file is deleted to keep storage clean.
- **Timestamp in filename** — each run produces a uniquely named file (`TableName_2024-04-08T10:00:00Z.parquet`), enabling easy audit and reprocessing.
- **Alert on both success and failure** — Logic App webhook fires regardless of outcome for full observability.

---

## 👤 Author

**Junaid** — Senior Systems Engineer transitioning to Data Engineering  
📍 Hyderabad, India  
🐙 [github.com/iamjunaidatgit](https://github.com/iamjunaidatgit)
