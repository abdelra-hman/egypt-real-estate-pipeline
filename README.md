# Egypt Real Estate Data Pipeline (Property Finder)

An end-to-end data engineering pipeline that extracts real estate listings (Buy & Rent) across 17 Egyptian governorates from Property Finder, ingests raw snapshots into **Databricks Delta Lake** using **Auto Loader**, and processes them into a **Silver** layer with **SCD Type 2** historical tracking.

---

## Architecture

```text
[Property Finder (__NEXT_DATA__)]
        │
        ▼  (Python: curl_cffi + BeautifulSoup)
[Landing Zone (Atomic CSVs)]
        │
        ▼  (Databricks Auto Loader: cloudFiles)
[Bronze Layer: real_estate.bronze.raw_listings]
        │
        ▼  (PySpark + Delta Lake: SCD Type 2 Merge)
[Silver Layer: real_estate.silver.cleaned_listings]
```

---

## Key Engineering Highlights

### 1. Robust Web Scraper (`scraper/propertyfinder_scraper.py`)
- **Full Coverage:** Extracts all property types (`properties-for-sale` & `properties-for-rent`) across all available pagination pages and 17 governorates.
- **Anti-Bot & Resilience:** Uses `curl_cffi` (`chrome120` TLS impersonation), randomized polite delays (`3.0s` base), exponential backoff on `429`/`5xx`, and `403` cooldown handling.
- **Data Integrity:** Implements **atomic file writes** (`tempfile` + `os.replace`) to prevent file corruption on interruption, and proper CSV quoting/escaping (`escapechar="\\"`) to eliminate column-shift issues in Spark.
- **Cross-Run Change Tracking:** Compares incoming listings against the previous snapshot to log `price_egp` and `area_value` changes in `propertyfinder_price_history.csv`.

### 2. Bronze Layer (`databricks/01_bronze_autoloader.py`)
- **Incremental Ingestion:** Uses Databricks **Auto Loader** (`cloudFiles`) with `trigger(availableNow=True)` for cost-efficient batch streaming.
- **Schema & Parsing Alignment:** Configured with `multiLine="true"`, `quote='"'`, `escape="\\"`, and `cloudFiles.rescuedDataColumn` (`_rescued_data`) to safely capture malformed records without pipeline failure.
- **Lineage Metadata:** Enriches every record with `ingestion_timestamp` and `source_file` (`_metadata.file_path`).

### 3. Silver Layer (`databricks/02_silver_scd2_merge.py`)
- **Deduplication:** Ranks records per `listing_id` using `Window` functions ordered by `ingestion_timestamp` and `scraped_at`.
- **Data Cleaning & Outlier Filtering:**
  - Cleans numeric fields (`price_egp`, `bedrooms`, `bathrooms`, `area_value`) via regex and `try_cast`.
  - Filters out invalid prices (`<= 0`) and anomalous areas (`< 30` sqm) to prevent `price_per_sqm` distortion.
  - Remediates legacy column-shifted rows (`is_shifted` detection via `scraped_at` timestamp validation).
- **SCD Type 2 Merge:** Tracks historical changes in `price_egp` and `area_value` by closing old records (`is_current = false`, `effective_to = ingestion_timestamp`) and inserting updated versions (`is_current = true`).

---

## Project Structure

```text
egypt-real-estate-pipeline/
├── scraper/
│   └── propertyfinder_scraper.py    # Python scraper with atomic writes & history tracking
├── databricks/
│   ├── 01_bronze_autoloader.py      # Auto Loader ingestion to Bronze Delta table
│   └── 02_silver_scd2_merge.py      # Cleaning, deduplication & SCD Type 2 Merge
├── requirements.txt                 # Python dependencies for the scraper
├── .gitignore                       # Excludes raw CSVs, checkpoints, and cache
└── README.md
```

---

## Tech Stack
- **Extraction:** Python, `curl_cffi`, `BeautifulSoup4`
- **Storage & Lakehouse:** Databricks, Delta Lake, Unity Catalog Volumes
- **Processing:** PySpark, Databricks Auto Loader (`cloudFiles`), Spark SQL

---

## Setup & Usage

1. **Install Scraper Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Run the Scraper:**
   Configure `landing_dir` and `state_dir` in `scraper/propertyfinder_scraper.py`, then run:
   ```bash
   python scraper/propertyfinder_scraper.py
   ```
3. **Run Databricks Pipeline:**
   Schedule `01_bronze_autoloader.py` followed by `02_silver_scd2_merge.py` sequentially in a Databricks Workflow Job.






```mermaid
flowchart LR
    %% Styling
    classDef source fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc
    classDef bronze fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#fef3c7
    classDef silver fill:#1e293b,stroke:#94a3b8,stroke-width:2px,color:#f1f5f9
    classDef gold fill:#422006,stroke:#eab308,stroke-width:2px,color:#fef08a

    subgraph S ["🌐 1. Extraction Layer"]
        direction TB
        PF["Property Finder Egypt<br/>(__NEXT_DATA__)"]:::source
        SC["Python Scraper<br/>(curl_cffi + BeautifulSoup)"]:::source
        CSV["Atomic CSV Snapshots<br/>(escapechar='\\')"]:::source
        PF --> SC --> CSV
    end

    subgraph B ["🥉 2. Bronze Layer (Raw)"]
        direction TB
        VOL["Unity Catalog Volume<br/>(/landing/)"]:::bronze
        AL["Databricks Auto Loader<br/>(cloudFiles + availableNow)"]:::bronze
        BT[("real_estate.bronze.raw_listings<br/>(Delta Table + _rescued_data)")]:::bronze
        VOL --> AL --> BT
    end

    subgraph V ["🥈 3. Silver Layer (Cleaned)"]
        direction TB
        DD["Deduplication & Cleaning<br/>(Window Rank + try_cast)"]:::silver
        FLT["Outlier & Shift Filtering<br/>(area >= 30, is_shifted)"]:::silver
        SCD["SCD Type 2 Merge<br/>(is_current, effective_from/to)"]:::silver
        ST[("real_estate.silver.cleaned_listings<br/>(Delta Table)")]:::silver
        DD --> FLT --> SCD --> ST
    end

    subgraph G ["🥇 4. Gold Layer (Analytics - Next)"]
        direction TB
        BI["Power BI / Dashboards<br/>(Price Trends & Market KPIs)"]:::gold
    end

    CSV --> VOL
    BT --> DD
    ST -.-> BI