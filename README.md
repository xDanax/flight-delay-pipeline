# ✈️ Flight Delay Data Pipeline (Python + SQLite)

## 🎯Project Overview

- An end-to-end Python data pipeline that automatically ingests, cleans, transforms, and loads U.S. flight delay data into SQLite for analytical use, taken from the [Bureau of Transport Statistics](https://www.bts.gov/).  

- The goal is to simulate a real-world data engineering + analytics workflow, producing clean, query-ready tables that can be used to analyze flight delays by city and day of the week.

- The pipeline is fully automated using Python and loads results in a SQLite database.

## ⚙️ Tech Stack
- Python (pandas, sqlite3, pathlib)

- SQLite

- CSV data ingestion

- Modular ETL pipeline design

## 📊 Dataset

- Raw flight data sourced from a public U.S. flights dataset: [Bureau of Transport Statistics](https://www.bts.gov/)

- Includes flight dates, origin/destination cities, delays, cancellations, diversions, and distances

- This project focuses on January 2025 flight data, so time-based analysis is done using day-of-week trends instead of monthly aggregation

## 🚀 Pipeline Architecture
```
Raw CSV
   ↓
Ingest (ingest.py)
   ↓
Clean & Validate (clean.py)
   ↓
Transform / Aggregate (transform.py)
   ↓
Load to SQLite (load.py)
```

*All steps are orchestrated by pipeline.py.*

## File Structure
```
flight-delay-pipeline/
│
├── ingest.py          # Reads raw CSV and saves timestamped ingested files
├── clean.py           # Cleans data, enforces types, handles missing values
├── transform.py       # Creates analytical aggregation tables
├── load.py            # Loads transformed data into SQLite
├── pipeline.py        # Runs the full end-to-end pipeline
│
├── flight_delays.db   # SQLite database with analytics tables
├── flights_raw.zip    # Raw flight data (compressed)
```

## 🧹 Data Cleaning Logic

**Key cleaning and validation steps include:**

` - Parsing flight dates into proper datetime format`

` - Converting delay and distance columns to numeric values`

` - Standardizing city names`

` - Removing duplicate records`


**Handling missing arrival delays**

` - Cancelled flights are allowed to have missing arrival delays`

` - Non-cancelled flights with missing arrival delays are excluded from analytics`

*This ensures metrics are accurate and not skewed by invalid records.*


## 📈 Transformations & Analytics Tables

### Table 1: City-Level Metrics

- Aggregated by origin city, including:

  `Total number of flights`

  `Average departure delay`

  `Average arrival delay`

  `Cancellation rate`

  `Diversion rate`

  `Average flight distance`

### Table 2: Day-of-Week Metrics

- Aggregated by day of the week, including:

  `Total flights`

  `Average departure delay`

  `Average arrival delay`

  `Cancellation rate`

  `Diversion rate`

*These tables are designed for downstream analysis and visualization.*

## 💻 Database Output

The transformed tables are stored in SQLite as:

  `agg_city_metrics`

  `agg_day_of_week_metrics`

*This allows easy querying using SQL or connection to BI tools.*

### How to Run the Pipeline

1. Ensure Python and required libraries are installed

2. Unzip flights_raw.zip into the data/raw directory

3. Run the pipeline:
```python
pipeline.py
```

This will:

1. Ingest the raw data

2. Clean and validate records

3. Generate analytics tables

4. Load results into flight_delays.db


## 💼 Project Use Case Examples:

- Identify cities with consistently high delays

- Compare weekday vs weekend flight performance

- Support operational decision-making with clean, aggregated data

- Serve as a foundation for dashboards or predictive modeling

### 👤 Author
Danalee Smith

BMath Statistics + CS
