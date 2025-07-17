# Stock Data Pipeline Setup Guide

This guide explains how to set up and use the stock data pipeline for collecting Korean stock market data.

## Overview

The stock data pipeline automatically:
1. **Downloads all stock lists** from KIS (Korean Investment & Securities) API based on current date
2. **Collects daily stock data** for all listed stocks
3. **Stores data in MySQL database** with proper data management
4. **Runs daily through Apache Airflow** for automated data collection

## Prerequisites

### 1. Korean Investment & Securities (KIS) API Access
- Register for a KIS API account at [KIS Developer Portal](https://openapi.koreainvestment.com)
- Obtain your API credentials:
  - App Key (36 characters)
  - App Secret Key
  - Account ID

### 2. Required Infrastructure
- Docker & Docker Compose
- MySQL Database
- Apache Airflow (configured via Docker)

## Setup Instructions

### 1. Clone and Configure

```bash
git clone <repository-url>
cd stock_pipeline
```

### 2. Configure API Credentials

Edit `jobs/cf.py` and replace the placeholder values:

```python
# KIS API Configuration
appkey = "YOUR_36_CHARACTER_KIS_API_KEY"
secretkey = "YOUR_KIS_API_SECRET_KEY"
account_id = "YOUR_KIS_ACCOUNT_ID"

# Database Configuration
host = "localhost"
username = "airflow"
password = "airflow"
port = 3306
database = "airflow_daily_craw"
```

### 3. Start the Pipeline

```bash
# Start all services
docker-compose up -d

# Check services are running
docker-compose ps
```

### 4. Access Services

- **Airflow Web UI**: http://localhost:8081
  - Username: `airflow`
  - Password: `airflow`
- **Spark Master UI**: http://localhost:9090

## Pipeline Components

### 1. Data Collection (Collector)
- **File**: `jobs/collector.py`
- **Function**: `kis_get_all_stock()` - Downloads complete stock list
- **Function**: `kis_get_values()` - Fetches historical stock data
- **Updates**: Stock list is refreshed daily based on current date

### 2. Data Processing (Stock Filter)
- **File**: `jobs/stock_filter.py`
- **Function**: Processes and filters stock data
- **Database**: Manages MySQL storage and data integrity
- **Logic**: Handles stock splits and corporate actions

### 3. Main Pipeline (Stock Crawl)
- **File**: `jobs/stock_crawl_main.py`
- **Schedule**: Daily at 15:31 KST (after market close)
- **Process**:
  1. Download current stock list
  2. Identify new/changed stocks
  3. Collect missing historical data
  4. Store in database

### 4. Airflow DAG
- **File**: `dags/download-stock-data.py`
- **Schedule**: `"31 15 * * *"` (Daily at 15:31 KST)
- **Tasks**:
  1. `data_collect` - Start notification
  2. `delete_data` - Clean up old data
  3. `crawl_main` - Run main collection
  4. `filter_data` - Process with Spark
  5. `end` - Completion notification

## Database Schema

### Stock List Table (`all_stock_list`)
```sql
CREATE TABLE all_stock_list (
    stockCode VARCHAR(8) NOT NULL,
    stockName VARCHAR(50) NOT NULL,
    priceChange TEXT NOT NULL,
    listeddate TEXT NOT NULL,
    etldate TEXT NOT NULL,
    etlcheck TEXT NOT NULL
);
```

### Stock Data Table (`all_stock`)
```sql
CREATE TABLE all_stock (
    stockdate VARCHAR(8) NOT NULL,
    stockcode VARCHAR(6) NOT NULL,
    stockclose FLOAT NOT NULL,
    stockopen FLOAT NOT NULL,
    stockhigh FLOAT NOT NULL,
    stocklow FLOAT NOT NULL,
    stockvolume FLOAT NOT NULL,
    stockpricevolume FLOAT NOT NULL
);
```

## Key Features

### 1. Automated Stock List Updates
- Downloads complete stock universe daily
- Detects new listings and delistings
- Handles corporate actions (stock splits, mergers)

### 2. Incremental Data Loading
- Only downloads missing data since last update
- Efficient date range processing (30-day chunks)
- Duplicate prevention with `INSERT IGNORE`

### 3. Data Quality Management
- Handles stock splits and price adjustments
- Validates data integrity
- Maintains ETL audit trail

### 4. Production-Ready Architecture
- Containerized deployment
- Airflow orchestration
- Spark processing
- MySQL storage
- Error handling and logging

## Monitoring and Troubleshooting

### Check Airflow Logs
```bash
# View scheduler logs
docker-compose logs scheduler

# View webserver logs
docker-compose logs webserver
```

### Check Database
```bash
# Connect to MySQL
docker-compose exec postgres psql -U airflow -d airflow
```

### Manual Testing
```bash
# Test stock collection
docker exec -it <container-name> python /opt/airflow/jobs/stock_crawl_main.py
```

## Dependencies

All dependencies are managed through Docker. Key packages include:
- `python-kis`: KIS API client
- `apache-airflow`: Workflow orchestration
- `pyspark`: Big data processing
- `pymysql`: Database connectivity
- `pandas`: Data manipulation

## Customization

### Modify Collection Schedule
Edit `dags/download-stock-data.py`:
```python
schedule_interval="31 15 * * *"  # Change this cron expression
```

### Add New Data Sources
1. Extend `collector.py` with new API methods
2. Update `stock_filter.py` for processing
3. Modify database schema as needed

### Data Retention
Configure in `jobs/delete_all_stock_data.py` for cleanup policies.

## Production Considerations

1. **Security**: Store credentials in environment variables or secret management
2. **Monitoring**: Set up alerting for failed DAG runs
3. **Backup**: Regular database backups
4. **Scaling**: Consider distributed Spark for large datasets
5. **Rate Limiting**: Respect KIS API rate limits