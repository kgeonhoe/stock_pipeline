# Korean Stock Data Pipeline

2024 Data Engineering Project - Automated Korean Stock Market Data Collection Pipeline

## 🎯 Project Overview

This pipeline automatically **downloads all Korean stock lists** (based on current date) and **collects daily stock data** for comprehensive market analysis. The system uses Apache Airflow for orchestration and is designed for production-grade data collection.

### Key Features

✅ **Complete Stock Universe**: Downloads all KOSPI and KOSDAQ stocks daily  
✅ **Automated Data Collection**: Runs daily at 15:31 KST (after market close)  
✅ **Incremental Loading**: Only collects new/missing data for efficiency  
✅ **Corporate Actions**: Handles stock splits, mergers, and price adjustments  
✅ **Production Ready**: Docker-based deployment with monitoring  

## 🚀 Quick Start

### 1. Prerequisites
- Docker & Docker Compose
- KIS (Korean Investment & Securities) API credentials
- Basic understanding of Korean stock market

### 2. Setup
```bash
# Clone repository
git clone <repository-url>
cd stock_pipeline

# Configure API credentials in jobs/cf.py
cp jobs/cf.py.example jobs/cf.py
# Edit cf.py with your KIS API credentials

# Start the pipeline
docker-compose up -d
```

### 3. Access Services
- **Airflow UI**: http://localhost:8081 (admin/admin)
- **Spark UI**: http://localhost:9090
- **Pipeline runs daily at 15:31 KST automatically**

## 📊 What This Pipeline Does

### Daily Process (Automated)
1. **📅 Current Date Check**: Downloads today's complete stock list
2. **🔍 Stock Discovery**: Identifies all KOSPI & KOSDAQ listed stocks
3. **📈 Data Collection**: Fetches historical data for each stock
4. **💾 Storage**: Stores in MySQL with proper indexing
5. **🧹 Maintenance**: Handles data quality and corporate actions

### Data Sources
- **KIS API**: Korean Investment & Securities official API
- **Coverage**: All KOSPI and KOSDAQ stocks
- **Frequency**: Daily updates
- **History**: Configurable lookback period

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   KIS API       │    │   Apache        │    │   MySQL         │
│   (Stock Data)  │────│   Airflow       │────│   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                       ┌─────────────────┐
                       │   Apache Spark  │
                       │   (Processing)  │
                       └─────────────────┘
```

### Components
- **Collector**: Downloads stock lists and price data
- **Stock Filter**: Processes and validates data
- **Airflow DAG**: Orchestrates the daily workflow
- **Docker**: Containerized deployment

## 📁 Project Structure

```
stock_pipeline/
├── dags/                    # Airflow DAGs
│   └── download-stock-data.py  # Main scheduling DAG
├── jobs/                    # Core pipeline code
│   ├── collector.py         # Stock data collection
│   ├── stock_filter.py      # Data processing
│   ├── stock_crawl_main.py  # Main pipeline logic
│   └── cf.py               # Configuration (API keys)
├── examples/               # Usage examples
│   └── stock_pipeline_demo.py
├── docker-compose.yml      # Service orchestration
├── requirements.txt        # Python dependencies
├── SETUP_GUIDE.md         # Detailed setup instructions
└── README.md              # This file
```

## 🎮 Usage Examples

### Run Demo
```bash
# See how the pipeline works
python examples/stock_pipeline_demo.py
```

### Manual Collection
```bash
# Run stock collection manually
docker exec -it <container> python /opt/airflow/jobs/stock_crawl_main.py
```

### Check Data
```bash
# View collected data
docker exec -it <mysql-container> mysql -u airflow -p airflow_daily_craw
```

## 📋 Data Schema

### Stock List (`all_stock_list`)
- `stockCode`: 6-digit stock code
- `stockName`: Company name
- `priceChange`: Corporate action indicator
- `listeddate`: IPO date
- `etldate`: Collection date
- `etlcheck`: Processing status

### Stock Data (`all_stock`)
- `stockdate`: Trading date
- `stockcode`: Stock identifier
- `stockclose/open/high/low`: Price data
- `stockvolume`: Trading volume
- `stockpricevolume`: Trading value

## 🔧 Configuration

### API Setup
Edit `jobs/cf.py`:
```python
appkey = "YOUR_36_CHAR_KIS_API_KEY"
secretkey = "YOUR_KIS_SECRET_KEY"
account_id = "YOUR_ACCOUNT_ID"
```

### Schedule Modification
Edit `dags/download-stock-data.py`:
```python
schedule_interval="31 15 * * *"  # Daily at 15:31 KST
```

## 🔍 Monitoring

### Airflow Dashboard
- View DAG runs and task status
- Check logs for debugging
- Monitor execution times

### Data Quality
- Automatic duplicate prevention
- Corporate action handling
- Missing data detection

## 🤝 Problem Statement Implementation

This pipeline directly addresses the requirements:

1. **✅ 모든 종목 리스트 다운로드 (현재 날짜 기준)**
   - Implemented in `collector.kis_get_all_stock()`
   - Updates daily with current market data
   - Handles new listings and delistings

2. **✅ 해당 종목 기준 데이터 수집**
   - Collects OHLCV data for all stocks
   - Incremental loading for efficiency
   - Historical backfill capability

3. **✅ Airflow로 일일 데이터 수집 및 적재**
   - Automated daily execution at 15:31 KST
   - Complete ETL pipeline with error handling
   - Production-ready scheduling and monitoring

## 🚀 Production Deployment

### Security
- Store credentials in environment variables
- Use secret management systems
- Enable SSL/TLS for API connections

### Scaling
- Distributed Spark processing
- Database partitioning
- Load balancing

### Monitoring
- Airflow alerts for failures
- Database monitoring
- API rate limit tracking

## 📚 Documentation

- **[SETUP_GUIDE.md](SETUP_GUIDE.md)**: Complete setup instructions
- **[examples/](examples/)**: Code examples and demonstrations
- **Inline comments**: Detailed code documentation

## 🛠️ Technology Stack

- **Python 3.11**: Core language
- **Apache Airflow 2.7.1**: Workflow orchestration
- **Apache Spark**: Data processing
- **MySQL**: Data storage
- **Docker**: Containerization
- **KIS API**: Stock data source

## 📈 Use Cases

- **Quantitative Trading**: Historical data for backtesting
- **Market Analysis**: Complete market coverage
- **Risk Management**: Real-time portfolio monitoring
- **Research**: Academic and professional analysis
- **Machine Learning**: Training data for models

---

## Original Setup Instructions

### Directory Structure

```
| data
  |- your data goes here
| jobs
  |- your pyspark .py files go here
| notebooks
  |- jupyter notebooks for practice go here
| resources
  |- .jars for spark third-party app go here
docker-compose.yml
```

### Docker Setup
(download & use wsl if you use Windows OS)
https://docs.docker.com/engine/install/ubuntu/

### How to run pyspark project

run containers:

``` bash
$ docker-compose up -d

 ✔ Network de-2024_default-network    Created
 ✔ Container de-2024-spark-master-1   Started
 ✔ Container de-2024-jupyter-spark-1  Started
 ✔ Container de-2024-spark-worker-1   Started
```

spark-master UI: localhost:9090

spark-submit:

``` bash
$ docker exec -it de-2024-spark-master-1 spark-submit --master spark://spark-master:7077 jobs/hello-world.py data/<filename>
```

### Jupyter Notebook

test codes in jupyter notebook environment - localhost:8888

---

**Built for Korean Stock Market Data Collection**  
*Automated • Scalable • Production-Ready*
