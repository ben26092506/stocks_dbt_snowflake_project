````markdown
# Real-Time Stock Data Pipeline with Kafka, MinIO, Snowflake, dbt and Airflow

This project demonstrates an end-to-end Modern Data Stack pipeline for live stock market data.

The pipeline fetches live stock quotes from the Finnhub API, streams them through Apache Kafka, stores raw JSON events in MinIO, loads the data into Snowflake with Apache Airflow, transforms it with dbt, and creates analytics-ready Gold views for dashboards and reporting.

## Tech Stack

- **Python** – API integration, Kafka producer and Kafka consumer
- **Apache Kafka** – real-time event streaming
- **Kafdrop** – Kafka topic inspection UI
- **MinIO** – local S3-compatible object storage
- **Apache Airflow** – workflow orchestration
- **PostgreSQL** – Airflow metadata database
- **Snowflake** – cloud data warehouse
- **dbt** – SQL-based transformations and data modeling
- **Docker Compose** – local infrastructure setup

## Architecture

```text
Finnhub API
    ↓
Python Kafka Producer
    ↓
Kafka topic: stock-quotes
    ↓
Python Kafka Consumer
    ↓
MinIO bucket: bronze-transactions
    ↓
Airflow DAG
    ↓
Snowflake Bronze table
    ↓
dbt Bronze / Silver / Gold models
    ↓
Analytics-ready views
```

## Project Structure

```text
snowflake_project/
├── infra/
│   ├── docker-compose.yml
│   ├── producer/
│   │   └── producer.py
│   ├── consumer/
│   │   └── consumer.py
│   ├── dags/
│   │   ├── airflow.py
│   │   └── airflow_decorator.py
│   ├── logs/
│   └── plugins/
├── dbt_stocks/
│   └── dbt_stocks/
│       ├── analyses/
│       ├── macros/
│       ├── models/
│       │   ├── bronze/
│       │   │   ├── bronze_stg_stock_quotes.sql
│       │   │   └── sources.yml
│       │   ├── silver/
│       │   │   └── silver_clean_stock_quotes.sql
│       │   └── gold/
│       │       ├── gold_candle_chart.sql
│       │       ├── gold_kpi.sql
│       │       └── gold_tree_chart.sql
│       ├── seeds/
│       ├── snapshots/
│       ├── tests/
│       └── dbt_project.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Pipeline Overview

This project implements a real-time stock market data pipeline.

The producer fetches quote data for selected stock symbols from the Finnhub API. The data is sent as JSON messages to a Kafka topic. A Kafka consumer reads the messages and stores them as JSON files in MinIO. Airflow then loads the raw JSON files from MinIO into a Snowflake Bronze table. dbt transforms the raw data into Silver and Gold views.

## Data Flow

### 1. Finnhub API

The project uses the Finnhub quote API to fetch live stock market data.

Example stock symbols:

```text
AAPL, MSFT, TSLA, GOOGL, AMZN
```

The API returns quote fields such as:

```text
c   = current price
d   = change amount
dp  = change percentage
h   = day high
l   = day low
o   = day open
pc  = previous close
t   = market timestamp
```

The producer enriches each record with:

```text
symbol
fetched_at
```

### 2. Kafka Producer

The Python producer is located in:

```text
infra/producer/producer.py
```

It performs the following steps:

1. Fetch stock quote data from the Finnhub API.
2. Convert the API response into a Python dictionary.
3. Add metadata fields such as `symbol` and `fetched_at`.
4. Serialize the dictionary into JSON bytes.
5. Send the message to the Kafka topic `stock-quotes`.

Before running the project, replace the placeholder API key in `producer.py`:

```python
API_KEY = 'INSERT YOUR API KEY HERE'
```

### 3. Kafka Topic

The Kafka topic used in this project is:

```text
stock-quotes
```

The topic can be created and inspected through Kafdrop.

Kafdrop UI:

```text
http://localhost:9000
```

### 4. Kafka Consumer

The Python consumer is located in:

```text
infra/consumer/consumer.py
```

It performs the following steps:

1. Subscribe to the Kafka topic `stock-quotes`.
2. Read JSON messages from Kafka.
3. Deserialize the Kafka message from bytes into a Python dictionary.
4. Store each record as a JSON file in MinIO.

Example MinIO object path:

```text
AAPL/1779442277.json
```

### 5. MinIO

MinIO is used as a local S3-compatible object store.

It acts as the raw landing zone of the pipeline and simulates cloud object storage such as AWS S3.

MinIO Console:

```text
http://localhost:9001
```

MinIO S3 API:

```text
http://localhost:9002
```

The bucket used in this project is:

```text
bronze-transactions
```

### 6. Airflow

Airflow orchestrates the data loading process from MinIO into Snowflake.

The main DAG is located in:

```text
infra/dags/airflow_decorator.py
```

The DAG is named:

```text
minio_to_snowflake_decorators
```

It performs the following steps:

1. Download JSON files from MinIO.
2. Upload the files to a Snowflake internal table stage using `PUT`.
3. Load the staged files into the Snowflake Bronze table using `COPY INTO`.

Airflow UI:

```text
http://localhost:8081
```

Before running the project, replace the Snowflake credential placeholders API key in `airflow.py`:

```python
SNOWFLAKE_USER = 'INSERT YOUR USER NAME HERE'
SNOWFLAKE_PASSWORD = 'INSERT YOUR PASSWORD HERE'
SNOWFLAKE_ACCOUNT = 'INSERT YOUR ACCOUNT NAME HERE'
SNOWFLAKE_WAREHOUSE = 'INSERT YOUR WAREHOUSE HERE'
SNOWFLAKE_DB = 'INSERT YOUR DATABASE HERE'
SNOWFLAKE_SCHEMA = 'INSERT YOUR SCHEMA HERE'
```

### 7. Snowflake

Snowflake is used as the cloud data warehouse.

The raw JSON files are loaded into a Bronze table and then transformed using dbt.

### 8. dbt

dbt is used for SQL transformations.

The dbt project is located in:

```text
dbt_stocks/dbt_stocks/
```

The project follows a Bronze / Silver / Gold modeling approach.

## dbt Models

### Bronze Layer

Model:

```text
bronze_stg_stock_quotes
```

File:

```text
dbt_stocks/dbt_stocks/models/bronze/bronze_stg_stock_quotes.sql
```

Purpose:

- Reads raw JSON data from the Snowflake source table.
- Extracts JSON fields.
- Casts values into SQL columns.
- Converts timestamps.
- Prepares the data for the Silver layer.

### Silver Layer

Model:

```text
silver_clean_stock_quotes
```

File:

```text
dbt_stocks/dbt_stocks/models/silver/silver_clean_stock_quotes.sql
```

Purpose:

- Cleans and standardizes stock quote data.
- Rounds numeric values.
- Removes invalid records.
- Provides a clean base table for Gold models.

### Gold Layer

The Gold layer contains analytics-ready views.

#### gold_kpi

File:

```text
dbt_stocks/dbt_stocks/models/gold/gold_kpi.sql
```

Purpose:

- Returns the latest stock quote per symbol.
- Uses `ROW_NUMBER()` to select the newest record based on `fetched_at`.

Columns:

```text
symbol
current_price
change_amount
change_percent
```

#### gold_candle_chart

File:

```text
dbt_stocks/dbt_stocks/models/gold/gold_candle_chart.sql
```

Purpose:

- Builds candlestick chart data per symbol and trading day.
- Calculates open, close, high, low and trend line values.

Columns:

```text
symbol
candle_time
candle_low
candle_high
candle_open
candle_close
trend_line
```

#### gold_tree_chart

File:

```text
dbt_stocks/dbt_stocks/models/gold/gold_tree_chart.sql
```

Purpose:

- Calculates average price and volatility metrics per symbol.
- Uses the latest available trading day per symbol.
- Calculates all-time volatility with `STDDEV_POP`.

Columns:

```text
symbol
avg_price
volatility
relative_volatility
```

## Local Setup

### 1. Clone the repository

```bash
git clone https://github.com/ben26092506/stocks_dbt_snowflake_project.git
cd stocks_dbt_snowflake_project
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Add your Finnhub API key

Open:

```text
infra/producer/producer.py
```

### 5. Start Docker services

Go to the infrastructure folder:

```bash
cd infra
```

Start the services:

```bash
docker compose up -d
```

This starts:

```text
Zookeeper
Kafka
Kafdrop
MinIO
Airflow Webserver
Airflow Scheduler
PostgreSQL
```

### 6. Initialize Airflow database

```bash
docker compose exec airflow-webserver airflow db migrate
```

### 7. Create Airflow user

```bash
docker compose exec airflow-webserver airflow users create \
  --username 'INSERT YOUR USERNAME' \
  --firstname 'INSERT YOUR FIRSTNAME' \
  --lastname 'INSERT YOUR LASTNAME' \
  --role Admin \
  --email example@example.com \
  --password password123
```

### 8. Create Kafka topic

Open Kafdrop:

```text
http://localhost:9000
```

Create the topic:

```text
stock-quotes
```

### 9. Create MinIO bucket

Open MinIO:

```text
http://localhost:9001
```

Create the bucket:

```text
bronze-transactions
```

### 10. Run the producer

From the project root:

```bash
python infra/producer/producer.py
```

The producer fetches live stock quote data and sends it to Kafka.

### 11. Run the consumer

From the project root:

```bash
python infra/consumer/consumer.py
```

The consumer reads stock quote messages from Kafka and stores them in MinIO.

### 12. Trigger the Airflow DAG

Open Airflow:

```text
http://localhost:8081
```

Trigger the DAG:

```text
minio_to_snowflake_decorators
```

The DAG loads files from MinIO into Snowflake.

### 13. Run dbt

Go to the dbt project folder:

```bash
cd dbt_stocks/dbt_stocks
```

Run all dbt models:

```bash
dbt run
```

Run a specific model:

```bash
dbt run --select gold_kpi
```

## Docker Services

### Kafka

Kafka is used as the real-time streaming layer.

Internal Docker endpoint:

```text
kafka:9092
```

Host endpoint:

```text
host.docker.internal:29092
```

### Kafdrop

Kafdrop is used to inspect Kafka topics and messages.

URL:

```text
http://localhost:9000
```

### MinIO

MinIO is used as local S3-compatible storage.

Console:

```text
http://localhost:9001
```

S3 API:

```text
http://localhost:9002
```

### Airflow

Airflow is used to orchestrate the MinIO-to-Snowflake load process.

URL:

```text
http://localhost:8081
```

### PostgreSQL

PostgreSQL is used as the Airflow metadata database.

## Useful Commands

Start infrastructure:

```bash
cd infra
docker compose up -d
```

Stop infrastructure:

```bash
cd infra
docker compose down
```

Check running containers:

```bash
docker compose ps
```

Run dbt:

```bash
cd dbt_stocks/dbt_stocks
dbt run
```

Generate and serve dbt docs:

```bash
dbt docs generate
dbt docs serve
```

## Current Status

The project currently includes:

- Local Kafka setup with Docker Compose
- Kafdrop UI for Kafka inspection
- Python producer for Finnhub stock quote data
- Python consumer for writing Kafka events to MinIO
- MinIO bucket as raw object storage
- Airflow DAG for loading MinIO files into Snowflake
- Snowflake Bronze table
- dbt Bronze, Silver and Gold models
- Gold views for KPI, candlestick chart and tree chart analytics

## Future Improvements

Possible future improvements:

- Move credentials into environment variables or Airflow Connections
- Add dbt tests for data quality
- Add dbt source freshness checks
- Add Power BI dashboard connected to Snowflake Gold views
- Add incremental dbt models
- Add CI/CD pipeline for dbt validation
- Add cleanup logic for already loaded MinIO files
- Containerize producer and consumer scripts
````
