# Stock Analytics Pipeline

End-to-end data pipeline for automated stock market technical analysis using Airflow, dbt, Snowflake, and Preset.

## Overview

Automated system that:
- Fetches daily stock prices (AAPL, NVDA, TSLA, MSFT) from yfinance
- Calculates technical indicators (MA, RSI, MACD, Bollinger Bands)
- Provides interactive dashboards for trading analysis


## Architecture
```
yfinance API → Airflow (ETL) → Snowflake → dbt (ELT) → Preset Dashboards
```

**Data Flow**:
1. **ETL**: Python extracts 180 days of stock data, loads to Snowflake
2. **ELT**: dbt transforms data through 3 layers (staging → intermediate → marts)
3. **Visualization**: Preset displays technical indicators

## Tech Stack

- **Orchestration**: Apache Airflow 2.x
- **Warehouse**: Snowflake
- **Transformation**: dbt Core
- **Visualization**: Preset
- **Container**: Docker

## Project Structure
```
├── dags/
│   └── stock_etl_dbt.py              # Airflow DAG
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_stock_daily.sql
│   │   ├── intermediate/
│   │   │   ├── int_moving_averages.sql
│   │   │   ├── int_rsi.sql
│   │   │   ├── int_macd.sql
│   │   │   ├── int_bollinger.sql
│   │   │   └── int_daily_returns.sql
│   │   └── marts/
│   │       └── mart_stock_daily_metrics.sql
│   ├── snapshots/
│   │   └── stock_price_snapshot.sql
│   ├── dbt_project.yml
│   └── profiles.yml
└── docker-compose.yml
```

## Quick Start

### Prerequisites
- Docker Desktop
- Snowflake account

### Setup

1. **Clone repository**
```bash
git clone https://github.com/Ashley96yy/DATA226_LAB2.git
cd stock-analytics-pipeline
```

2. **Start Airflow**
```bash
docker-compose up -d
```

3. **Configure Airflow** (http://localhost:8080)
   
   **Connection** (Admin → Connections):
   - Connection ID: `snowflake_default`
   - Type: Snowflake
   - Host: `your-account.snowflakecomputing.com`
   - Database: `XX_DB`
   - Schema: `STOCK`
   - Warehouse: `BEETLE_QUERY_WH`
   
   **Variable** (Admin → Variables):
   - Key: `yfinance_symbols`
   - Value: `["AAPL", "NVDA", "TSLA", "MSFT]`

4. **Run Pipeline**
   - Enable `stock_etl_dbt` DAG
   - Trigger manually or wait for daily schedule

## Technical Indicators

| Indicator | Description | Window |
|-----------|-------------|--------|
| MA 5/20/50 | Moving Averages | 5, 20, 50 days |
| RSI-14 | Relative Strength Index | 14 days |
| MACD | Momentum Indicator | 12, 26, 9 days |
| Bollinger Bands | Volatility Bands | 20 days ± 2σ |

## Data Layers

- **Raw**: `fact_stock_price_daily` (yfinance data)
- **Staging**: `stg_stock_daily` (cleaned data)
- **Intermediate**: 5 models (technical indicators)
- **Marts**: `mart_stock_daily_metrics` (consolidated)

## Testing
```bash
cd dbt
dbt test --profiles-dir .
```

Tests validate:
- Uniqueness (primary keys)
- Not null (critical fields)
- Value ranges (RSI 0-100, prices > 0)
- Referential integrity

## Dashboard

Preset visualizations include:
- Price with moving averages
- RSI indicator with overbought/oversold zones
- MACD analysis
- Bollinger Bands
- Trading signals summary table


