
from __future__ import annotations
from datetime import datetime, timedelta
import json
import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# ============================================================================
# Configuration
# ============================================================================
SNOWFLAKE_CONN_ID = 'snowflake_default'
DATABASE = 'ASH_DB'
SCHEMA_ETL = 'STOCK'
WAREHOUSE = 'BEETLE_QUERY_WH'

DBT_PROJECT_DIR = "/opt/airflow/dbt"

# ============================================================================
# ETL Functions
# ============================================================================

def get_stock_symbols(raw: str | None) -> list[str]:
    if not raw:
        return ["NVDA", "AAPL"]
    try:
        vals = json.loads(raw)
        return [str(v).strip().upper() for v in vals]
    except Exception:
        return [s.strip().upper() for s in raw.strip("[]").replace('"','').split(",") if s.strip()]

def fetch_and_upsert(**context):
    # 1. Determine data range: 180 days back from execution date
    ds = context["ds"]
    execution_date = datetime.strptime(ds, "%Y-%m-%d").date()
    end_date = execution_date + timedelta(days = 1) # inclusive end date
    start_date = execution_date - timedelta(days = 179)
    
    print(f"Date range: {start_date} to {end_date}")
    
    # 2. Get stock symbols from Airflow Variable
    symbols = get_stock_symbols(Variable.get("yfinance_symbols", default_var = None))
    print(f"Stock symbols: {symbols}")

    # 3. Fetch data from yfinance
    frames = []

    for symbol in symbols:
        try:
            print(f"Fetching data for {symbol}...")
            df = yf.download(symbol, start = start_date.isoformat(), end = end_date.isoformat(), progress = False)
            
            if df.empty:
                print(f"No data for symbol: {symbol}")
                continue
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df = df.reset_index()
            
            df = df.rename(columns = {
                "Date": "DATE",
                "Open": "OPEN",
                "Close": "CLOSE", 
                "High": "MAX",
                "Low": "MIN",
                "Volume": "VOLUME",
            })
            
            df["SYMBOL"] = symbol
            df = df[["SYMBOL", "DATE", "OPEN", "CLOSE", "MIN", "MAX", "VOLUME"]]
            
            frames.append(df)
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            continue
    
    if not frames:
        print("No data fetched for any symbol.")
        return 0

    final_df = pd.concat(frames, ignore_index=True)
    final_df["DATE"] = pd.to_datetime(final_df["DATE"]).dt.date

    original_rows = len(final_df)
    final_df = final_df.dropna(subset=['OPEN', 'CLOSE'], how='any')
    final_df['VOLUME'] = final_df['VOLUME'].fillna(0)
    cleaned_rows = len(final_df)
    
    if original_rows > cleaned_rows:
        print(f"Removed {original_rows - cleaned_rows} rows with NaN values")
    
    if final_df.empty:
        print("No valid data after cleaning NaN values.")
        return 0

    # 4. Write to Snowflake (Transaction + MERGE)    
    hook = SnowflakeHook(snowflake_conn_id = "snowflake_default")
    conn = hook.get_conn()
    cs = conn.cursor()

    try:
        cs.execute(f"USE WAREHOUSE {WAREHOUSE}") 
        cs.execute(f"USE DATABASE {DATABASE}")
        cs.execute(f"USE SCHEMA {SCHEMA_ETL}")
        

        cs.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        ver, wh, role, db, sch = cs.fetchone()

        # Create fact table if it doesn't exist
        cs.execute("""
            CREATE TABLE IF NOT EXISTS fact_stock_price_daily (
                SYMBOL VARCHAR(10) NOT NULL,
                "DATE" DATE NOT NULL,
                "OPEN" FLOAT,
                "CLOSE" FLOAT,
                "MIN" FLOAT,
                "MAX" FLOAT,
                VOLUME BIGINT,
                PRIMARY KEY (SYMBOL, "DATE")
            )
        """)
        
        cs.execute("BEGIN")

        # Create temporary table 
        cs.execute("""
            CREATE TEMPORARY TABLE tmp_fact_stock_price_daily AS
            SELECT * FROM fact_stock_price_daily WHERE 1=0
        """)

        rows = [
            {
                "SYMBOL": row[0],      
                "DATE": row[1],      
                "OPEN": float(row[2]),  
                "CLOSE": float(row[3]), 
                "MIN": float(row[4]), 
                "MAX": float(row[5]),   
                "VOLUME": int(row[6]) if pd.notna(row[6]) else 0,  
            }
            for row in final_df.itertuples(index=False)
        ]

        if rows:
            cs.executemany("""
                INSERT INTO tmp_fact_stock_price_daily
                (SYMBOL, "DATE", "OPEN", "CLOSE", "MIN", "MAX", VOLUME)
                VALUES (%(SYMBOL)s, %(DATE)s, %(OPEN)s, %(CLOSE)s, %(MIN)s, %(MAX)s, %(VOLUME)s)
            """, rows)

        
        # MERGE operation 
        cs.execute("""
            MERGE INTO fact_stock_price_daily t
            USING tmp_fact_stock_price_daily s
              ON t.SYMBOL = s.SYMBOL AND t."DATE" = s."DATE"
            WHEN MATCHED THEN UPDATE SET
              "OPEN"=s."OPEN", 
              "CLOSE"=s."CLOSE", 
              "MIN"=s."MIN", 
              "MAX"=s."MAX", 
              VOLUME=s.VOLUME
            WHEN NOT MATCHED THEN INSERT
              (SYMBOL, "DATE", "OPEN", "CLOSE", "MIN", "MAX", VOLUME)
            VALUES
              (s.SYMBOL, s."DATE", s."OPEN", s."CLOSE", s."MIN", s."MAX", s.VOLUME)
        """)

        cs.execute("COMMIT")
        print(f"Upserted {len(rows)} rows into fact_stock_price_daily.")
        return len(rows)
    
    except Exception as e:
        cs.execute("ROLLBACK")
        print(f"Error during database operation: {e}")
        raise
    finally:
        cs.close()
        conn.close()

# ============================================================================
# ELT (dbt)
# ============================================================================

default_args = {
    "owner": "data226_lab2",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_etl_dbt",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  
    catchup=False,
    default_args=default_args,
    tags=["lab2", "yfinance", "etl", "elt", "dbt"],
) as dag:

    # ============================================================================
    # ETL
    # ============================================================================

    etl_task = PythonOperator(
        task_id="fetch_and_upsert",
        python_callable=fetch_and_upsert,
        provide_context=True,
    )

    # ============================================================================
    # ELT
    # ============================================================================
    
    # Task 1: dbt debug - test connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir .",
    )

    # Task 2: dbt deps - install package dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
    )

    # Task 3: dbt snapshot - capture historical changes
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir .",
    )

    # Task 4: dbt run - execute all models (staging -> intermediate -> marts)
    dbt_run = BashOperator(
        task_id="dbt_run_all_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    # Task 5: dbt test - run all data quality tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    etl_task >> dbt_debug >> dbt_deps >> dbt_snapshot >> dbt_run >> dbt_test
