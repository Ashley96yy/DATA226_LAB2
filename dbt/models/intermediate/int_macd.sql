{{
    config(
        materialized='table'
    )
}}

WITH ema_calc AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        
        -- 12-day EMA
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS EMA_12,
        
        -- 26-day EMA
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) AS EMA_26
    FROM {{ ref('stg_stock_daily') }}
    WHERE DATA_QUALITY_FLAG = 'VALID'
),

macd_line AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        EMA_12,
        EMA_26,
        ROUND(EMA_12 - EMA_26, 4) as MACD_LINE
    FROM ema_calc
),

signal_line AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        EMA_12,
        EMA_26,
        MACD_LINE,
        ROUND(
            AVG(MACD_LINE) OVER (
                PARTITION BY SYMBOL 
                ORDER BY "DATE" 
                ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS SIGNAL_LINE
    FROM macd_line
)

SELECT 
    SYMBOL,
    "DATE",
    "CLOSE",
    EMA_12,
    EMA_26,
    MACD_LINE,
    SIGNAL_LINE,
    
    -- MACD Histogram = MACD Line - Signal Line
    ROUND(MACD_LINE - SIGNAL_LINE, 4) AS MACD_HISTOGRAM,
    
    -- MACD Signal
    CASE 
        WHEN MACD_LINE > SIGNAL_LINE 
             AND LAG(MACD_LINE) OVER (PARTITION BY SYMBOL ORDER BY "DATE") <= LAG(SIGNAL_LINE) OVER (PARTITION BY SYMBOL ORDER BY "DATE")
        THEN 'BULLISH_CROSSOVER'
        WHEN MACD_LINE < SIGNAL_LINE 
             AND LAG(MACD_LINE) OVER (PARTITION BY SYMBOL ORDER BY "DATE") >= LAG(SIGNAL_LINE) OVER (PARTITION BY SYMBOL ORDER BY "DATE")
        THEN 'BEARISH_CROSSOVER'
        WHEN MACD_LINE > SIGNAL_LINE THEN 'BULLISH'
        WHEN MACD_LINE < SIGNAL_LINE THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS MACD_SIGNAL

FROM signal_line
