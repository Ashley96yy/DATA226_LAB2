{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_stock_daily') }}
    WHERE DATA_QUALITY_FLAG = 'VALID'
),

ma AS (
    SELECT * FROM {{ ref('int_moving_averages') }}
),

rsi AS (
    SELECT * FROM {{ ref('int_rsi') }}
),

bollinger AS (
    SELECT * FROM {{ ref('int_bollinger') }}
),

macd AS (
    SELECT * FROM {{ ref('int_macd') }}
)

SELECT 
    -- Basic information
    base.SYMBOL,
    base."DATE",
    base."CLOSE",
    base."OPEN",
    base."MAX",
    base."MIN",
    base.VOLUME,
    
    -- Daily changes
    base.DAILY_CHANGE,
    base.DAILY_CHANGE_PCT,
    base.INTRADAY_RANGE,
    base.INTRADAY_RANGE_PCT,
    
    -- Moving Averages
    ma.MA_5,
    ma.MA_20,
    ma.MA_50,
    ma.EMA_12,
    ma.EMA_26,
    
    -- MA Cross Signals (Golden Cross / Death Cross)
    CASE 
        -- Golden Cross: MA5 crosses above MA20
        WHEN ma.MA_5 > ma.MA_20 
             AND LAG(ma.MA_5) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE") <= LAG(ma.MA_20) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE") 
        THEN 'GOLDEN_CROSS_5_20'
        
        -- Death Cross: MA5 crosses below MA20
        WHEN ma.MA_5 < ma.MA_20 
             AND LAG(ma.MA_5) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE") >= LAG(ma.MA_20) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE")
        THEN 'DEATH_CROSS_5_20'
        
        -- Golden Cross: MA20 crosses above MA50
        WHEN ma.MA_20 > ma.MA_50 
             AND LAG(ma.MA_20) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE") <= LAG(ma.MA_50) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE")
        THEN 'GOLDEN_CROSS_20_50'
        
        -- Death Cross: MA20 crosses below MA50
        WHEN ma.MA_20 < ma.MA_50 
             AND LAG(ma.MA_20) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE") >= LAG(ma.MA_50) OVER (PARTITION BY base.SYMBOL ORDER BY base."DATE")
        THEN 'DEATH_CROSS_20_50'
        
        ELSE 'NO_CROSS'
    END as MA_CROSS_SIGNAL,
    
    -- Price vs MA trend analysis
    CASE 
        WHEN base."CLOSE" > ma.MA_5 AND base."CLOSE" > ma.MA_20 AND base."CLOSE" > ma.MA_50 THEN 'STRONG_UPTREND'
        WHEN base."CLOSE" < ma.MA_5 AND base."CLOSE" < ma.MA_20 AND base."CLOSE" < ma.MA_50 THEN 'STRONG_DOWNTREND'
        WHEN base."CLOSE" > ma.MA_20 THEN 'UPTREND'
        WHEN base."CLOSE" < ma.MA_20 THEN 'DOWNTREND'
        ELSE 'SIDEWAYS'
    END as PRICE_TREND,
    
    -- RSI indicators
    rsi.RSI_14,
    rsi.RSI_SIGNAL,
    
    -- Bollinger Bands
    bollinger.BB_UPPER,
    bollinger.BB_LOWER,
    bollinger.BB_MIDDLE,
    bollinger.BB_WIDTH_PCT,
    bollinger.BB_PERCENT_B,
    bollinger.BB_SIGNAL,
    bollinger.BB_SQUEEZE,
    
    -- MACD indicators
    macd.MACD_LINE,
    macd.SIGNAL_LINE,
    macd.MACD_HISTOGRAM,
    macd.MACD_SIGNAL,
    
    -- Volatility (using intraday range as proxy)
    CASE 
        WHEN base.INTRADAY_RANGE_PCT > 5 THEN 'HIGH_VOLATILITY'
        WHEN base.INTRADAY_RANGE_PCT < 1 THEN 'LOW_VOLATILITY'
        ELSE 'NORMAL_VOLATILITY'
    END as VOLATILITY_SIGNAL

FROM base
LEFT JOIN ma 
    ON base.SYMBOL = ma.SYMBOL AND base."DATE" = ma."DATE"
LEFT JOIN rsi 
    ON base.SYMBOL = rsi.SYMBOL AND base."DATE" = rsi."DATE"
LEFT JOIN bollinger 
    ON base.SYMBOL = bollinger.SYMBOL AND base."DATE" = bollinger."DATE"
LEFT JOIN macd 
    ON base.SYMBOL = macd.SYMBOL AND base."DATE" = macd."DATE"