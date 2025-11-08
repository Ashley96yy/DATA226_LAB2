

WITH ma_std AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        ROUND(
            AVG("CLOSE") OVER (
                PARTITION BY SYMBOL 
                ORDER BY "DATE" 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS BB_MIDDLE,
        ROUND(
            STDDEV("CLOSE") OVER (
                PARTITION BY SYMBOL 
                ORDER BY "DATE" 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS BB_STDDEV
    FROM ASH_DB.STOCK_raw.stg_stock_daily
    WHERE DATA_QUALITY_FLAG = 'VALID'
)

SELECT 
    SYMBOL,
    "DATE",
    "CLOSE",
    BB_MIDDLE,
    BB_STDDEV,
    ROUND(BB_MIDDLE + (2 * BB_STDDEV), 4) AS BB_UPPER,
    ROUND(BB_MIDDLE - (2 * BB_STDDEV), 4) AS BB_LOWER,
    
    -- Band Width (volatility measure)
    ROUND(
        ((BB_MIDDLE + (2 * BB_STDDEV)) - (BB_MIDDLE - (2 * BB_STDDEV))) / NULLIF(BB_MIDDLE, 0) * 100,
        4
    ) AS BB_WIDTH_PCT,
    
    -- %B (price position within bands)
    ROUND(
        ("CLOSE" - (BB_MIDDLE - (2 * BB_STDDEV))) / 
        NULLIF((BB_MIDDLE + (2 * BB_STDDEV)) - (BB_MIDDLE - (2 * BB_STDDEV)), 0) * 100,
        4
    ) AS BB_PERCENT_B,
    
    -- Bollinger Band Signal
    CASE 
        WHEN "CLOSE" > BB_MIDDLE + (2 * BB_STDDEV) THEN 'ABOVE_UPPER_BAND'
        WHEN "CLOSE" < BB_MIDDLE - (2 * BB_STDDEV) THEN 'BELOW_LOWER_BAND'
        WHEN "CLOSE" > BB_MIDDLE THEN 'ABOVE_MIDDLE'
        WHEN "CLOSE" < BB_MIDDLE THEN 'BELOW_MIDDLE'
        ELSE 'AT_MIDDLE'
    END AS BB_SIGNAL,
    
    -- Bollinger Squeeze (low volatility)
    CASE 
        WHEN ((BB_MIDDLE + (2 * BB_STDDEV)) - (BB_MIDDLE - (2 * BB_STDDEV))) / NULLIF(BB_MIDDLE, 0) < 0.10 
        THEN TRUE
        ELSE FALSE
    END AS BB_SQUEEZE

FROM ma_std