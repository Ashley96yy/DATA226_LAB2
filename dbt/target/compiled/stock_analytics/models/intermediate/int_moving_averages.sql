

SELECT 
    SYMBOL,
    "DATE",
    "CLOSE",
    
    -- Short-term moving averages
    ROUND(
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        4
    ) as MA_5,
    
    -- Medium-term moving averages
    ROUND(
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        4
    ) as MA_20,
    
    -- Long-term moving averages
    ROUND(
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ),
        4
    ) as MA_50,
    
    -- Exponential Moving Averages
    ROUND(
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ),
        4
    ) as EMA_12,
    
    ROUND(
        AVG("CLOSE") OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ),
        4
    ) as EMA_26,
    
    -- Price position relative to MA20
    ROUND(
        (("CLOSE" / NULLIF(
            AVG("CLOSE") OVER (
                PARTITION BY SYMBOL 
                ORDER BY "DATE" 
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ), 
            0
        )) - 1) * 100,
        4
    ) AS PRICE_VS_MA20_PCT,
    
    -- Golden Cross / Death Cross signal
    CASE 
        WHEN AVG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) >
             AVG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE" ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
        THEN 'GOLDEN_CROSS'
        WHEN AVG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE" ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) <
             AVG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE" ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
        THEN 'DEATH_CROSS'
        ELSE 'NEUTRAL'
    END AS MA_CROSS_SIGNAL

FROM ASH_DB.STOCK_raw.stg_stock_daily
WHERE DATA_QUALITY_FLAG = 'VALID'