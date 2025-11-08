-- SELECT
-- 	SYMBOL,
--     "DATE",
--     "CLOSE",
--     
--     -- Previous day's close
--     LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE") AS PREV_ADJ_CLOSE,
--     
--     -- Daily change (absolte)
--     "CLOSE" - LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE") AS ADJ_DAILY_CHANGE,
--     
--     -- Daily return (percentage)
--     ROUND(
--         ("CLOSE" - LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE")) 
--         / NULLIF(LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE"), 0) * 100, 
--         4
--     ) AS ADJ_DAILY_RETURN_PCT,
--     
--     -- Log return (for financial analysis)
--     LN(
--         "CLOSE" / NULLIF(LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE"), 0)
--     ) AS LOG_RETURN,
--     
--     -- Rolling volatility (20-day standard deviation)
--     ROUND(
--         STDDEV(
--             ("CLOSE" - LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE")) 
--             / NULLIF(LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE"), 0)
--         ) OVER (
--             PARTITION BY SYMBOL 
--             ORDER BY "DATE" 
--             ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
--         ) * 100,
--         4
--     ) AS VOLATILITY_20D,
--     
--     -- Cumulative return from start
--     ROUND(
--         (("CLOSE" / FIRST_VALUE("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE")) - 1) * 100,
--         4
--     ) AS CUMULATIVE_RETURN_PCT

-- FROM ASH_DB.STOCK_raw.stg_stock_daily
-- WHERE DATA_QUALITY_FLAG = 'VALID'



WITH base AS (
    SELECT
        SYMBOL,
        "DATE",
        "CLOSE",
        LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE") AS prev_close
    FROM ASH_DB.STOCK_raw.stg_stock_daily
    WHERE DATA_QUALITY_FLAG = 'VALID'
),

calc AS (
    SELECT
        SYMBOL,
        "DATE",
        "CLOSE",
        prev_close,
        ("CLOSE" - prev_close) AS adj_daily_change,
        CASE
            WHEN prev_close IS NOT NULL AND prev_close <> 0
                THEN ("CLOSE" - prev_close) / prev_close
            ELSE NULL
        END AS ret_decimal,
        CASE
            WHEN prev_close IS NOT NULL AND prev_close <> 0
                THEN LN("CLOSE" / prev_close)
            ELSE NULL
        END AS log_return
    FROM base
)

SELECT
    SYMBOL,
    "DATE",
    "CLOSE",
    prev_close AS PREV_ADJ_CLOSE,
    adj_daily_change AS ADJ_DAILY_CHANGE,
    ROUND(ret_decimal * 100, 4) AS ADJ_DAILY_RETURN_PCT,
    log_return AS LOG_RETURN,

    ROUND(
        STDDEV(ret_decimal) OVER (
            PARTITION BY SYMBOL
            ORDER BY "DATE"
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * 100,
        4
    ) AS VOLATILITY_20D,

    ROUND(
        ( "CLOSE" / FIRST_VALUE("CLOSE") OVER (
            PARTITION BY SYMBOL
            ORDER BY "DATE"
        ) - 1 ) * 100,
        4
    ) AS CUMULATIVE_RETURN_PCT
FROM calc