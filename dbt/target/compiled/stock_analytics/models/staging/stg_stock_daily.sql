

WITH source_data AS (
	SELECT
		SYMBOL,
        "DATE",
        "OPEN",
        "CLOSE",
        "MIN",
        "MAX",
        VOLUME
	FROM ASH_DB.STOCK.fact_stock_price_daily
),
data_quality_check AS (
	SELECT
		*,
        CASE
			WHEN "CLOSE" = 0 OR "OPEN" = 0 THEN 'INVALID'
			WHEN "MAX" < "MIN" THEN 'ANOMALY'
			WHEN "CLOSE" IS NULL OR "OPEN" IS NULL THEN 'MISSING'
			ELSE 'VALID'
		END AS DATA_QUALITY_FLAG
	FROM source_data
)

SELECT 
	-- Basic fileds
    SYMBOL,
    "DATE",
    "OPEN",
    "CLOSE",
    "MIN",
    "MAX",
    VOLUME,
    DATA_QUALITY_FLAG,
    
    -- Daily price change
    "CLOSE" - "OPEN" as DAILY_CHANGE,
    
    -- Daily percentage change
    ROUND(("CLOSE" - "OPEN") / NULLIF("OPEN", 0) * 100, 2) as DAILY_CHANGE_PCT,
    
    -- Intraday range
    "MAX" - "MIN" as INTRADAY_RANGE,
    
    -- Intraday range (percentage)
    ROUND(("MAX" - "MIN") / NULLIF("MIN", 0) * 100, 2) as INTRADAY_RANGE_PCT

FROM data_quality_check
WHERE DATA_QUALITY_FLAG = 'VALID'