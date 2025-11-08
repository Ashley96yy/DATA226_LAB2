


WITH price_changes AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        "CLOSE" - LAG("CLOSE") OVER (PARTITION BY SYMBOL ORDER BY "DATE") AS PRICE_CHANGE
    FROM ASH_DB.STOCK_raw.stg_stock_daily
    WHERE DATA_QUALITY_FLAG = 'VALID'
),

gains_losses AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        CASE WHEN PRICE_CHANGE > 0 THEN PRICE_CHANGE ELSE 0 END as GAIN,
        CASE WHEN PRICE_CHANGE < 0 THEN ABS(PRICE_CHANGE) ELSE 0 END as LOSS
    FROM price_changes
),

avg_gains_losses AS (
    SELECT 
        SYMBOL,
        "DATE",
        "CLOSE",
        
        -- Average gain over 14 days
        AVG(gain) OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS AVG_GAIN_14,
        
        -- Average loss over 14 days
        AVG(loss) OVER (
            PARTITION BY SYMBOL 
            ORDER BY "DATE" 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS AVG_LOSS_14
    FROM gains_losses
)

SELECT 
    SYMBOL,
    "DATE",
    "CLOSE",
    AVG_GAIN_14,
    AVG_LOSS_14,
    
    -- Relative Strength
    ROUND(
        AVG_GAIN_14 / NULLIF(AVG_LOSS_14, 0),
        4
    ) AS RELATIVE_STRENGTH,
    
    -- RSI (14-day)
    ROUND(
        100 - (100 / (1 + (AVG_GAIN_14 / NULLIF(AVG_LOSS_14, 0)))),
        4
    ) AS RSI_14,
    
    -- RSI signal 
    CASE 
        WHEN 100 - (100 / (1 + (AVG_GAIN_14 / NULLIF(AVG_LOSS_14, 0)))) > 70 THEN 'OVERBOUGHT'
        WHEN 100 - (100 / (1 + (AVG_GAIN_14 / NULLIF(AVG_LOSS_14, 0)))) < 30 THEN 'OVERSOLD'
        ELSE 'NEUTRAL'
    END AS RSI_SIGNAL

FROM avg_gains_losses