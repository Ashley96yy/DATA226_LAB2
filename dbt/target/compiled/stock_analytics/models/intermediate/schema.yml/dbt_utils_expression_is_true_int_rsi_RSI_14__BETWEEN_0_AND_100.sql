



select
    *
from (select * from ASH_DB.STOCK_analytics.int_rsi where RSI_14 IS NOT NULL) dbt_subquery

where not(RSI_14 BETWEEN 0 AND 100)

