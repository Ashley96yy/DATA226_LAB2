



select
    *
from (select * from ASH_DB.STOCK_analytics.int_daily_returns where VOLATILITY_20D IS NOT NULL) dbt_subquery

where not(VOLATILITY_20D >= 0)

