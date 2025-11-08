



select
    *
from (select * from ASH_DB.STOCK_analytics.int_bollinger where BB_LOWER IS NOT NULL AND BB_MIDDLE IS NOT NULL) dbt_subquery

where not(BB_LOWER < BB_MIDDLE)

