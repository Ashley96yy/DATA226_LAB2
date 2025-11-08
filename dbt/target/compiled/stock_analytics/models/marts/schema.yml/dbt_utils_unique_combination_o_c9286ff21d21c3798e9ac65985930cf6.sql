





with validation_errors as (

    select
        SYMBOL, DATE
    from ASH_DB.STOCK_analytics.mart_stock_daily_metrics
    group by SYMBOL, DATE
    having count(*) > 1

)

select *
from validation_errors


