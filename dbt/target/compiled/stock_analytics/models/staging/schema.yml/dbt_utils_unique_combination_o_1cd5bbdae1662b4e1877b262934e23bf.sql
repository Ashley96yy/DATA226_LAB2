





with validation_errors as (

    select
        SYMBOL, DATE
    from ASH_DB.STOCK_raw.stg_stock_daily
    group by SYMBOL, DATE
    having count(*) > 1

)

select *
from validation_errors


