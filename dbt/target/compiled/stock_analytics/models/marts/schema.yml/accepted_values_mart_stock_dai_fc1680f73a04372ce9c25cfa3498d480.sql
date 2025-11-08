
    
    

with all_values as (

    select
        TRADING_SIGNAL as value_field,
        count(*) as n_records

    from ASH_DB.STOCK_RAW_stock_analytics.mart_stock_daily_metrics
    group by TRADING_SIGNAL

)

select *
from all_values
where value_field not in (
    'STRONG_BUY','BUY','HOLD','SELL','STRONG_SELL'
)


