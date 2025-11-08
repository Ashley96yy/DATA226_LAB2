
    
    

with all_values as (

    select
        PRICE_TREND as value_field,
        count(*) as n_records

    from ASH_DB.STOCK_analytics.mart_stock_daily_metrics
    group by PRICE_TREND

)

select *
from all_values
where value_field not in (
    'STRONG_UPTREND','UPTREND','SIDEWAYS','DOWNTREND','STRONG_DOWNTREND'
)


