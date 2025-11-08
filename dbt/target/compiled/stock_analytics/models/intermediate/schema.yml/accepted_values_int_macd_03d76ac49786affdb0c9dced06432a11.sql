
    
    

with all_values as (

    select
        MACD_SIGNAL as value_field,
        count(*) as n_records

    from (select * from ASH_DB.STOCK_analytics.int_macd where MACD_SIGNAL IS NOT NULL) dbt_subquery
    group by MACD_SIGNAL

)

select *
from all_values
where value_field not in (
    'BULLISH_CROSSOVER','BEARISH_CROSSOVER','BULLISH','BEARISH','NEUTRAL'
)


