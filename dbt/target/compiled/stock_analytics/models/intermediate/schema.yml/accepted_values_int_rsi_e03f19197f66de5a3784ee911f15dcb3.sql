
    
    

with all_values as (

    select
        RSI_SIGNAL as value_field,
        count(*) as n_records

    from (select * from ASH_DB.STOCK_analytics.int_rsi where RSI_SIGNAL IS NOT NULL) dbt_subquery
    group by RSI_SIGNAL

)

select *
from all_values
where value_field not in (
    'OVERBOUGHT','OVERSOLD','NEUTRAL'
)


