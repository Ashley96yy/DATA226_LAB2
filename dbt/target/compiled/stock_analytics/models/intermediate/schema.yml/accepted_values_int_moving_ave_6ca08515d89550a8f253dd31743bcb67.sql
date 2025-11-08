
    
    

with all_values as (

    select
        MA_CROSS_SIGNAL as value_field,
        count(*) as n_records

    from (select * from ASH_DB.STOCK_analytics.int_moving_averages where MA_CROSS_SIGNAL IS NOT NULL) dbt_subquery
    group by MA_CROSS_SIGNAL

)

select *
from all_values
where value_field not in (
    'GOLDEN_CROSS','DEATH_CROSS','NEUTRAL'
)


