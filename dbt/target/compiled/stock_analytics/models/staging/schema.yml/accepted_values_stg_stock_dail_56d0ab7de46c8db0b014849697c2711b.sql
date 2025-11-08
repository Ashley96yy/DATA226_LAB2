
    
    

with all_values as (

    select
        DATA_QUALITY_FLAG as value_field,
        count(*) as n_records

    from ASH_DB.STOCK_raw.stg_stock_daily
    group by DATA_QUALITY_FLAG

)

select *
from all_values
where value_field not in (
    'VALID','INVALID','ANOMALY','MISSING','NO_TRADING'
)


