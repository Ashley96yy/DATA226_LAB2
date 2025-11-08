select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.not_null_stg_stock_daily_DATA_QUALITY_FLAG
    
      
    ) dbt_internal_test