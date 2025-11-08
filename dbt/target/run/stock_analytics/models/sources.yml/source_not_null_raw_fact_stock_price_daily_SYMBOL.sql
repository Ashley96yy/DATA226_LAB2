select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.source_not_null_raw_fact_stock_price_daily_SYMBOL
    
      
    ) dbt_internal_test