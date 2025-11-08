select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.not_null_int_moving_averages_DATE
    
      
    ) dbt_internal_test