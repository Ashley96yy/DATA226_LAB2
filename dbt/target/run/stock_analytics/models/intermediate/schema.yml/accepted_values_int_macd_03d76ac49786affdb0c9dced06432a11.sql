select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.accepted_values_int_macd_03d76ac49786affdb0c9dced06432a11
    
      
    ) dbt_internal_test