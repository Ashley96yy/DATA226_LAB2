select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.accepted_values_int_moving_ave_6ca08515d89550a8f253dd31743bcb67
    
      
    ) dbt_internal_test