select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.dbt_utils_unique_combination_o_c9286ff21d21c3798e9ac65985930cf6
    
      
    ) dbt_internal_test