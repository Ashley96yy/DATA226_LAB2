select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.dbt_utils_expression_is_true_int_bollinger_BB_LOWER___BB_MIDDLE
    
      
    ) dbt_internal_test