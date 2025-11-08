select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.dbt_utils_expression_is_true_i_073ecbdfb3f8461b722a2236ffc246ef
    
      
    ) dbt_internal_test