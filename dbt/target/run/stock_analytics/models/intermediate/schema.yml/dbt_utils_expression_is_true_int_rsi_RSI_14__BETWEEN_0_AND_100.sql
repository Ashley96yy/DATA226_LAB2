select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.dbt_utils_expression_is_true_int_rsi_RSI_14__BETWEEN_0_AND_100
    
      
    ) dbt_internal_test