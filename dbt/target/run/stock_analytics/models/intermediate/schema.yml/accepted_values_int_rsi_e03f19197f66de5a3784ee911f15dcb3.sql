select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.accepted_values_int_rsi_e03f19197f66de5a3784ee911f15dcb3
    
      
    ) dbt_internal_test