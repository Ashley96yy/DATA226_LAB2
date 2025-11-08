select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.accepted_values_stg_stock_dail_56d0ab7de46c8db0b014849697c2711b
    
      
    ) dbt_internal_test