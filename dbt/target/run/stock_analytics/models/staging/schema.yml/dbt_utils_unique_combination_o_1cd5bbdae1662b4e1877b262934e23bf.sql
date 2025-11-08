select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.dbt_utils_unique_combination_o_1cd5bbdae1662b4e1877b262934e23bf
    
      
    ) dbt_internal_test