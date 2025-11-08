select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from ASH_DB.STOCK_test_failures.accepted_values_mart_stock_dai_7f3b5ce4f2b7b009a1eb3e6e2df356f8
    
      
    ) dbt_internal_test