{% snapshot stock_price_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='snapshot_key',
      strategy='check',
      check_cols=['CLOSE', 'VOLUME'],
      invalidate_hard_deletes=True
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['SYMBOL', 'DATE']) }} AS snapshot_key,
    SYMBOL,
    "DATE",
    "OPEN",
    "CLOSE",
    "MIN",
    "MAX",
    VOLUME
FROM {{ source('raw', 'fact_stock_price_daily') }}
WHERE "DATE" >= CURRENT_DATE - INTERVAL '180 days'

{% endsnapshot %}