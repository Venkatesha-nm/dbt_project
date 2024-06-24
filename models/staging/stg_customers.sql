with raw as (
    select * from {{source("Snowflake source","raw_customer")}}
),
final as (
    select ID as customer_id,
    FIRST_NAME,
    LAST_NAME,
    from raw
)

select * from final