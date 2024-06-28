with raw as(
    select * from {{source("Snowflake source","raw_payments")}}
),
final as (
    select ID as PAYMET_ID,
    ORDER_ID,
    PAYMENT_METHOD as PAYMENT_MODE,
    AMOUNT/100 as SALES_AMOUNT
    from raw
)
select * from final