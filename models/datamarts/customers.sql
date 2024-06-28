{{
    config(
        schema='DBT_VNM'
    )
}}


with customers as 
(
select * from {{ref("stg_customers")}}
),
orders as (

    select * from {{ref("stg_orders")}}
),
payments as 
(
  select * from {{ref("stg_payments")}}
),
customer_level_details as 
(
   select c.FIRST_NAME
          ,c.LAST_NAME
          ,c.customer_id
          ,min(o.ORDER_DATE) as first_order          
          ,max(o.ORDER_DATE) as recent_order
          from customers c
   left join orders o on c.customer_id = o.customer_id
        group by 1,2,3
),
payment_details as 
(
   select o.customer_id
         ,sum(p.SALES_AMOUNT) as amount
         from payments p 
         left join orders o
         on p.ORDER_ID = o.ORDER_ID
         group by 1
),
final as 
(
   select c.FIRST_NAME
          ,c.LAST_NAME
          ,c.customer_id
          ,c.first_order
          ,c.recent_order
          ,p.AMOUNT
           from customer_level_details c
        left join payment_details p on c.customer_id = p.customer_id
)
select * from final




--select * from customer_level_details