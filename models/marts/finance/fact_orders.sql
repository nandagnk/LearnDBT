with orders as
(select * from {{ ref('stg_jaffle_shop__orders') }}),
payments as
(select * from {{ ref('stg_stripe__payments') }}),
order_payments as (
    select 
        p.order_id,    
        sum(case when p.status = 'success' then p.amount  else 0 end) as order_amount
    from payments p
    group by p.order_id
),
final as (
    select 
        order_id,
        customer_id,
        order_date,
        order_amount
      from orders o left join order_payments using (order_id)
)
select *
from final