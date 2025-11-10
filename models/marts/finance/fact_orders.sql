with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
)

,payments as (
    select * from {{ ref('stg_stripe__payments') }}
    where status = 'success'
)

select
    o.order_id,
    o.customer_id,
    sum(p.amount) as amount
from orders as o
left join payments as p
    on o.order_id = p.order_id
group by all