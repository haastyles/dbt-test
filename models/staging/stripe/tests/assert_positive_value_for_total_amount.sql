with payments as (
    select *
    from {{ ref('stg_stripe__payments') }}
)

,order_sum as (
    select 
        order_id,
        sum(amount) as order_sum
    from payments
    group by all
)

select *
from order_sum
having order_sum < 0