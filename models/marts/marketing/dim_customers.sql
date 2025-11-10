with customers as (

     select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as ( 

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where status = 'success'

),

order_payments as (
    select 
        o.*,
        sum(amount) as amount
    from orders as o
    left join payments as p using (order_id)
    group by all
),

customer_order_payments as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value

    from order_payments as o

    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        cop.first_order_date,
        cop.most_recent_order_date,
        coalesce (cop.number_of_orders, 0) as number_of_orders,
        cop.lifetime_value

    from customers

    left join customer_order_payments as cop using (customer_id)

)

select * from final