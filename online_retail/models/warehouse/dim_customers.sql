with staging as (

    select * from {{ ref('stg_online_retail') }}
    where customer_id is not null

),

customers as (

    select
        customer_id,
        max(country) as country,
        min(date(invoice_timestamp))   as first_order_date,
        max(date(invoice_timestamp))   as most_recent_order_date,
        count(distinct invoice_id)     as total_orders
    from staging
    group by customer_id

)

select * from customers