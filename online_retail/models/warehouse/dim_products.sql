with staging as (

    select * from {{ ref('stg_online_retail') }}
    where product_id is not null

),

products as (

    select
        product_id,
        max(product_description),
        count(distinct invoice_id) as total_orders
    from staging
    group by product_id

)

select * from products