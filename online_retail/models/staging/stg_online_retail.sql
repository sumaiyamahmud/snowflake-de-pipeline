with source as (

    select * from {{ source('raw', 'online_retail') }}

),

renamed as (

    select
        invoice_no       as invoice_id,
        stock_code       as product_id,
        description      as product_description,
        quantity,
        invoice_date     as invoice_timestamp,
        unit_price,
        customer_id,
        country
    from source
    where invoice_no is not null
      and stock_code is not null

)

select * from renamed