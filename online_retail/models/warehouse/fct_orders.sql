with staging as (

    select * from {{ ref('stg_online_retail') }}
    where customer_id is not null

),

aggregated as (

    select
        invoice_id,
        customer_id,
        product_id,
        date(invoice_timestamp)            as invoice_date,
        min(invoice_timestamp)             as invoice_timestamp,
        sum(quantity)                      as quantity,
        avg(unit_price)                    as unit_price,
        sum(quantity * unit_price)         as line_total,
        case
            when invoice_id like 'C%' then true
            else false
        end                                as is_cancellation
    from staging
    group by
        invoice_id,
        customer_id,
        product_id,
        date(invoice_timestamp),
        case when invoice_id like 'C%' then true else false end

)

select * from aggregated