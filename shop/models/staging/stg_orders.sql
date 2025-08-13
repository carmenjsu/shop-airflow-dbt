WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM
        {{ source('raw', 'raw_order') }} 
)
SELECT * FROM orders