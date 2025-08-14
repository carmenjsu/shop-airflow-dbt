WITH customers AS (
    SELECT
        customer_id,
        name AS customer_name,
        email AS customer_email
    FROM
        {{ source('raw', 'raw_customer') }} 
)

SELECT * FROM customers