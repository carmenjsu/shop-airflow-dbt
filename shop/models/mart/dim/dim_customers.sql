WITH customers AS (
    SELECT
        customer_id,
        customer_name,
        customer_email
    FROM
        {{ ref('stg_customers') }}
)

SELECT * FROM customers