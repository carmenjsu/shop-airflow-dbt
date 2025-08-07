WITH customers AS (
    SELECT
        customer_id,
        name,
        email
    FROM
        {{ ref('stg_customers') }}
)

SELECT 
    customer_id,
    name AS customer_name,
    email AS customer_email
FROM
    customers