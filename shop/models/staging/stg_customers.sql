WITH customers AS (
    SELECT
        customer_id,
        name AS customer_name,
        email AS customer_email
    FROM
        raw.RAW_CUSTOMER
)

SELECT * FROM customers