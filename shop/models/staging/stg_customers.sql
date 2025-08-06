WITH customers AS (
    SELECT
        customer_id,
        name,
        email
    FROM
        raw.RAW_CUSTOMER
)

SELECT * FROM customers