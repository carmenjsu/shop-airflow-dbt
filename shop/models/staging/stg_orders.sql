WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM
        raw.RAW_ORDER
)
SELECT * FROM orders