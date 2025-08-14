WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM
        {{ ref('stg_orders') }}
),

shipments AS (
    SELECT
        shipment_id,
        order_id,
        status,
        shipped_at,
        delivered_at
    FROM
        {{ ref('stg_shipments') }}
),

orders_shipments AS (
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    s.shipment_id,
    s.status AS shipment_status,
    s.shipped_at, 
    s.delivered_at          
FROM orders o
LEFT JOIN shipments s ON o.order_id = s.order_id
)

SELECT * FROM orders_shipments