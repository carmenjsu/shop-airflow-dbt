WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM
        {{ ref('stg_orders') }}
),

customers AS (
    SELECT
        customer_id,
        name,
        email
    FROM
        {{ ref('stg_customers') }}
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
)

SELECT 
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    s.shipment_id,
    s.status AS shipment_status,
    s.shipped_at,
    s.delivered_at,
    DATE(s.delivered_at) - DATE(s.shipped_at) AS date_difference,
    CASE 
        WHEN delivered_at IS NULL AND DATEDIFF(day, shipped_at, CURRENT_TIMESTAMP) <= 2 THEN 'In Transit'                        
        WHEN delivered_at IS NULL AND DATEDIFF(day, shipped_at, CURRENT_TIMESTAMP) > 2 THEN 'Delayed'
        WHEN delivered_at IS NOT NULL AND DATEDIFF(day, shipped_at, delivered_at) > 2 THEN 'Delayed'
        ELSE 'On Time'
    END AS delayed_status   
FROM
    orders o
LEFT JOIN
    customers c ON o.customer_id = c.customer_id
LEFT JOIN
    shipments s ON o.order_id = s.order_id