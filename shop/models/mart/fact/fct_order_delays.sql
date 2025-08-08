WITH orders_shipments AS (
    SELECT
        order_id,
        customer_id,
        order_date, 
        shipment_id,
        shipment_status,
        shipped_at,
        delivered_at
    FROM    
        {{ ref('fct_orders_shipments') }}

),

customers AS (
    SELECT
        customer_id,
        customer_name,
        customer_email
    FROM
        {{ ref('dim_customers') }}
)

SELECT 
    os.order_id,
    os.customer_id,
    c.customer_name,
    c.customer_email,
    os.shipment_id,
    os.shipment_status,
    os.shipped_at,
    os.delivered_at,
    CASE
        WHEN os.delivered_at IS NOT NULL THEN DATE(os.delivered_at) - DATE(os.shipped_at) 
        WHEN os.delivered_at IS NULL THEN DATEDIFF(day, os.shipped_at, CURRENT_TIMESTAMP)
    END AS date_difference,
    CASE 
        WHEN os.delivered_at IS NULL AND DATEDIFF(day, os.shipped_at, CURRENT_TIMESTAMP) <= 2 THEN 'In Transit'                        
        WHEN os.delivered_at IS NULL AND DATEDIFF(day, os.shipped_at, CURRENT_TIMESTAMP) > 2 THEN 'Delayed'
        WHEN os.delivered_at IS NOT NULL AND DATEDIFF(day, os.shipped_at, os.delivered_at) > 2 THEN 'Delayed'
        ELSE 'On Time'
    END AS delayed_status   
FROM
    orders_shipments os
LEFT JOIN
    customers c ON os.customer_id = c.customer_id
