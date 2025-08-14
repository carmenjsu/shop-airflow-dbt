WITH shipments AS (
    SELECT
        shipment_id,
        order_id,
        status,
        shipped_at,
        delivered_at
    FROM
        {{ source('raw', 'raw_shipment') }} 
)

SELECT * FROM shipments