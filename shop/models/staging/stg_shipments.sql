WITH shipments AS (
    SELECT
        shipment_id,
        order_id,
        status,
        shipped_at,
        delivered_at
    FROM
        raw.RAW_SHIPMENT
)

SELECT * FROM shipments