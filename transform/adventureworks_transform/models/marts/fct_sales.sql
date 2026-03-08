WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
addresses AS (
    SELECT * FROM {{ ref('stg_address') }}
)

SELECT
    oi.order_item_id,
    o.order_id,
    o.order_at,
    o.customer_id,
    o.ship_address_id,
    a.city,
    a.country,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.item_total,
    o.amount_tax,
    o.amount_total
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN addresses a ON o.ship_address_id = a.address_id