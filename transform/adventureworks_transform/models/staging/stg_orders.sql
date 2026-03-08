SELECT
    salesorderid AS order_id,
    orderdate AS order_at,
    customerid AS customer_id,
    shiptoaddressid AS ship_address_id, -- Correction ici : ShipToAddressID
    billtoaddressid AS bill_address_id, -- Ajouté pour être complet
    status AS order_status,
    subtotal AS amount_subtotal,
    taxamt AS amount_tax,
    freight AS amount_freight,
    totaldue AS amount_total
FROM {{ source('azure_silver', 'SalesOrderHeader') }}