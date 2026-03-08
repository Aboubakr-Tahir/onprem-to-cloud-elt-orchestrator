SELECT
    salesorderid AS order_id,
    salesorderdetailid AS order_item_id,
    orderqty AS quantity,
    productid AS product_id,
    unitprice AS unit_price,
    unitpricediscount AS unit_price_discount,
    linetotal AS item_total
FROM {{ source('azure_silver', 'SalesOrderDetail') }}