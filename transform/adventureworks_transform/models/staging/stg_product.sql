WITH raw_product AS (
    SELECT * FROM {{ source('azure_silver', 'Product') }}
)

SELECT
    productid AS product_id,
    name AS product_name,
    productnumber AS product_number,
    color,
    standardcost AS standard_cost,
    listprice AS list_price,
    productcategoryid AS product_category_id,
    productmodelid AS product_model_id,
    sellstartdate AS sell_start_at,
    modifieddate AS last_update_at
FROM raw_product