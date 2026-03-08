WITH raw_customer AS (
    SELECT * FROM {{ source('azure_silver', 'Customer') }}
)

SELECT
    customerid AS customer_id,
    companyname AS company_name,
    firstname AS first_name,
    lastname AS last_name,
    emailaddress AS email,
    phone,
    modifieddate AS last_update_at
FROM raw_customer