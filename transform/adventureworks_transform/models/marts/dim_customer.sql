SELECT
    customer_id,
    company_name,
    first_name || ' ' || last_name AS full_name,
    email,
    phone
FROM {{ ref('stg_customer') }}