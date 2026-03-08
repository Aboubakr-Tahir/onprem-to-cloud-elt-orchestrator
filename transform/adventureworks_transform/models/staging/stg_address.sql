SELECT
    addressid AS address_id,
    addressline1 AS address_1,
    city,
    stateprovince AS state,
    countryregion AS country,
    postalcode AS postal_code
FROM {{ source('azure_silver', 'Address') }}