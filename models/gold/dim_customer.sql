{{ config(
    alias='DIM_CUSTOMER'
) }}

SELECT DISTINCT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_EMAIL,
    CUSTOMER_PHONE,
    CITY_NAME
FROM {{ ref('customer_order_snapshot') }}
WHERE DBT_VALID_TO IS NULL


