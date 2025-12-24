{{ config(
    materialized='incremental',
    unique_key='warehouse_name',
    tags=['silver'],
    incremental_strategy='delete+insert'
) }}

{% set src = source('warehouse_loc', 'warehouse') %}

SELECT
    CITY_NAME,
    STATE_NAME,
    REGION,
    WAREHOUSE_NAME,
    
    {% if is_incremental() %}
        ingestion_ts
    {% else %}
        CURRENT_TIMESTAMP AS ingestion_ts
    {% endif %}

FROM {{ target.database }}.{{ target.schema }}_{{ src.schema }}.{{ src.identifier }}