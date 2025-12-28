-- depends_on: {{ source('warehouse_loc', 'warehouse') }}

{{ config(
    materialized='incremental',
    unique_key='WAREHOUSE_NAME',
    tags=['silver'],
    on_schema_change='sync_all_columns',
    incremental_strategy='merge',
    merge_update_columns = ['CITY_NAME', 'STATE_NAME', 'REGION']
) }}

{% set src = source('warehouse_loc', 'warehouse') %}

SELECT
    CITY_NAME,
    STATE_NAME,
    REGION,
    WAREHOUSE_NAME,
    CURRENT_TIMESTAMP AS INGESTION_TS
FROM {{ target.database }}.{{ target.schema }}_{{ src.schema }}.{{ src.identifier }}