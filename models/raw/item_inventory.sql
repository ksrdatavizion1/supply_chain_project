--item_inventiry.sql
{{ config(
    materialized='incremental',
    unique_key='item_id',
     tags=['raw'],
    pre_hook=[
        "USE DATABASE {{ target.database }};",
        "USE SCHEMA LANDING;",
        "{{ copy_into_inventory() }}"
    ],
    post_hook=[
        """
        DELETE FROM {{ target.database }}.LANDING.RAW_INVENTORY
        WHERE LOAD_TS < DATEADD(DAY, -90, CURRENT_DATE);
        """,
        """
               INSERT INTO {{ target.database }}.AUDIT.MODEL_EXECUTION_LOG (
                    model_name,
                    load_date,
                    row_count,
                    file_name,
                    last_modified,
                    status,
                    comments
                )
                SELECT
                    'item_inventory' AS model_name,
                    MAX(LOAD_TS)     AS load_date,
                    COUNT(*)         AS row_count,
                    STG_FILE_NAME    AS file_name,
                    MAX(STG_LAST_MODIFIED) AS last_modified,
                    'SUCCESS'        AS status,
                    'Load completed successfully via COPY INTO + dbt incremental' AS comments
                FROM {{ target.database }}.LANDING.RAW_INVENTORY
                WHERE CAST(LOAD_TS AS DATE) = CURRENT_DATE
                AND STG_FILE_NAME IS NOT NULL
                AND STG_FILE_NAME NOT IN (
                    SELECT file_name
                    FROM {{ target.database }}.AUDIT.MODEL_EXECUTION_LOG
                    WHERE model_name = 'item_inventory'
                )
                GROUP BY STG_FILE_NAME;

        """
    ]
) }}

 SELECT
    item_id:: VARCHAR(100)           AS item_id,
    item_name ::VARCHAR(100)        AS item_name,
    category ::VARCHAR(100)        AS category,
    variant_name:: VARCHAR(100)    AS variant_name,
    fuel_type ::VARCHAR(100)       AS fuel_type
FROM {{ source('items', 'inventory') }}

{% if is_incremental() %}
WHERE item_id NOT IN (SELECT item_id FROM {{ this }})
{% endif %}