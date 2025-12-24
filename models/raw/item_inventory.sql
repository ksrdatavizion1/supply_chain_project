{{
    config(
        materialized="incremental",
        unique_key="item_id",
        incremental_strategy="merge",
        tags=["raw"],
        pre_hook=[
            "USE DATABASE {{ target.database }};",
            "USE SCHEMA LANDING;",
            "{{ copy_into_inventory() }}",
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

        """,
        ],
    )
}}

select
    item_id::varchar(100) as item_id,
    item_name::varchar(100) as item_name,
    category::varchar(100) as category,
    variant_name::varchar(100) as variant_name,
    fuel_type::varchar(100) as fuel_type
from {{ source("items", "inventory") }}

{% if is_incremental() %}
    where item_id not in (select item_id from {{ this }})
{% endif %}
