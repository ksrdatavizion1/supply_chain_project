{% macro log_model_execution(model_name, table_name) %}
  {# 1. Get today's row count for records updated today and still active #}
  {% set row_count_query %}
    SELECT COUNT(*) AS new_count
    FROM {{ table_name }}
    WHERE DBT_VALID_TO IS NULL
      AND DATE(DBT_UPDATED_AT) = CURRENT_DATE
  {% endset %}

  {% set row_count_result = run_query(row_count_query) %}
  {% set row_count = 0 %}
  {% if row_count_result and row_count_result.rows %}
    {% set row_count = row_count_result.rows[0][0] %}
  {% endif %}

  {# 2. Check if log already exists today for this model #}
  {% set already_logged_query %}
    SELECT COUNT(*) FROM SUPPLY_CHAIN.AUDIT.MODEL_EXECUTION_LOG
    WHERE model_name = '{{ model_name }}'
      AND CAST(load_date AS DATE) = CURRENT_DATE
  {% endset %}

  {% set already_logged_result = run_query(already_logged_query) %}
  {% set already_logged = 0 %}
  {% if already_logged_result and already_logged_result.rows %}
    {% set already_logged = already_logged_result.rows[0][0] %}
  {% endif %}

  {# 3. Insert only if not logged yet and row_count > 0 #}
  {% if already_logged == 0  %}
    {% set insert_query %}
      INSERT INTO SUPPLY_CHAIN.AUDIT.MODEL_EXECUTION_LOG (
          model_name,
          load_date,
          row_count,
          file_name,
          last_modified,
          status,
          comments
      )
      VALUES (
          '{{ model_name }}',
          CURRENT_TIMESTAMP,
          {{ row_count }},
          NULL,
          NULL,
          'SUCCESS',
          'Snapshot completed successfully for {{ model_name }}'
      )
    {% endset %}
    {% do run_query(insert_query) %}
    {{ log("✅ Inserted log for " ~ model_name ~ " (row_count=" ~ row_count ~ ")", info=True) }}
  {% else %}
    {{ log("ℹ️ Skipping log insert for " ~ model_name ~ " (already_logged=" ~ already_logged ~ ", row_count=" ~ row_count ~ ")", info=True) }}
  {% endif %}
{% endmacro %}