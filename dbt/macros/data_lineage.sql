

{% macro log_model_run(model_name, rows_affected=0, execution_time_ms=0, status='success', error_message=null) %}

-- Log model execution for lineage tracking
WITH model_node AS (
    SELECT node_id FROM data_lineage_nodes WHERE node_name = '{{ model_name }}'
)
INSERT INTO data_lineage_runs (
    node_id,
    run_started_at,
    run_completed_at,
    status,
    rows_affected,
    execution_time_ms,
    error_message,
    dbt_run_id
)
SELECT 
    mn.node_id,
    current_timestamp - interval '{{ execution_time_ms }} milliseconds',
    current_timestamp,
    '{{ status }}',
    {{ rows_affected }},
    {{ execution_time_ms }},
    {% if error_message %}'{{ error_message }}'{% else %}NULL{% endif %},
    '{{ invocation_id }}'
FROM model_node mn;

{% endmacro %}