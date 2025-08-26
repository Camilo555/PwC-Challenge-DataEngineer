

{% macro restore_archived_data(table_name, schema_name, start_date, end_date=none) %}

-- Restore archived data back to main table
{%- set archive_table = schema_name + '.' + table_name + '_archive' -%}
{%- set main_table = schema_name + '.' + table_name -%}

{%- set date_condition -%}
    archived_at >= '{{ start_date }}'::timestamp
    {%- if end_date %} AND archived_at <= '{{ end_date }}'::timestamp{% endif -%}
{%- endset -%}

-- Check if archive table exists
{%- set archive_exists_query -%}
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = '{{ schema_name }}' 
          AND table_name = '{{ table_name }}_archive'
    )
{%- endset -%}

{%- set archive_exists = run_query(archive_exists_query) -%}

{% if archive_exists.rows[0][0] %}
    -- Count records to restore
    {%- set count_query = "SELECT COUNT(*) FROM " + archive_table + " WHERE " + date_condition -%}
    {%- set count_result = run_query(count_query) -%}
    {%- set records_to_restore = count_result.rows[0][0] if count_result.rows else 0 -%}
    
    {% if records_to_restore > 0 %}
        -- Perform restoration
        WITH restored_records AS (
            SELECT * EXCLUDE (archived_at, retention_policy_id)
            FROM {{ archive_table }}
            WHERE {{ date_condition }}
        ),
        
        insert_result AS (
            INSERT INTO {{ main_table }}
            SELECT * FROM restored_records
            ON CONFLICT DO NOTHING  -- Prevent duplicates if records already exist
            RETURNING *
        )
        
        SELECT 
            '{{ table_name }}' as table_name,
            '{{ schema_name }}' as schema_name,
            {{ records_to_restore }} as records_identified,
            COUNT(*) as records_restored,
            'success' as status,
            current_timestamp as restoration_timestamp
        FROM insert_result;
        
        -- Log restoration activity
        INSERT INTO data_retention_log (
            policy_id,
            records_evaluated,
            records_archived,
            records_deleted,
            execution_status,
            error_message
        )
        SELECT 
            gen_random_uuid(),
            {{ records_to_restore }},
            -{{ records_to_restore }}, -- Negative indicates restoration
            0,
            'success',
            'Data restoration from {{ start_date }} to {{ end_date or "present" }}'
        ;
        
    {% else %}
        SELECT 
            '{{ table_name }}' as table_name,
            '{{ schema_name }}' as schema_name,
            0 as records_identified,
            0 as records_restored,
            'no_data' as status,
            'No archived data found for specified date range' as message;
    {% endif %}
    
{% else %}
    SELECT 
        '{{ table_name }}' as table_name,
        '{{ schema_name }}' as schema_name,
        'error' as status,
        'Archive table does not exist' as message;
{% endif %}

{% endmacro %}