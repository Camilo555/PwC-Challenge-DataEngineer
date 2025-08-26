

{% macro get_primary_key_columns(table_name) %}
    {%- set pk_columns = {
        'fact_sales': ['sale_key'],
        'dim_customers': ['customer_sk'],
        'dim_products': ['product_sk'],
        'agg_sales_daily': ['date_key', 'customer_id', 'stock_code']
    } -%}
    {{ return(pk_columns.get(table_name, ['id'])) }}
{% endmacro %}