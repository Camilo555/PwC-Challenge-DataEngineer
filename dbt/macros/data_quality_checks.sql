{% macro test_data_freshness(model, date_column, max_age_hours=24) %}
    select count(*)
    from {{ model }}
    where {{ date_column }} < current_timestamp - interval '{{ max_age_hours }} hours'
{% endmacro %}

{% macro test_row_count_anomaly(model, threshold_pct=50) %}
    with current_count as (
        select count(*) as row_count
        from {{ model }}
        where date = current_date - 1
    ),
    
    historical_avg as (
        select avg(daily_count) as avg_count
        from (
            select date, count(*) as daily_count
            from {{ model }}
            where date between current_date - 30 and current_date - 2
            group by date
        ) t
    )
    
    select 
        c.row_count,
        h.avg_count,
        case when h.avg_count > 0 then 
            abs((c.row_count - h.avg_count)::float / h.avg_count::float) * 100 
        else 0 end as variance_pct
    from current_count c
    cross join historical_avg h
    where case when h.avg_count > 0 then 
            abs((c.row_count - h.avg_count)::float / h.avg_count::float) * 100 
        else 0 end > {{ threshold_pct }}
{% endmacro %}

{% macro test_revenue_anomaly(model, revenue_column, threshold_pct=30) %}
    with current_revenue as (
        select sum({{ revenue_column }}) as revenue
        from {{ model }}
        where date = current_date - 1
    ),
    
    historical_avg as (
        select avg(daily_revenue) as avg_revenue
        from (
            select date, sum({{ revenue_column }}) as daily_revenue
            from {{ model }}
            where date between current_date - 30 and current_date - 2
            group by date
        ) t
    )
    
    select 
        c.revenue,
        h.avg_revenue,
        case when h.avg_revenue > 0 then 
            abs((c.revenue - h.avg_revenue)::float / h.avg_revenue::float) * 100 
        else 0 end as variance_pct
    from current_revenue c
    cross join historical_avg h
    where case when h.avg_revenue > 0 then 
            abs((c.revenue - h.avg_revenue)::float / h.avg_revenue::float) * 100 
        else 0 end > {{ threshold_pct }}
{% endmacro %}

{% macro test_null_percentage(model, column, max_null_pct=5) %}
    select 
        count(*) as total_rows,
        count(case when {{ column }} is null then 1 end) as null_rows,
        (count(case when {{ column }} is null then 1 end)::float / count(*)::float) * 100 as null_percentage
    from {{ model }}
    having (count(case when {{ column }} is null then 1 end)::float / count(*)::float) * 100 > {{ max_null_pct }}
{% endmacro %}

{% macro test_duplicate_records(model, unique_columns) %}
    select 
        {{ unique_columns | join(', ') }},
        count(*) as duplicate_count
    from {{ model }}
    group by {{ unique_columns | join(', ') }}
    having count(*) > 1
{% endmacro %}

{% macro test_referential_integrity(child_model, parent_model, child_key, parent_key) %}
    select 
        c.{{ child_key }},
        'Missing in parent table' as issue
    from {{ child_model }} c
    left join {{ parent_model }} p
        on c.{{ child_key }} = p.{{ parent_key }}
    where p.{{ parent_key }} is null
        and c.{{ child_key }} is not null
{% endmacro %}

{% macro test_business_rule_validation(model, rule_description, rule_sql) %}
    -- {{ rule_description }}
    select *
    from (
        {{ rule_sql }}
    ) violations
    where violations.violation_count > 0
{% endmacro %}

{% macro generate_data_quality_report(models) %}
    {% set report_query %}
        select 
            '{{ model }}' as model_name,
            'Row Count' as test_type,
            count(*) as metric_value,
            current_timestamp as test_timestamp
        from {{ model }}
        
        union all
        
        select 
            '{{ model }}' as model_name,
            'Null Customer IDs' as test_type,
            count(case when customer_id is null then 1 end) as metric_value,
            current_timestamp as test_timestamp
        from {{ model }}
        where '{{ model }}'.customer_id is defined
    {% endset %}
    
with quality_metrics as (
    {% for model in model_list %}
    {% set quality_tests = [
        {'name': 'row_count', 'sql': 'count(*)'},
        {'name': 'null_customer_ids', 'sql': "count(case when customer_id is null then 1 end)", 'condition': 'customer_id'},
        {'name': 'duplicate_records', 'sql': 'count(*) - count(distinct id)', 'condition': 'id'},
        {'name': 'future_dates', 'sql': "count(case when created_at > current_timestamp then 1 end)", 'condition': 'created_at'},
        {'name': 'negative_amounts', 'sql': "count(case when total_amount < 0 then 1 end)", 'condition': 'total_amount'}
    ] %}
    
    {% for test in quality_tests %}
    select 
        '{{ model }}' as model_name,
        '{{ test.name }}' as test_type,
        (select {{ test.sql }} from {{ model }}) as metric_value,
        current_timestamp as test_timestamp
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

quality_summary as (
    select 
        model_name,
        sum(case when test_type = 'row_count' then metric_value else 0 end) as total_rows,
        sum(case when test_type = 'null_customer_ids' then metric_value else 0 end) as null_customers,
        sum(case when test_type = 'duplicate_records' then metric_value else 0 end) as duplicates,
        sum(case when test_type = 'future_dates' then metric_value else 0 end) as future_dates,
        sum(case when test_type = 'negative_amounts' then metric_value else 0 end) as negative_amounts,
        
        -- Calculate quality score (0-100)
        case 
            when sum(case when test_type = 'row_count' then metric_value else 0 end) = 0 then 0
            else round(
                100.0 * (
                    1 - (
                        sum(case when test_type != 'row_count' then metric_value else 0 end) * 1.0 /
                        sum(case when test_type = 'row_count' then metric_value else 0 end)
                    )
                ), 2
            )
        end as quality_score,
        
        current_timestamp as report_timestamp
        
    from quality_metrics
    group by model_name
)

select * from quality_summary
union all
select 
    'OVERALL' as model_name,
    sum(total_rows) as total_rows,
    sum(null_customers) as null_customers,
    sum(duplicates) as duplicates,
    sum(future_dates) as future_dates,
    sum(negative_amounts) as negative_amounts,
    avg(quality_score) as quality_score,
    current_timestamp as report_timestamp
from quality_summary

{% endmacro %}

{% macro test_data_distribution(model, column, expected_distribution_type='normal') %}
    with distribution_stats as (
        select 
            count(*) as total_count,
            avg({{ column }}) as mean_value,
            stddev({{ column }}) as std_dev,
            min({{ column }}) as min_value,
            max({{ column }}) as max_value,
            percentile_cont(0.25) within group (order by {{ column }}) as q1,
            percentile_cont(0.5) within group (order by {{ column }}) as median,
            percentile_cont(0.75) within group (order by {{ column }}) as q3,
            percentile_cont(0.95) within group (order by {{ column }}) as p95,
            percentile_cont(0.99) within group (order by {{ column }}) as p99
        from {{ model }}
        where {{ column }} is not null
    ),
    
    outlier_detection as (
        select 
            ds.*,
            ds.q3 + 1.5 * (ds.q3 - ds.q1) as upper_fence,
            ds.q1 - 1.5 * (ds.q3 - ds.q1) as lower_fence,
            
            -- Skewness approximation
            case 
                when ds.std_dev > 0 then
                    (ds.mean_value - ds.median) / ds.std_dev
                else 0
            end as skewness
            
        from distribution_stats ds
    )
    
    select 
        '{{ column }}' as column_name,
        total_count,
        mean_value,
        std_dev,
        min_value,
        max_value,
        q1,
        median,
        q3,
        p95,
        p99,
        round(skewness, 4) as skewness,
        
        case 
            when abs(skewness) < 0.5 then 'Approximately Normal'
            when abs(skewness) < 1.0 then 'Moderately Skewed'
            else 'Highly Skewed'
        end as distribution_assessment,
        
        current_timestamp as analyzed_at
        
    from outlier_detection
{% endmacro %}

{% macro test_temporal_consistency(model, date_column, grain='day') %}
    with date_range as (
        select 
            min({{ date_column }})::date as start_date,
            max({{ date_column }})::date as end_date
        from {{ model }}
    ),
    
    expected_dates as (
        select generate_series(
            (select start_date from date_range),
            (select end_date from date_range),
            interval '1 {{ grain }}'
        )::date as expected_date
    ),
    
    actual_dates as (
        select 
            {{ date_column }}::date as actual_date,
            count(*) as record_count
        from {{ model }}
        group by {{ date_column }}::date
    ),
    
    missing_dates as (
        select ed.expected_date
        from expected_dates ed
        left join actual_dates ad
            on ed.expected_date = ad.actual_date
        where ad.actual_date is null
    )
    
    select 
        'temporal_consistency_check' as test_name,
        (select count(*) from missing_dates) as missing_date_count,
        (select count(*) from expected_dates) as expected_date_count,
        round(
            100.0 * (
                (select count(*) from expected_dates) - (select count(*) from missing_dates)
            ) / (select count(*) from expected_dates), 2
        ) as completeness_percentage,
        current_timestamp as test_timestamp
    
    -- Return results only if there are issues
    where (select count(*) from missing_dates) > 0
{% endmacro %}

{% macro monitor_data_quality_trends(model, quality_metrics_table) %}
    with current_metrics as (
        select 
            '{{ model }}' as model_name,
            count(*) as row_count,
            count(case when customer_id is null then 1 end) as null_customer_ids,
            count(case when total_amount < 0 then 1 end) as negative_amounts,
            count(case when created_at > current_timestamp then 1 end) as future_dates,
            current_date as metric_date
        from {{ model }}
    ),
    
    historical_metrics as (
        select 
            metric_date,
            row_count,
            null_customer_ids,
            negative_amounts,
            future_dates
        from {{ quality_metrics_table }}
        where model_name = '{{ model }}'
          and metric_date >= current_date - 30
        order by metric_date desc
        limit 30
    ),
    
    trend_analysis as (
        select 
            cm.*,
            
            -- Calculate quality score
            case 
                when cm.row_count = 0 then 0
                else 100.0 * (1 - ((cm.null_customer_ids + cm.negative_amounts + cm.future_dates) * 1.0 / cm.row_count))
            end as quality_score
            
        from current_metrics cm
    )
    
    select 
        model_name,
        row_count,
        null_customer_ids,
        negative_amounts,
        future_dates,
        quality_score,
        
        case 
            when quality_score < 80 then 'CRITICAL'
            when quality_score < 90 then 'WARNING'
            else 'OK'
        end as alert_level,
        
        metric_date,
        current_timestamp as calculated_at
        
    from trend_analysis
{% endmacro %}