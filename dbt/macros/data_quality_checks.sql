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


-- Comprehensive Data Quality Framework
{% macro run_comprehensive_data_quality_suite(model_name) %}

with quality_tests as (
    -- Row count validation
    select 
        '{{ model_name }}' as model_name,
        'row_count' as test_type,
        count(*) as metric_value,
        case when count(*) = 0 then 'FAIL' else 'PASS' end as test_result,
        'Table should contain data' as test_description
    from {{ model_name }}
    
    union all
    
    -- Duplicate detection  
    select 
        '{{ model_name }}' as model_name,
        'duplicate_detection' as test_type,
        count(*) - count(distinct id) as metric_value,
        case when count(*) - count(distinct id) > 0 then 'FAIL' else 'PASS' end as test_result,
        'No duplicate records should exist' as test_description
    from {{ model_name }}
    where id is not null
    
    union all
    
    -- NULL key validation
    select 
        '{{ model_name }}' as model_name,
        'null_primary_keys' as test_type,
        count(case when id is null then 1 end) as metric_value,
        case when count(case when id is null then 1 end) > 0 then 'FAIL' else 'PASS' end as test_result,
        'Primary keys should not be NULL' as test_description
    from {{ model_name }}
    
    union all
    
    -- Future date validation
    select 
        '{{ model_name }}' as model_name,
        'future_dates' as test_type,
        count(case when created_at > current_timestamp then 1 end) as metric_value,
        case when count(case when created_at > current_timestamp then 1 end) > 0 then 'FAIL' else 'PASS' end as test_result,
        'Dates should not be in the future' as test_description
    from {{ model_name }}
    where created_at is not null
    
    union all
    
    -- Data completeness validation
    select 
        '{{ model_name }}' as model_name,
        'data_completeness' as test_type,
        round(100.0 * count(case when customer_id is not null then 1 end) / count(*), 2) as metric_value,
        case 
            when 100.0 * count(case when customer_id is not null then 1 end) / count(*) < 95 
            then 'FAIL' else 'PASS' 
        end as test_result,
        'Customer ID completeness should be >= 95%' as test_description
    from {{ model_name }}
    
    union all
    
    -- Business rule validation - positive amounts
    select 
        '{{ model_name }}' as model_name,
        'positive_amounts' as test_type,
        count(case when total_amount <= 0 and not is_return then 1 end) as metric_value,
        case 
            when count(case when total_amount <= 0 and not is_return then 1 end) > 0 
            then 'FAIL' else 'PASS' 
        end as test_result,
        'Non-return transactions should have positive amounts' as test_description
    from {{ model_name }}
    where total_amount is not null and is_return is not null
    
    union all
    
    -- Referential integrity - customer dimension
    select 
        '{{ model_name }}' as model_name,
        'customer_referential_integrity' as test_type,
        count(*) as metric_value,
        case when count(*) > 0 then 'FAIL' else 'PASS' end as test_result,
        'All customers should exist in customer dimension' as test_description
    from {{ model_name }} f
    left join {{ ref('dim_customers') }} d on f.customer_id = d.customer_id and d.is_current = true
    where f.customer_id is not null 
      and d.customer_id is null
    
    union all
    
    -- Statistical outlier detection
    select 
        '{{ model_name }}' as model_name,
        'statistical_outliers' as test_type,
        count(*) as metric_value,
        case when count(*) > (select count(*) * 0.05 from {{ model_name }}) then 'WARNING' else 'PASS' end as test_result,
        'Statistical outliers should be < 5% of dataset' as test_description
    from (
        select 
            total_amount,
            avg(total_amount) over () as mean_amount,
            stddev(total_amount) over () as stddev_amount
        from {{ model_name }}
        where total_amount is not null and not is_return
    ) outlier_check
    where abs(total_amount - mean_amount) > 3 * stddev_amount
    
    union all
    
    -- Data freshness validation
    select 
        '{{ model_name }}' as model_name,
        'data_freshness' as test_type,
        extract(hour from current_timestamp - max(created_at)) as metric_value,
        case 
            when extract(hour from current_timestamp - max(created_at)) > 24 
            then 'WARNING' else 'PASS' 
        end as test_result,
        'Data should be fresh within 24 hours' as test_description
    from {{ model_name }}
    where created_at is not null
),

quality_summary as (
    select 
        model_name,
        count(*) as total_tests,
        count(case when test_result = 'PASS' then 1 end) as tests_passed,
        count(case when test_result = 'FAIL' then 1 end) as tests_failed,
        count(case when test_result = 'WARNING' then 1 end) as tests_warning,
        round(100.0 * count(case when test_result = 'PASS' then 1 end) / count(*), 2) as pass_rate,
        current_timestamp as test_execution_time
    from quality_tests
    group by model_name
)

select 
    qt.*,
    qs.total_tests,
    qs.tests_passed,
    qs.tests_failed,
    qs.tests_warning,
    qs.pass_rate,
    qs.test_execution_time
from quality_tests qt
join quality_summary qs on qt.model_name = qs.model_name
order by 
    case qt.test_result 
        when 'FAIL' then 1
        when 'WARNING' then 2
        else 3
    end,
    qt.test_type

{% endmacro %}


-- Advanced Schema Validation
{% macro validate_schema_evolution(model_name, expected_columns) %}

with current_schema as (
    select 
        column_name,
        data_type,
        is_nullable,
        column_default
    from information_schema.columns
    where table_name = '{{ model_name.identifier }}'
      and table_schema = '{{ model_name.schema }}'
),

expected_schema as (
    {% for column in expected_columns %}
    select 
        '{{ column.name }}' as column_name,
        '{{ column.data_type }}' as expected_data_type,
        {{ column.nullable | default(true) }} as expected_nullable
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

schema_comparison as (
    select 
        coalesce(cs.column_name, es.column_name) as column_name,
        cs.data_type as current_data_type,
        es.expected_data_type,
        cs.is_nullable as current_nullable,
        es.expected_nullable,
        
        case 
            when cs.column_name is null then 'MISSING_COLUMN'
            when es.column_name is null then 'UNEXPECTED_COLUMN'
            when cs.data_type != es.expected_data_type then 'TYPE_MISMATCH'
            when cs.is_nullable != es.expected_nullable then 'NULLABLE_MISMATCH'
            else 'MATCH'
        end as validation_result
        
    from current_schema cs
    full outer join expected_schema es on cs.column_name = es.column_name
)

select 
    '{{ model_name }}' as model_name,
    column_name,
    current_data_type,
    expected_data_type,
    current_nullable,
    expected_nullable,
    validation_result,
    case 
        when validation_result = 'MATCH' then 'PASS'
        when validation_result in ('MISSING_COLUMN', 'TYPE_MISMATCH') then 'FAIL'
        else 'WARNING'
    end as test_result,
    current_timestamp as validated_at
from schema_comparison
order by 
    case validation_result
        when 'MATCH' then 4
        when 'NULLABLE_MISMATCH' then 3
        when 'UNEXPECTED_COLUMN' then 2
        else 1
    end,
    column_name

{% endmacro %}


-- Data Lineage and Impact Analysis
{% macro analyze_data_lineage_impact(model_name) %}

with downstream_dependencies as (
    -- This would typically connect to dbt's graph structure
    -- For demonstration, using a static mapping
    select '{{ model_name }}' as source_model, 'customer_metrics_daily' as dependent_model
    union all select '{{ model_name }}', 'revenue_analytics_monthly'
    union all select '{{ model_name }}', 'cohort_analysis_weekly'
),

data_quality_impact as (
    select 
        dd.source_model,
        dd.dependent_model,
        
        -- Simulate quality score calculation
        95.5 as current_quality_score,
        92.0 as quality_threshold,
        
        case 
            when 95.5 >= 92.0 then 'LOW'
            when 95.5 >= 85.0 then 'MEDIUM'
            else 'HIGH'
        end as impact_risk_level,
        
        -- Estimated downstream record impact
        case dd.dependent_model
            when 'customer_metrics_daily' then (select count(distinct customer_id) from {{ model_name }})
            when 'revenue_analytics_monthly' then (select count(distinct date_trunc('month', invoice_date)) from {{ model_name }})
            else 0
        end as estimated_affected_records,
        
        current_timestamp as analysis_timestamp
        
    from downstream_dependencies dd
)

select 
    source_model,
    dependent_model,
    current_quality_score,
    quality_threshold,
    impact_risk_level,
    estimated_affected_records,
    
    -- Recommended actions
    case impact_risk_level
        when 'HIGH' then 'Immediate review required - downstream models may be affected'
        when 'MEDIUM' then 'Monitor closely - consider dependency refresh'
        else 'Normal monitoring sufficient'
    end as recommended_action,
    
    analysis_timestamp
    
from data_quality_impact
order by 
    case impact_risk_level
        when 'HIGH' then 1
        when 'MEDIUM' then 2
        else 3
    end,
    estimated_affected_records desc

{% endmacro %}


-- Automated Data Profiling
{% macro generate_data_profile(model_name, sample_size=10000) %}

with data_sample as (
    select * 
    from {{ model_name }}
    {% if target.type in ('snowflake', 'bigquery') %}
        sample ({{ (sample_size / 1000000.0 * 100) | round(2) }} percent)
    {% else %}
        order by random()
        limit {{ sample_size }}
    {% endif %}
),

numeric_profile as (
    select 
        'total_amount' as column_name,
        'numeric' as data_type,
        count(*) as row_count,
        count(total_amount) as non_null_count,
        count(*) - count(total_amount) as null_count,
        round(100.0 * count(total_amount) / count(*), 2) as completeness_pct,
        
        min(total_amount) as min_value,
        max(total_amount) as max_value,
        round(avg(total_amount), 2) as mean_value,
        round(stddev(total_amount), 2) as stddev_value,
        
        percentile_cont(0.25) within group (order by total_amount) as q1,
        percentile_cont(0.5) within group (order by total_amount) as median,
        percentile_cont(0.75) within group (order by total_amount) as q3,
        percentile_cont(0.95) within group (order by total_amount) as p95,
        percentile_cont(0.99) within group (order by total_amount) as p99
        
    from data_sample
    where total_amount is not null
),

categorical_profile as (
    select 
        'customer_segment' as column_name,
        'categorical' as data_type,
        count(*) as row_count,
        count(customer_segment) as non_null_count,
        count(*) - count(customer_segment) as null_count,
        round(100.0 * count(customer_segment) / count(*), 2) as completeness_pct,
        
        count(distinct customer_segment) as unique_values,
        mode() within group (order by customer_segment) as mode_value,
        
        null::numeric as min_value,
        null::numeric as max_value,
        null::numeric as mean_value,
        null::numeric as stddev_value,
        null::numeric as q1,
        null::numeric as median,
        null::numeric as q3,
        null::numeric as p95,
        null::numeric as p99
        
    from data_sample
),

date_profile as (
    select 
        'invoice_date' as column_name,
        'date' as data_type,
        count(*) as row_count,
        count(invoice_date) as non_null_count,
        count(*) - count(invoice_date) as null_count,
        round(100.0 * count(invoice_date) / count(*), 2) as completeness_pct,
        
        null::bigint as unique_values,
        null::text as mode_value,
        
        extract(epoch from min(invoice_date))::numeric as min_value,
        extract(epoch from max(invoice_date))::numeric as max_value,
        extract(epoch from avg(invoice_date))::numeric as mean_value,
        null::numeric as stddev_value,
        null::numeric as q1,
        null::numeric as median,
        null::numeric as q3,
        null::numeric as p95,
        null::numeric as p99
        
    from data_sample
    where invoice_date is not null
),

combined_profile as (
    select * from numeric_profile
    union all
    select * from categorical_profile  
    union all
    select * from date_profile
)

select 
    '{{ model_name }}' as model_name,
    cp.*,
    {{ sample_size }} as sample_size,
    current_timestamp as profiled_at
    
from combined_profile cp
order by column_name, data_type

{% endmacro %}