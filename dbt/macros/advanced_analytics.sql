

/*
Advanced Analytics Macros for PwC Data Engineering Platform
Comprehensive set of analytical transformation macros including:
- Customer Churn Prediction
- RFM Analysis
- Cohort Analysis
- Time Series Analytics
- Statistical Functions
*/

-- RFM Analysis Macro for Customer Segmentation
{% macro calculate_rfm_scores(table_name, customer_col, date_col, amount_col, analysis_date=None) %}
  {% if analysis_date is none %}
    {% set analysis_date = "CURRENT_DATE" %}
  {% else %}
    {% set analysis_date = "'" ~ analysis_date ~ "'::date" %}
  {% endif %}

  WITH customer_metrics AS (
    SELECT 
      {{ customer_col }} as customer_id,
      -- Recency: Days since last purchase
      {{ analysis_date }} - MAX({{ date_col }}::date) as recency_days,
      -- Frequency: Number of transactions
      COUNT(*) as frequency,
      -- Monetary: Total amount spent
      SUM({{ amount_col }}) as monetary
    FROM {{ table_name }}
    WHERE {{ customer_col }} IS NOT NULL
    GROUP BY {{ customer_col }}
  ),
  
  rfm_percentiles AS (
    SELECT 
      customer_id,
      recency_days,
      frequency,
      monetary,
      -- Calculate percentile ranks for scoring
      PERCENT_RANK() OVER (ORDER BY recency_days DESC) as recency_percentile,
      PERCENT_RANK() OVER (ORDER BY frequency ASC) as frequency_percentile,
      PERCENT_RANK() OVER (ORDER BY monetary ASC) as monetary_percentile
    FROM customer_metrics
  )
  
  SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    -- RFM Scores (1-5 scale)
    CASE 
      WHEN recency_percentile >= 0.8 THEN 5
      WHEN recency_percentile >= 0.6 THEN 4
      WHEN recency_percentile >= 0.4 THEN 3
      WHEN recency_percentile >= 0.2 THEN 2
      ELSE 1
    END as recency_score,
    
    CASE 
      WHEN frequency_percentile >= 0.8 THEN 5
      WHEN frequency_percentile >= 0.6 THEN 4
      WHEN frequency_percentile >= 0.4 THEN 3
      WHEN frequency_percentile >= 0.2 THEN 2
      ELSE 1
    END as frequency_score,
    
    CASE 
      WHEN monetary_percentile >= 0.8 THEN 5
      WHEN monetary_percentile >= 0.6 THEN 4
      WHEN monetary_percentile >= 0.4 THEN 3
      WHEN monetary_percentile >= 0.2 THEN 2
      ELSE 1
    END as monetary_score
  FROM rfm_percentiles

{% endmacro %}


-- Customer Churn Prediction with Advanced Risk Scoring
{% macro customer_churn_prediction(customer_table, transaction_table, customer_id_col, transaction_date_col, revenue_col, churn_days=90) %}

with customer_activity as (
    select 
        c.{{ customer_id_col }},
        
        -- Customer attributes
        c.first_purchase_date,
        c.total_orders,
        c.total_spent,
        
        -- Recent activity
        max(t.{{ transaction_date_col }}) as last_transaction_date,
        current_date - max(t.{{ transaction_date_col }}) as days_since_last_transaction,
        
        -- Activity patterns (last 90 days)
        sum(case 
            when t.{{ transaction_date_col }} >= current_date - {{ churn_days }} 
            then t.{{ revenue_col }} 
            else 0 
        end) as recent_revenue,
        
        count(case 
            when t.{{ transaction_date_col }} >= current_date - {{ churn_days }} 
            then 1 
        end) as recent_transactions,
        
        -- Historical patterns
        avg(t.{{ revenue_col }}) as avg_transaction_value,
        
        -- Purchase frequency (transactions per month)
        count(t.transaction_id) * 30.0 / 
        greatest(current_date - c.first_purchase_date, 1) as purchase_frequency_monthly,
        
        -- Trend analysis (comparing last 90 days to previous 90 days)
        sum(case 
            when t.{{ transaction_date_col }} between 
                current_date - {{ churn_days * 2 }} and current_date - {{ churn_days }}
            then t.{{ revenue_col }} 
            else 0 
        end) as previous_period_revenue
        
    from {{ customer_table }} c
    left join {{ transaction_table }} t
        on c.{{ customer_id_col }} = t.{{ customer_id_col }}
    group by 
        c.{{ customer_id_col }}, c.first_purchase_date, 
        c.total_orders, c.total_spent
),

churn_risk_scoring as (
    select 
        *,
        
        -- Recency risk (0-5 scale)
        case 
            when days_since_last_transaction <= 7 then 0
            when days_since_last_transaction <= 30 then 1
            when days_since_last_transaction <= 60 then 2
            when days_since_last_transaction <= 90 then 3
            when days_since_last_transaction <= 180 then 4
            else 5
        end as recency_risk,
        
        -- Frequency risk (0-5 scale)
        case 
            when purchase_frequency_monthly >= 2 then 0
            when purchase_frequency_monthly >= 1 then 1
            when purchase_frequency_monthly >= 0.5 then 2
            when purchase_frequency_monthly >= 0.25 then 3
            when purchase_frequency_monthly >= 0.1 then 4
            else 5
        end as frequency_risk,
        
        -- Revenue trend risk (0-5 scale)
        case 
            when recent_revenue > previous_period_revenue * 1.2 then 0
            when recent_revenue > previous_period_revenue * 0.8 then 1
            when recent_revenue > previous_period_revenue * 0.5 then 2
            when recent_revenue > previous_period_revenue * 0.2 then 3
            when recent_revenue > 0 then 4
            else 5
        end as revenue_trend_risk,
        
        -- Activity risk (0-5 scale)
        case 
            when recent_transactions >= 5 then 0
            when recent_transactions >= 3 then 1
            when recent_transactions >= 2 then 2
            when recent_transactions >= 1 then 3
            when recent_transactions = 0 and days_since_last_transaction <= 180 then 4
            else 5
        end as activity_risk
        
    from customer_activity
),

churn_prediction as (
    select 
        *,
        
        -- Composite churn risk score (0-20 scale)
        recency_risk + frequency_risk + revenue_trend_risk + activity_risk as churn_risk_score,
        
        -- Churn probability (simplified model)
        case 
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 4 then 0.1
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 8 then 0.3
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 12 then 0.6
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 16 then 0.8
            else 0.95
        end as churn_probability,
        
        -- Churn risk category
        case 
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 4 then 'Low Risk'
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 8 then 'Medium Risk'
            when recency_risk + frequency_risk + revenue_trend_risk + activity_risk <= 12 then 'High Risk'
            else 'Critical Risk'
        end as churn_risk_category,
        
        -- Recommended actions
        case 
            when recency_risk >= 4 then 'Re-engagement Campaign'
            when frequency_risk >= 4 then 'Increase Purchase Frequency'
            when revenue_trend_risk >= 4 then 'Value Recovery Campaign'
            when activity_risk >= 4 then 'Activity Stimulation'
            else 'Monitor'
        end as recommended_action,
        
        current_timestamp as calculated_at
        
    from churn_risk_scoring
)

select * from churn_prediction
order by churn_risk_score desc, total_spent desc

{% endmacro %}


-- Customer Segmentation Based on RFM Scores
{% macro rfm_segmentation(rfm_table) %}
  
  SELECT 
    *,
    CASE 
      WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
      WHEN recency_score >= 3 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Loyal Customers'
      WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 4 THEN 'Potential Loyalists'
      WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
      WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Promising'
      WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Need Attention'
      WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 4 THEN 'About to Sleep'
      WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'At Risk'
      WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score <= 2 THEN 'Cannot Lose Them'
      WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score >= 2 THEN 'Hibernating'
      ELSE 'Lost'
    END as rfm_segment,
    
    -- Composite RFM Score
    CONCAT(recency_score::text, frequency_score::text, monetary_score::text) as rfm_score
    
  FROM {{ rfm_table }}

{% endmacro %}


-- Cohort Analysis for Customer Retention
{% macro cohort_analysis(table_name, customer_col, date_col, cohort_type='monthly') %}

  {% if cohort_type == 'monthly' %}
    {% set date_trunc = "DATE_TRUNC('month', " ~ date_col ~ ")" %}
  {% elif cohort_type == 'weekly' %}
    {% set date_trunc = "DATE_TRUNC('week', " ~ date_col ~ ")" %}
  {% else %}
    {% set date_trunc = "DATE_TRUNC('day', " ~ date_col ~ ")" %}
  {% endif %}

  WITH customer_cohorts AS (
    SELECT 
      {{ customer_col }} as customer_id,
      {{ date_trunc }} as cohort_month,
      {{ date_trunc }} as transaction_month,
      MIN({{ date_trunc }}) OVER (PARTITION BY {{ customer_col }}) as first_transaction_month
    FROM {{ table_name }}
    WHERE {{ customer_col }} IS NOT NULL
  ),
  
  cohort_table AS (
    SELECT 
      first_transaction_month as cohort_month,
      transaction_month,
      COUNT(DISTINCT customer_id) as customers,
      -- Calculate period number (0 for first month, 1 for second, etc.)
      EXTRACT(MONTH FROM AGE(transaction_month, first_transaction_month)) as period_number
    FROM customer_cohorts
    GROUP BY first_transaction_month, transaction_month
  ),
  
  cohort_sizes AS (
    SELECT 
      cohort_month,
      COUNT(DISTINCT customer_id) as cohort_size
    FROM customer_cohorts
    WHERE transaction_month = first_transaction_month
    GROUP BY cohort_month
  )
  
  SELECT 
    ct.cohort_month,
    ct.period_number,
    ct.customers,
    cs.cohort_size,
    ROUND(ct.customers * 100.0 / cs.cohort_size, 2) as retention_rate
  FROM cohort_table ct
  JOIN cohort_sizes cs ON ct.cohort_month = cs.cohort_month
  ORDER BY ct.cohort_month, ct.period_number

{% endmacro %}


-- Time Series Analysis with Moving Averages and Trends
{% macro time_series_analysis(table_name, date_col, value_col, window_size=7) %}

  SELECT 
    {{ date_col }} as analysis_date,
    {{ value_col }} as value,
    
    -- Moving averages
    AVG({{ value_col }}) OVER (
      ORDER BY {{ date_col }} 
      ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
    ) as moving_avg_{{ window_size }}d,
    
    -- Growth rates
    LAG({{ value_col }}, 1) OVER (ORDER BY {{ date_col }}) as prev_value,
    LAG({{ value_col }}, {{ window_size }}) OVER (ORDER BY {{ date_col }}) as value_{{ window_size }}d_ago,
    
    -- Calculate percentage changes
    CASE 
      WHEN LAG({{ value_col }}, 1) OVER (ORDER BY {{ date_col }}) > 0 THEN
        ({{ value_col }} - LAG({{ value_col }}, 1) OVER (ORDER BY {{ date_col }})) * 100.0 / 
        LAG({{ value_col }}, 1) OVER (ORDER BY {{ date_col }})
      ELSE NULL
    END as day_over_day_pct_change,
    
    CASE 
      WHEN LAG({{ value_col }}, {{ window_size }}) OVER (ORDER BY {{ date_col }}) > 0 THEN
        ({{ value_col }} - LAG({{ value_col }}, {{ window_size }}) OVER (ORDER BY {{ date_col }})) * 100.0 / 
        LAG({{ value_col }}, {{ window_size }}) OVER (ORDER BY {{ date_col }})
      ELSE NULL
    END as period_over_period_pct_change,
    
    -- Trend indicators
    CASE 
      WHEN {{ value_col }} > LAG({{ value_col }}, 1) OVER (ORDER BY {{ date_col }}) THEN 'UP'
      WHEN {{ value_col }} < LAG({{ value_col }}, 1) OVER (ORDER BY {{ date_col }}) THEN 'DOWN'
      ELSE 'FLAT'
    END as trend_direction
    
  FROM {{ table_name }}
  ORDER BY {{ date_col }}

{% endmacro %}


-- Statistical Outlier Detection
{% macro detect_outliers(table_name, column_name, method='iqr', threshold=1.5) %}

  {% if method == 'iqr' %}
    WITH percentiles AS (
      SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {{ column_name }}) as q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ column_name }}) as q3
      FROM {{ table_name }}
      WHERE {{ column_name }} IS NOT NULL
    ),
    
    outlier_bounds AS (
      SELECT 
        q1,
        q3,
        q3 - q1 as iqr,
        q1 - ({{ threshold }} * (q3 - q1)) as lower_bound,
        q3 + ({{ threshold }} * (q3 - q1)) as upper_bound
      FROM percentiles
    )
    
    SELECT 
      t.*,
      CASE 
        WHEN t.{{ column_name }} < ob.lower_bound OR t.{{ column_name }} > ob.upper_bound 
        THEN true 
        ELSE false 
      END as is_outlier,
      ob.lower_bound,
      ob.upper_bound
    FROM {{ table_name }} t
    CROSS JOIN outlier_bounds ob
    WHERE t.{{ column_name }} IS NOT NULL
    
  {% elif method == 'zscore' %}
    WITH stats AS (
      SELECT 
        AVG({{ column_name }}) as mean_val,
        STDDEV({{ column_name }}) as stddev_val
      FROM {{ table_name }}
      WHERE {{ column_name }} IS NOT NULL
    )
    
    SELECT 
      t.*,
      ABS((t.{{ column_name }} - s.mean_val) / s.stddev_val) as z_score,
      CASE 
        WHEN ABS((t.{{ column_name }} - s.mean_val) / s.stddev_val) > {{ threshold }}
        THEN true 
        ELSE false 
      END as is_outlier
    FROM {{ table_name }} t
    CROSS JOIN stats s
    WHERE t.{{ column_name }} IS NOT NULL
    
  {% endif %}

{% endmacro %}


-- Advanced Customer Lifetime Value (CLV) Calculation with Machine Learning Features
{% macro calculate_customer_ltv(transaction_table, customer_col, date_col, revenue_col, prediction_months=12) %}

WITH customer_metrics AS (
    SELECT 
        {{ customer_col }} as customer_id,
        MIN({{ date_col }}) as first_purchase_date,
        MAX({{ date_col }}) as last_purchase_date,
        COUNT(*) as total_transactions,
        SUM({{ revenue_col }}) as total_revenue,
        AVG({{ revenue_col }}) as avg_order_value,
        
        -- Calculate customer tenure in months
        EXTRACT(MONTH FROM AGE(MAX({{ date_col }}), MIN({{ date_col }}))) + 1 as tenure_months,
        
        -- Purchase frequency (transactions per month)
        COUNT(*) * 1.0 / NULLIF(EXTRACT(MONTH FROM AGE(MAX({{ date_col }}), MIN({{ date_col }}))) + 1, 0) as purchase_frequency,
        
        -- Revenue per month
        SUM({{ revenue_col }}) * 1.0 / NULLIF(EXTRACT(MONTH FROM AGE(MAX({{ date_col }}), MIN({{ date_col }}))) + 1, 0) as monthly_revenue,
        
        -- Days since last purchase
        EXTRACT(DAY FROM CURRENT_DATE - MAX({{ date_col }})) as days_since_last_purchase,
        
        -- Standard deviation of order values (purchase consistency)
        STDDEV({{ revenue_col }}) as order_value_stddev
        
    FROM {{ transaction_table }}
    WHERE {{ customer_col }} IS NOT NULL
      AND {{ revenue_col }} > 0
    GROUP BY {{ customer_col }}
),

ltv_calculation AS (
    SELECT 
        customer_id,
        first_purchase_date,
        last_purchase_date,
        total_transactions,
        total_revenue,
        avg_order_value,
        tenure_months,
        purchase_frequency,
        monthly_revenue,
        days_since_last_purchase,
        order_value_stddev,
        
        -- Predicted customer lifetime (based on purchase frequency and recency)
        CASE 
            WHEN days_since_last_purchase <= 30 THEN tenure_months + {{ prediction_months }}
            WHEN days_since_last_purchase <= 90 THEN tenure_months + ({{ prediction_months }} * 0.7)
            WHEN days_since_last_purchase <= 180 THEN tenure_months + ({{ prediction_months }} * 0.4)
            ELSE tenure_months + ({{ prediction_months }} * 0.2)
        END as predicted_lifetime_months,
        
        -- Churn probability based on recency and frequency
        CASE 
            WHEN days_since_last_purchase > 365 THEN 0.9
            WHEN days_since_last_purchase > 180 THEN 0.7
            WHEN days_since_last_purchase > 90 THEN 0.5
            WHEN days_since_last_purchase > 30 THEN 0.2
            ELSE 0.1
        END as churn_probability
        
    FROM customer_metrics
),

ltv_final AS (
    SELECT 
        *,
        
        -- Historical CLV (actual value to date)
        total_revenue as historical_clv,
        
        -- Predicted CLV using different methods
        -- Method 1: Simple extrapolation
        monthly_revenue * predicted_lifetime_months * (1 - churn_probability) as predicted_clv_simple,
        
        -- Method 2: Frequency-based prediction
        avg_order_value * purchase_frequency * predicted_lifetime_months * (1 - churn_probability) as predicted_clv_frequency,
        
        -- Method 3: Cohort-adjusted prediction (conservative)
        CASE 
            WHEN tenure_months <= 3 THEN monthly_revenue * 6 * (1 - churn_probability)
            WHEN tenure_months <= 12 THEN monthly_revenue * (predicted_lifetime_months * 0.8) * (1 - churn_probability)
            ELSE monthly_revenue * predicted_lifetime_months * (1 - churn_probability)
        END as predicted_clv_conservative,
        
        -- Customer value tier
        CASE 
            WHEN total_revenue >= 5000 THEN 'VIP'
            WHEN total_revenue >= 2000 THEN 'High Value'
            WHEN total_revenue >= 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_tier,
        
        -- Purchase behavior classification
        CASE 
            WHEN purchase_frequency >= 2 AND avg_order_value >= 100 THEN 'High Frequency, High Value'
            WHEN purchase_frequency >= 2 THEN 'High Frequency, Low Value'
            WHEN avg_order_value >= 100 THEN 'Low Frequency, High Value'
            ELSE 'Low Frequency, Low Value'
        END as purchase_behavior,
        
        -- Customer lifecycle stage
        CASE 
            WHEN days_since_last_purchase <= 30 THEN 'Active'
            WHEN days_since_last_purchase <= 90 THEN 'At Risk'
            WHEN days_since_last_purchase <= 180 THEN 'Dormant'
            ELSE 'Lost'
        END as lifecycle_stage,
        
        current_timestamp as calculated_at
        
    FROM ltv_calculation
)

SELECT * FROM ltv_final
ORDER BY predicted_clv_frequency DESC

{% endmacro %}


-- Advanced Market Basket Analysis with Association Rules
{% macro market_basket_analysis(transaction_table, transaction_id_col, product_col, min_support=0.01, min_confidence=0.5) %}

WITH transaction_items AS (
    SELECT 
        {{ transaction_id_col }} as transaction_id,
        {{ product_col }} as product_id,
        1 as item_count
    FROM {{ transaction_table }}
    WHERE {{ transaction_id_col }} IS NOT NULL
      AND {{ product_col }} IS NOT NULL
),

total_transactions AS (
    SELECT COUNT(DISTINCT transaction_id) as total_txn_count
    FROM transaction_items
),

item_support AS (
    SELECT 
        product_id,
        COUNT(DISTINCT transaction_id) as item_txn_count,
        COUNT(DISTINCT transaction_id) * 1.0 / (SELECT total_txn_count FROM total_transactions) as support
    FROM transaction_items
    GROUP BY product_id
    HAVING COUNT(DISTINCT transaction_id) * 1.0 / (SELECT total_txn_count FROM total_transactions) >= {{ min_support }}
),

item_pairs AS (
    SELECT 
        t1.transaction_id,
        t1.product_id as product_a,
        t2.product_id as product_b
    FROM transaction_items t1
    INNER JOIN transaction_items t2
        ON t1.transaction_id = t2.transaction_id
        AND t1.product_id < t2.product_id  -- Avoid duplicates and self-pairs
    INNER JOIN item_support s1 ON t1.product_id = s1.product_id
    INNER JOIN item_support s2 ON t2.product_id = s2.product_id
),

pair_statistics AS (
    SELECT 
        product_a,
        product_b,
        COUNT(*) as pair_count,
        COUNT(*) * 1.0 / (SELECT total_txn_count FROM total_transactions) as pair_support,
        
        -- Get individual item supports for confidence calculation
        MAX(CASE WHEN s.product_id = ip.product_a THEN s.support END) as support_a,
        MAX(CASE WHEN s.product_id = ip.product_b THEN s.support END) as support_b
        
    FROM item_pairs ip
    CROSS JOIN item_support s
    WHERE s.product_id IN (ip.product_a, ip.product_b)
    GROUP BY product_a, product_b
    HAVING COUNT(*) * 1.0 / (SELECT total_txn_count FROM total_transactions) >= {{ min_support }}
),

association_rules AS (
    SELECT 
        product_a,
        product_b,
        pair_count,
        pair_support,
        support_a,
        support_b,
        
        -- Confidence: P(B|A) = P(A,B) / P(A)
        pair_support / NULLIF(support_a, 0) as confidence_a_to_b,
        pair_support / NULLIF(support_b, 0) as confidence_b_to_a,
        
        -- Lift: P(A,B) / (P(A) * P(B))
        pair_support / NULLIF(support_a * support_b, 0) as lift,
        
        -- Conviction: (1 - P(B)) / (1 - P(B|A))
        (1 - support_b) / NULLIF(1 - (pair_support / NULLIF(support_a, 0)), 0) as conviction_a_to_b,
        (1 - support_a) / NULLIF(1 - (pair_support / NULLIF(support_b, 0)), 0) as conviction_b_to_a
        
    FROM pair_statistics
),

final_rules AS (
    SELECT 
        *,
        -- Rule strength interpretation
        CASE 
            WHEN lift > 3 AND confidence_a_to_b > 0.8 THEN 'Very Strong'
            WHEN lift > 2 AND confidence_a_to_b > 0.6 THEN 'Strong'
            WHEN lift > 1.5 AND confidence_a_to_b > 0.4 THEN 'Moderate'
            WHEN lift > 1 THEN 'Weak'
            ELSE 'Negative'
        END as rule_strength,
        
        -- Business recommendation
        CASE 
            WHEN lift > 2 AND confidence_a_to_b > {{ min_confidence }} THEN 'Cross-sell Opportunity'
            WHEN lift > 1.5 AND confidence_a_to_b > {{ min_confidence }} THEN 'Bundle Opportunity'
            WHEN lift > 1 THEN 'Promotional Opportunity'
            ELSE 'No Action'
        END as business_recommendation,
        
        current_timestamp as analyzed_at
        
    FROM association_rules
    WHERE confidence_a_to_b >= {{ min_confidence }}
       OR confidence_b_to_a >= {{ min_confidence }}
)

SELECT * FROM final_rules
ORDER BY lift DESC, confidence_a_to_b DESC
LIMIT 100

{% endmacro %}


-- Advanced Seasonality and Trend Analysis
{% macro seasonality_trend_analysis(table_name, date_col, value_col, seasonality_type='monthly') %}

{% if seasonality_type == 'monthly' %}
    {% set date_part = 'EXTRACT(MONTH FROM ' ~ date_col ~ ')' %}
    {% set period_name = 'month' %}
{% elif seasonality_type == 'weekly' %}
    {% set date_part = 'EXTRACT(WEEK FROM ' ~ date_col ~ ')' %}
    {% set period_name = 'week' %}
{% elif seasonality_type == 'daily' %}
    {% set date_part = 'EXTRACT(DOW FROM ' ~ date_col ~ ')' %}
    {% set period_name = 'day_of_week' %}
{% else %}
    {% set date_part = 'EXTRACT(QUARTER FROM ' ~ date_col ~ ')' %}
    {% set period_name = 'quarter' %}
{% endif %}

WITH time_series_data AS (
    SELECT 
        {{ date_col }}::date as analysis_date,
        {{ date_part }} as period_part,
        EXTRACT(YEAR FROM {{ date_col }}) as year_part,
        SUM({{ value_col }}) as period_value,
        COUNT(*) as period_count
    FROM {{ table_name }}
    WHERE {{ date_col }} IS NOT NULL
      AND {{ value_col }} IS NOT NULL
    GROUP BY {{ date_col }}::date, {{ date_part }}, EXTRACT(YEAR FROM {{ date_col }})
),

seasonal_patterns AS (
    SELECT 
        period_part,
        COUNT(*) as period_occurrences,
        AVG(period_value) as avg_value,
        STDDEV(period_value) as stddev_value,
        MIN(period_value) as min_value,
        MAX(period_value) as max_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY period_value) as median_value
    FROM time_series_data
    GROUP BY period_part
),

trend_analysis AS (
    SELECT 
        *,
        -- Moving averages
        AVG(period_value) OVER (
            ORDER BY analysis_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as moving_avg_7,
        
        AVG(period_value) OVER (
            ORDER BY analysis_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as moving_avg_30,
        
        -- Trend direction
        period_value - LAG(period_value, 1) OVER (ORDER BY analysis_date) as day_over_day_change,
        
        period_value - LAG(period_value, 7) OVER (ORDER BY analysis_date) as week_over_week_change,
        
        period_value - LAG(period_value, 30) OVER (ORDER BY analysis_date) as month_over_month_change,
        
        -- Year-over-year comparison (same period, previous year)
        period_value - LAG(period_value) OVER (
            PARTITION BY period_part 
            ORDER BY year_part
        ) as year_over_year_change,
        
        -- Seasonal index (compared to overall average)
        period_value / AVG(period_value) OVER () as seasonal_index
        
    FROM time_series_data
),

anomaly_detection AS (
    SELECT 
        ta.*,
        sp.avg_value as seasonal_avg,
        sp.stddev_value as seasonal_stddev,
        
        -- Z-score for anomaly detection
        CASE 
            WHEN sp.stddev_value > 0 THEN
                ABS(ta.period_value - sp.avg_value) / sp.stddev_value
            ELSE 0
        END as seasonal_z_score,
        
        -- Anomaly flag
        CASE 
            WHEN sp.stddev_value > 0 AND ABS(ta.period_value - sp.avg_value) / sp.stddev_value > 2 THEN true
            ELSE false
        END as is_anomaly,
        
        -- Trend classification
        CASE 
            WHEN moving_avg_30 > LAG(moving_avg_30, 7) OVER (ORDER BY analysis_date) THEN 'Upward'
            WHEN moving_avg_30 < LAG(moving_avg_30, 7) OVER (ORDER BY analysis_date) THEN 'Downward'
            ELSE 'Stable'
        END as trend_direction,
        
        current_timestamp as analyzed_at
        
    FROM trend_analysis ta
    JOIN seasonal_patterns sp ON ta.period_part = sp.period_part
)

SELECT * FROM anomaly_detection
ORDER BY analysis_date DESC

{% endmacro %}


-- Advanced Customer Segmentation with RFM and Behavioral Patterns
{% macro advanced_customer_segmentation(customer_table, transaction_table, customer_col, date_col, revenue_col) %}

WITH customer_rfm AS (
    {{ calculate_rfm_scores(transaction_table, customer_col, date_col, revenue_col) }}
),

behavioral_metrics AS (
    SELECT 
        {{ customer_col }} as customer_id,
        COUNT(DISTINCT EXTRACT(MONTH FROM {{ date_col }})) as active_months,
        COUNT(DISTINCT {{ date_col }}::date) as active_days,
        
        -- Purchase patterns
        STDDEV({{ revenue_col }}) as order_value_variance,
        COUNT(DISTINCT CASE WHEN EXTRACT(DOW FROM {{ date_col }}) IN (6, 0) THEN {{ date_col }}::date END) as weekend_purchases,
        COUNT(DISTINCT CASE WHEN EXTRACT(DOW FROM {{ date_col }}) IN (1,2,3,4,5) THEN {{ date_col }}::date END) as weekday_purchases,
        
        -- Seasonal behavior
        COUNT(CASE WHEN EXTRACT(MONTH FROM {{ date_col }}) IN (11, 12) THEN 1 END) as holiday_purchases,
        COUNT(CASE WHEN EXTRACT(MONTH FROM {{ date_col }}) IN (6, 7, 8) THEN 1 END) as summer_purchases,
        
        -- Product diversity
        COUNT(DISTINCT product_category) as category_diversity,
        MAX({{ date_col }}) as last_purchase_date,
        MIN({{ date_col }}) as first_purchase_date
        
    FROM {{ transaction_table }}
    WHERE {{ customer_col }} IS NOT NULL
    GROUP BY {{ customer_col }}
),

advanced_segmentation AS (
    SELECT 
        rfm.*,
        bm.active_months,
        bm.active_days,
        bm.order_value_variance,
        bm.weekend_purchases,
        bm.weekday_purchases,
        bm.holiday_purchases,
        bm.summer_purchases,
        bm.category_diversity,
        bm.last_purchase_date,
        bm.first_purchase_date,
        
        -- Calculate engagement score
        CASE 
            WHEN bm.active_months >= 6 AND rfm.frequency >= 10 THEN 5
            WHEN bm.active_months >= 3 AND rfm.frequency >= 5 THEN 4
            WHEN bm.active_months >= 2 AND rfm.frequency >= 3 THEN 3
            WHEN bm.active_months >= 1 AND rfm.frequency >= 2 THEN 2
            ELSE 1
        END as engagement_score,
        
        -- Purchase timing preference
        CASE 
            WHEN bm.weekend_purchases > bm.weekday_purchases THEN 'Weekend Shopper'
            WHEN bm.weekday_purchases > bm.weekend_purchases * 2 THEN 'Weekday Shopper'
            ELSE 'Flexible Shopper'
        END as timing_preference,
        
        -- Seasonal preference
        CASE 
            WHEN bm.holiday_purchases >= 3 THEN 'Holiday Shopper'
            WHEN bm.summer_purchases >= 3 THEN 'Summer Shopper'
            ELSE 'Year-Round Shopper'
        END as seasonal_preference,
        
        -- Product exploration behavior
        CASE 
            WHEN bm.category_diversity >= 5 THEN 'Explorer'
            WHEN bm.category_diversity >= 3 THEN 'Moderate Explorer'
            ELSE 'Focused Buyer'
        END as exploration_behavior
        
    FROM customer_rfm rfm
    LEFT JOIN behavioral_metrics bm ON rfm.customer_id = bm.customer_id
),

final_segmentation AS (
    SELECT 
        *,
        -- Composite customer score
        (recency_score + frequency_score + monetary_score + engagement_score) / 4.0 as composite_score,
        
        -- Advanced customer segments
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 AND engagement_score >= 4 THEN 'VIP Champions'
            WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score >= 4 THEN 'Loyal Customers'
            WHEN recency_score >= 3 AND engagement_score >= 3 AND monetary_score >= 3 THEN 'Potential Loyalists'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk Customers'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cannot Lose Them'
            WHEN recency_score <= 2 AND engagement_score <= 2 THEN 'Lost Customers'
            ELSE 'Others'
        END as advanced_segment,
        
        -- Marketing strategy recommendation
        CASE 
            WHEN recency_score >= 4 AND monetary_score >= 4 THEN 'Upsell Premium Products'
            WHEN recency_score >= 3 AND frequency_score <= 2 THEN 'Increase Purchase Frequency'
            WHEN recency_score <= 2 AND monetary_score >= 3 THEN 'Win-Back Campaign'
            WHEN engagement_score <= 2 THEN 'Re-engagement Campaign'
            ELSE 'Standard Marketing'
        END as marketing_strategy,
        
        current_timestamp as segmented_at
        
    FROM advanced_segmentation
)

SELECT * FROM final_segmentation
ORDER BY composite_score DESC, monetary DESC

{% endmacro %}