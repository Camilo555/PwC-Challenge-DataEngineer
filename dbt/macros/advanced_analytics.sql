

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