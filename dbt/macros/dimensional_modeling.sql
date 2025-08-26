

{% macro calculate_customer_lifetime_value(customer_table, transaction_table, prediction_months=12) %}

{%- set prediction_days = prediction_months * 30 -%}

with customer_metrics as (
    select 
        c.customer_id,
        c.first_purchase_date,
        c.last_purchase_date,
        c.total_orders,
        c.total_spent,
        c.avg_order_value,
        
        -- Calculate customer lifetime in days
        current_date - c.first_purchase_date as lifetime_days,
        
        -- Calculate purchase frequency (orders per day)
        case 
            when current_date - c.first_purchase_date > 0 then 
                c.total_orders::float / (current_date - c.first_purchase_date)
            else 0
        end as purchase_frequency_per_day,
        
        -- Calculate churn probability based on recency
        case 
            when current_date - c.last_purchase_date <= 30 then 0.1
            when current_date - c.last_purchase_date <= 90 then 0.3
            when current_date - c.last_purchase_date <= 180 then 0.6
            when current_date - c.last_purchase_date <= 365 then 0.8
            else 0.95
        end as churn_probability
        
    from {{ customer_table }} c
    where c.total_orders > 0
),

clv_calculation as (
    select 
        customer_id,
        total_spent as historical_value,
        
        -- Predicted CLV using simplified formula:
        -- CLV = (Average Order Value × Purchase Frequency × Gross Margin × Lifespan) × (1 - Churn Rate)
        round(
            (
                avg_order_value * 
                (purchase_frequency_per_day * {{ prediction_days }}) *
                0.3 * -- Assumed 30% gross margin
                (1 - churn_probability)
            ), 2
        ) as predicted_clv,
        
        -- Total CLV (historical + predicted)
        round(
            total_spent + 
            (
                avg_order_value * 
                (purchase_frequency_per_day * {{ prediction_days }}) *
                0.3 * 
                (1 - churn_probability)
            ), 2
        ) as total_clv,
        
        -- CLV segments
        case 
            when round(
                total_spent + 
                (
                    avg_order_value * 
                    (purchase_frequency_per_day * {{ prediction_days }}) *
                    0.3 * 
                    (1 - churn_probability)
                ), 2
            ) >= 1000 then 'High Value'
            when round(
                total_spent + 
                (
                    avg_order_value * 
                    (purchase_frequency_per_day * {{ prediction_days }}) *
                    0.3 * 
                    (1 - churn_probability)
                ), 2
            ) >= 500 then 'Medium Value'
            when round(
                total_spent + 
                (
                    avg_order_value * 
                    (purchase_frequency_per_day * {{ prediction_days }}) *
                    0.3 * 
                    (1 - churn_probability)
                ), 2
            ) >= 100 then 'Low Value'
            else 'Minimal Value'
        end as clv_segment,
        
        purchase_frequency_per_day,
        churn_probability,
        lifetime_days,
        
        current_timestamp as calculated_at
        
    from customer_metrics
)

select * from clv_calculation

{% endmacro %}