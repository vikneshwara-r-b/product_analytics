{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='customer_id',
    incremental_strategy = 'merge',
    post_hook = "OPTIMIZE {{ this }} ZORDER BY (customer_id)",
    partition_by = ['country']
    ) 
}}

WITH customer_base AS (
    SELECT 
        customer_id,
        email,
        full_name,
        phone,
        city,
        state,
        country,
        registration_date,
        customer_status,
        customer_segment,
        is_email_valid,
        is_phone_valid,
        is_address_complete,
        first_seen_at,
        last_updated_at
    FROM {{ ref('customers_latest') }}
    WHERE NOT is_deleted
),

-- Calculate customer metrics from order history
customer_order_metrics AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) as lifetime_orders,
        SUM(net_revenue) as lifetime_value,
        AVG(net_revenue) as average_order_value,
        MAX(order_date) as last_order_date,
        MIN(order_date) as first_order_date,
        COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN order_id END) as completed_orders,
        COUNT(DISTINCT CASE WHEN order_status = 'CANCELLED' THEN order_id END) as cancelled_orders,
        SUM(CASE WHEN order_status = 'DELIVERED' THEN net_revenue ELSE 0 END) as delivered_revenue,
        COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
        
        -- Recent activity (last 90 days)
        COUNT(DISTINCT CASE WHEN order_date >= CURRENT_DATE - 90 THEN order_id END) as orders_last_90_days,
        SUM(CASE WHEN order_date >= CURRENT_DATE - 90 THEN net_revenue ELSE 0 END) as revenue_last_90_days,
        
        -- Calculate recency, frequency, monetary for RFM analysis
        DATEDIFF(CURRENT_DATE, MAX(order_date)) as days_since_last_order,
        COUNT(DISTINCT order_id) as frequency_score,
        AVG(net_revenue) as monetary_score
        
    FROM {{ ref('orders_cleaned') }}
    WHERE customer_id IS NOT NULL
        AND order_status != 'CANCELLED'
        AND is_valid_order = TRUE
    GROUP BY customer_id
),

-- Geographic mapping for regions
geographic_mapping AS (
    SELECT 
        customer_id,
        CASE 
            WHEN country IN ('USA', 'Canada') THEN 'North America'
            WHEN country IN ('UK', 'Germany', 'France') THEN 'Europe'
            WHEN country = 'Australia' THEN 'Asia Pacific'
            ELSE 'Other'
        END as region
    FROM customer_base
),

-- Calculate customer tiers and segments
customer_segmentation AS (
    SELECT 
        c.customer_id,
        
        -- Determine customer tier based on lifetime value and order frequency
        CASE 
            WHEN om.lifetime_value >= 5000 AND om.lifetime_orders >= 20 THEN 'PLATINUM'
            WHEN om.lifetime_value >= 2000 AND om.lifetime_orders >= 10 THEN 'GOLD'
            WHEN om.lifetime_value >= 500 AND om.lifetime_orders >= 5 THEN 'SILVER'
            ELSE 'BRONZE'
        END as customer_tier,
        
        -- Enhanced customer segment
        CASE 
            WHEN om.lifetime_orders IS NULL THEN 'PROSPECT'
            WHEN om.lifetime_orders = 1 THEN 'NEW'
            WHEN om.days_since_last_order <= 30 AND om.lifetime_orders >= 5 THEN 'VIP'
            WHEN om.days_since_last_order <= 90 AND om.lifetime_orders >= 2 THEN 'REGULAR'
            WHEN om.days_since_last_order > 365 THEN 'DORMANT'
            WHEN om.days_since_last_order > 90 THEN 'AT_RISK'
            ELSE 'ACTIVE'
        END as enhanced_segment,
        
        -- Customer lifecycle stage
        CASE 
            WHEN om.lifetime_orders IS NULL THEN 'PROSPECT'
            WHEN DATEDIFF(CURRENT_DATE, c.registration_date) <= 30 THEN 'NEW_CUSTOMER'
            WHEN om.lifetime_orders = 1 AND om.days_since_last_order > 90 THEN 'ONE_TIME_BUYER'
            WHEN om.lifetime_orders >= 2 AND om.days_since_last_order <= 90 THEN 'REPEAT_CUSTOMER'
            WHEN om.lifetime_value >= 1000 AND om.active_months >= 6 THEN 'LOYAL_CUSTOMER'
            WHEN om.days_since_last_order > 365 THEN 'LOST_CUSTOMER'
            ELSE 'DEVELOPING_CUSTOMER'
        END as lifecycle_stage
        
    FROM customer_base c
    LEFT JOIN customer_order_metrics om ON c.customer_id = om.customer_id
),

-- Data quality scoring
data_quality_assessment AS (
    SELECT 
        customer_id,
        (
            CAST(is_email_valid AS INT) * 0.4 +
            CAST(is_phone_valid AS INT) * 0.3 +
            CAST(is_address_complete AS INT) * 0.3
        ) as data_quality_score
    FROM customer_base
),

-- Final dimension assembly
final_dimension AS (
    SELECT 
        c.customer_id,
        
        -- Basic customer information
        c.email,
        c.full_name,
        c.phone,
        c.city,
        c.state,
        c.country,
        gm.region,
        c.registration_date,
        c.customer_status,
        c.customer_segment,
        
        -- Enhanced segmentation
        cs.customer_tier,
        cs.enhanced_segment,
        cs.lifecycle_stage,
        
        -- Order metrics
        COALESCE(om.lifetime_orders, 0) as lifetime_orders,
        COALESCE(om.lifetime_value, 0.00) as lifetime_value,
        COALESCE(om.average_order_value, 0.00) as average_order_value,
        COALESCE(om.completed_orders, 0) as completed_orders,
        COALESCE(om.cancelled_orders, 0) as cancelled_orders,
        COALESCE(om.delivered_revenue, 0.00) as delivered_revenue,
        
        -- Behavioral metrics
        COALESCE(om.days_since_last_order, 9999) as days_since_last_order,
        CASE WHEN om.lifetime_orders > 1 THEN TRUE ELSE FALSE END as is_repeat_customer,
        COALESCE(om.orders_last_90_days, 0) as orders_last_90_days,
        COALESCE(om.revenue_last_90_days, 0.00) as revenue_last_90_days,
        COALESCE(om.active_months, 0) as active_months,
        
        -- Customer value indicators
        CASE WHEN om.lifetime_value >= 1000 THEN TRUE ELSE FALSE END as is_high_value_customer,
        CASE WHEN om.days_since_last_order <= 90 THEN TRUE ELSE FALSE END as is_active_customer,
        CASE 
            WHEN om.cancelled_orders > 0 AND om.completed_orders > 0 
            THEN ROUND(om.cancelled_orders::DECIMAL / (om.completed_orders + om.cancelled_orders), 4)
            ELSE 0.0000 
        END as cancellation_rate,
        
        -- RFM Scores (1-5 scale)
        CASE 
            WHEN om.days_since_last_order IS NULL THEN 1
            WHEN om.days_since_last_order <= 30 THEN 5
            WHEN om.days_since_last_order <= 90 THEN 4
            WHEN om.days_since_last_order <= 180 THEN 3
            WHEN om.days_since_last_order <= 365 THEN 2
            ELSE 1
        END as recency_score,
        
        CASE 
            WHEN om.lifetime_orders IS NULL THEN 1
            WHEN om.lifetime_orders >= 20 THEN 5
            WHEN om.lifetime_orders >= 10 THEN 4
            WHEN om.lifetime_orders >= 5 THEN 3
            WHEN om.lifetime_orders >= 2 THEN 2
            ELSE 1
        END as frequency_score,
        
        CASE 
            WHEN om.lifetime_value IS NULL THEN 1
            WHEN om.lifetime_value >= 2000 THEN 5
            WHEN om.lifetime_value >= 1000 THEN 4
            WHEN om.lifetime_value >= 500 THEN 3
            WHEN om.lifetime_value >= 100 THEN 2
            ELSE 1
        END as monetary_score,
        
        -- Data quality
        dqa.data_quality_score,
        c.is_email_valid,
        c.is_phone_valid,
        c.is_address_complete,
        
        -- Metadata
        c.first_seen_at as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM customer_base c
    LEFT JOIN customer_order_metrics om ON c.customer_id = om.customer_id
    LEFT JOIN geographic_mapping gm ON c.customer_id = gm.customer_id
    LEFT JOIN customer_segmentation cs ON c.customer_id = cs.customer_id
    LEFT JOIN data_quality_assessment dqa ON c.customer_id = dqa.customer_id
)

SELECT * FROM final_dimension