-- =============================================================================
-- SILVER LAYER: orders_cleaned.sql
-- =============================================================================
-- Purpose: Clean and validate order data from CDC streams with business logic
-- Strategy: SCD Type-1 - Keep only current state, comprehensive data quality
-- =============================================================================

{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns',
    incremental_strategy='merge',
    file_format='delta',
    partition_by=['order_year', 'order_month'],
    post_hook = "OPTIMIZE {{ this }} ZORDER BY (order_id, customer_id, order_date)"
) }}

WITH ranked_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        payment_status,
        payment_method,
        shipping_address_line1,
        shipping_address_line2,
        shipping_city,
        shipping_state,
        shipping_country,
        shipping_postal_code,
        shipping_method,
        estimated_delivery_date,
        actual_delivery_date,
        order_total,
        tax_amount,
        shipping_cost,
        discount_amount,
        region,
        _cdc_operation,
        _cdc_timestamp,
        _ingested_at,
        _source_system,
        _batch_id,
        
        -- Rank by CDC timestamp to get latest version of each order
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY _cdc_timestamp DESC, _ingested_at DESC
        ) as rn
        
    FROM {{ source('bronze','orders_cdc') }}
    {% if is_incremental() %}
        -- Only process new or updated records since last run
        WHERE _cdc_timestamp > (
            SELECT COALESCE(MAX(last_updated_at), "{{ var('ingest_batch_date') }}"::timestamp) 
            FROM {{ this }}
        )
    {% endif %}
),

latest_orders AS (
    SELECT *
    FROM ranked_orders
    WHERE rn = 1  -- Get only the latest version of each order
      AND _cdc_operation != 'DELETE'  -- Exclude deleted orders
),

-- Data cleaning and standardization
cleaned_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        
        -- Standardize status values
        CASE 
            WHEN UPPER(TRIM(order_status)) IN ('PENDING', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED') 
            THEN UPPER(TRIM(order_status))
            WHEN UPPER(TRIM(order_status)) IN ('PROCESSING', 'CONFIRMED') THEN 'CONFIRMED'
            WHEN UPPER(TRIM(order_status)) IN ('IN_TRANSIT', 'OUT_FOR_DELIVERY') THEN 'SHIPPED'
            WHEN UPPER(TRIM(order_status)) IN ('COMPLETED', 'FULFILLED') THEN 'DELIVERED'
            WHEN UPPER(TRIM(order_status)) IN ('CANCELED', 'VOID', 'REJECTED') THEN 'CANCELLED'
            ELSE 'PENDING'
        END as order_status,
        
        CASE 
            WHEN UPPER(TRIM(payment_status)) IN ('PENDING', 'PAID', 'FAILED', 'REFUNDED') 
            THEN UPPER(TRIM(payment_status))
            WHEN UPPER(TRIM(payment_status)) IN ('AUTHORIZED', 'CAPTURED', 'COMPLETED') THEN 'PAID'
            WHEN UPPER(TRIM(payment_status)) IN ('DECLINED', 'EXPIRED', 'CANCELLED') THEN 'FAILED'
            WHEN UPPER(TRIM(payment_status)) IN ('REVERSED', 'CHARGEBACK') THEN 'REFUNDED'
            ELSE 'PENDING'
        END as payment_status,
        
        CASE 
            WHEN UPPER(TRIM(payment_method)) IN ('CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER', 'DIGITAL_WALLET') 
            THEN UPPER(TRIM(payment_method))
            WHEN UPPER(TRIM(payment_method)) IN ('VISA', 'MASTERCARD', 'AMEX', 'DISCOVER') THEN 'CREDIT_CARD'
            WHEN UPPER(TRIM(payment_method)) IN ('APPLE_PAY', 'GOOGLE_PAY', 'SAMSUNG_PAY') THEN 'DIGITAL_WALLET'
            WHEN UPPER(TRIM(payment_method)) IN ('ACH', 'WIRE_TRANSFER', 'DIRECT_DEBIT') THEN 'BANK_TRANSFER'
            ELSE 'CREDIT_CARD'
        END as payment_method,
        
        -- Clean shipping address fields
        TRIM(shipping_address_line1) as shipping_address_line1,
        TRIM(shipping_address_line2) as shipping_address_line2,
        TRIM(shipping_city) as shipping_city,
        TRIM(shipping_state) as shipping_state,
        UPPER(TRIM(shipping_country)) as shipping_country,
        TRIM(shipping_postal_code) as shipping_postal_code,
        
        CASE 
            WHEN UPPER(TRIM(shipping_method)) IN ('STANDARD', 'EXPRESS', 'OVERNIGHT', 'ECONOMY') 
            THEN UPPER(TRIM(shipping_method))
            WHEN UPPER(TRIM(shipping_method)) IN ('NEXT_DAY', 'ONE_DAY') THEN 'OVERNIGHT'
            WHEN UPPER(TRIM(shipping_method)) IN ('TWO_DAY', 'PRIORITY') THEN 'EXPRESS'
            WHEN UPPER(TRIM(shipping_method)) IN ('GROUND', 'REGULAR') THEN 'STANDARD'
            WHEN UPPER(TRIM(shipping_method)) IN ('SLOW', 'SAVER') THEN 'ECONOMY'
            ELSE 'STANDARD'
        END as shipping_method,
        
        estimated_delivery_date,
        actual_delivery_date,
        
        -- Financial fields validation and cleaning
        CASE 
            WHEN order_total IS NULL OR order_total < 0 THEN 0.00
            WHEN order_total > 50000 THEN 50000.00  -- Cap at reasonable maximum
            ELSE ROUND(order_total, 2)
        END as order_total,
        
        CASE 
            WHEN tax_amount IS NULL OR tax_amount < 0 THEN 0.00
            WHEN tax_amount > order_total * 0.2 THEN order_total * 0.2  -- Cap tax at 20%
            ELSE ROUND(tax_amount, 2)
        END as tax_amount,
        
        CASE 
            WHEN shipping_cost IS NULL OR shipping_cost < 0 THEN 0.00
            WHEN shipping_cost > 200 THEN 200.00  -- Cap shipping at reasonable maximum
            ELSE ROUND(shipping_cost, 2)
        END as shipping_cost,
        
        CASE 
            WHEN discount_amount IS NULL OR discount_amount < 0 THEN 0.00
            WHEN discount_amount > order_total THEN order_total  -- Discount can't exceed order total
            ELSE ROUND(discount_amount, 2)
        END as discount_amount,
        
        CASE 
            WHEN UPPER(TRIM(region)) IN ('NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL') 
            THEN UPPER(TRIM(region))
            -- Map some common variations
            WHEN UPPER(TRIM(region)) IN ('NORTHEAST', 'NE') THEN 'NORTH'
            WHEN UPPER(TRIM(region)) IN ('SOUTHEAST', 'SE') THEN 'SOUTH'
            WHEN UPPER(TRIM(region)) IN ('NORTHWEST', 'NW') THEN 'WEST'
            WHEN UPPER(TRIM(region)) IN ('SOUTHWEST', 'SW') THEN 'WEST'
            WHEN UPPER(TRIM(region)) IN ('MIDWEST', 'MW') THEN 'CENTRAL'
            ELSE 'CENTRAL'  -- Default region
        END as region,
        
        -- Metadata
        _cdc_operation,
        _cdc_timestamp,
        _ingested_at,
        _source_system,
        _batch_id
        
    FROM latest_orders
),

-- Calculate derived business metrics
enriched_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        payment_status,
        payment_method,
        shipping_address_line1,
        shipping_address_line2,
        shipping_city,
        shipping_state,
        shipping_country,
        shipping_postal_code,
        shipping_method,
        estimated_delivery_date,
        actual_delivery_date,
        order_total,
        tax_amount,
        shipping_cost,
        discount_amount,
        region,
        
        -- Calculate net revenue (order total minus tax)
        ROUND(order_total - tax_amount, 2) as net_revenue,
        
        -- Extract date components for partitioning and analysis
        YEAR(order_date) as order_year,
        MONTH(order_date) as order_month,
        DAY(order_date) as order_day,
        HOUR(order_date) as order_hour,
        DAYOFWEEK(order_date) as day_of_week_num,
        
        CASE DAYOFWEEK(order_date)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END as day_of_week,
        
        -- Weekend flag
        CASE 
            WHEN DAYOFWEEK(order_date) IN (1, 7) THEN TRUE 
            ELSE FALSE 
        END as is_weekend,
        
        -- Calculate delivery metrics
        CASE 
            WHEN actual_delivery_date IS NOT NULL AND order_date IS NOT NULL
            THEN DATEDIFF(actual_delivery_date, DATE(order_date))
            ELSE NULL
        END as delivery_days,
        
        -- On-time delivery flag
        CASE 
            WHEN actual_delivery_date IS NULL THEN NULL
            WHEN estimated_delivery_date IS NULL THEN NULL
            WHEN actual_delivery_date <= estimated_delivery_date THEN TRUE
            ELSE FALSE
        END as is_delivered_on_time,
        
        -- Address completeness flag
        CASE 
            WHEN shipping_address_line1 IS NOT NULL AND shipping_address_line1 != ''
                 AND shipping_city IS NOT NULL AND shipping_city != ''
                 AND shipping_state IS NOT NULL AND shipping_state != ''
                 AND shipping_country IS NOT NULL AND shipping_country != ''
                 AND shipping_postal_code IS NOT NULL AND shipping_postal_code != ''
            THEN TRUE 
            ELSE FALSE 
        END as has_complete_address,
        
        -- Business validation flags
        CASE 
            WHEN order_total > 0 
                 AND customer_id IS NOT NULL 
                 AND order_date IS NOT NULL
                 AND order_status IS NOT NULL
                 AND payment_status IS NOT NULL
                 -- Additional business rules
                 AND order_total >= (tax_amount + shipping_cost - discount_amount)
                 AND discount_amount <= order_total
            THEN TRUE 
            ELSE FALSE 
        END as is_valid_order,
        
        -- Metadata
        _cdc_operation,
        _cdc_timestamp,
        _ingested_at,
        _source_system,
        _batch_id
        
    FROM cleaned_orders
),

-- Add customer validation and business context
final_orders AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.payment_status,
        o.payment_method,
        o.shipping_address_line1,
        o.shipping_address_line2,
        o.shipping_city,
        o.shipping_state,
        o.shipping_country,
        o.shipping_postal_code,
        o.shipping_method,
        o.estimated_delivery_date,
        o.actual_delivery_date,
        o.order_total,
        o.tax_amount,
        o.shipping_cost,
        o.discount_amount,
        o.net_revenue,
        o.region,
        o.order_year,
        o.order_month,
        o.order_day,
        o.order_hour,
        o.day_of_week,
        o.is_weekend,
        o.delivery_days,
        o.is_delivered_on_time,
        o.has_complete_address,
        
        -- Enhanced validation with customer existence check
        CASE 
            WHEN o.is_valid_order = TRUE 
                 AND c.customer_id IS NOT NULL  -- Customer exists in silver layer
                 AND c.is_deleted = FALSE       -- Customer is active
            THEN TRUE 
            ELSE FALSE 
        END as is_valid_order,
        
        -- Additional business flags
        CASE WHEN o.discount_amount > 0 THEN TRUE ELSE FALSE END as has_discount,
        CASE WHEN o.shipping_country != 'USA' THEN TRUE ELSE FALSE END as is_international,
        CASE WHEN o.order_total >= 100 THEN TRUE ELSE FALSE END as is_large_order,
        CASE WHEN o.shipping_cost = 0 THEN TRUE ELSE FALSE END as is_free_shipping,
        
        -- Calculate order value categories
        CASE 
            WHEN o.order_total >= 500 THEN 'HIGH'
            WHEN o.order_total >= 100 THEN 'MEDIUM'
            WHEN o.order_total >= 25 THEN 'LOW'
            ELSE 'VERY_LOW'
        END as order_value_category,
        
        -- Seasonal indicators
        CASE 
            WHEN o.order_month IN (12, 1, 2) THEN 'WINTER'
            WHEN o.order_month IN (3, 4, 5) THEN 'SPRING'
            WHEN o.order_month IN (6, 7, 8) THEN 'SUMMER'
            WHEN o.order_month IN (9, 10, 11) THEN 'FALL'
        END as season,
        
        -- Holiday periods (approximate)
        CASE 
            WHEN o.order_month = 11 AND o.order_day >= 20 THEN 'BLACK_FRIDAY'
            WHEN o.order_month = 12 AND o.order_day <= 25 THEN 'CHRISTMAS'
            WHEN o.order_month = 12 AND o.order_day >= 26 THEN 'YEAR_END'
            WHEN o.order_month = 1 AND o.order_day <= 7 THEN 'NEW_YEAR'
            WHEN o.order_month = 2 AND o.order_day = 14 THEN 'VALENTINES'
            WHEN o.order_month = 5 AND o.order_day >= 8 AND o.order_day <= 14 THEN 'MOTHERS_DAY'
            ELSE 'REGULAR'
        END as holiday_period,
        
        -- Metadata
        {% if is_incremental() %}
            CASE 
                WHEN o._cdc_operation = 'INSERT' THEN o._cdc_timestamp
                ELSE COALESCE(
                    (SELECT MAX(first_seen_at) FROM {{ this }} WHERE order_id = o.order_id),
                    o._cdc_timestamp
                )
            END as first_seen_at,
        {%- else -%}
            o._cdc_timestamp as first_seen_at,  
        {%- endif -%}
        
        o._cdc_timestamp as last_updated_at,
        
        -- Soft delete flag
        CASE WHEN o._cdc_operation = 'DELETE' THEN TRUE ELSE FALSE END as is_deleted,
        
        -- Source metadata
        o._source_system,
        o._batch_id
        
    FROM enriched_orders o
    LEFT JOIN {{ ref('customers_latest') }} c 
        ON o.customer_id = c.customer_id
)

SELECT * FROM final_orders