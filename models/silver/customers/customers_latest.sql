{{ 
    config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    on_schema_change = 'sync_all_columns',
    incremental_strategy = 'merge',
    post_hook = "OPTIMIZE {{ this }} ZORDER BY (customer_id)"
    )
}} 

WITH ranked_customers AS (
    SELECT
        customer_id,
        email,
        first_name,
        last_name,
        phone,
        address_line1,
        address_line2,
        city,
        state,
        country,
        postal_code,
        registration_date,
        customer_status,
        customer_segment,
        _cdc_operation,
        _cdc_timestamp,
        _ingested_at,
        _source_system,
        _batch_id,
        -- Rank by CDC timestamp to get latest version
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY
                _cdc_timestamp DESC,
                _ingested_at DESC
        ) as rn
    FROM
        {{ source('bronze','customers_cdc') }}
    {% if is_incremental() %} -- Only process new or updated records since last run
    WHERE
        _cdc_timestamp > (
            SELECT
                COALESCE(MAX(last_updated_at), var('ingest_batch_date') :: timestamp)
            FROM
                {{ this }}
        ) 
    {% endif %}
),
latest_customers AS (
    SELECT
        customer_id,
        email,
        first_name,
        last_name,
        phone,
        address_line1,
        address_line2,
        city,
        state,
        country,
        postal_code,
        registration_date,
        customer_status,
        customer_segment,
        _cdc_operation,
        _cdc_timestamp,
        _ingested_at,
        _source_system,
        _batch_id
    FROM
        ranked_customers
    WHERE
        rn = 1 -- Get only the latest version of each customer
),
transformed_customers AS (
    SELECT
        customer_id,
        -- Basic customer information
        LOWER(TRIM(email)) as email,
        TRIM(first_name) as first_name,
        TRIM(last_name) as last_name,
        CONCAT(TRIM(first_name), ' ', TRIM(last_name)) as full_name,
        TRIM(phone) as phone,
        -- Address information
        TRIM(address_line1) as address_line1,
        TRIM(address_line2) as address_line2,
        TRIM(city) as city,
        TRIM(state) as state,
        UPPER(TRIM(country)) as country,
        TRIM(postal_code) as postal_code,
        -- Dates and status
        registration_date,
        UPPER(customer_status) as customer_status,
        UPPER(customer_segment) as customer_segment,
        -- Data Quality Flags
        CASE
            WHEN email IS NULL
            OR email = ''
            OR NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN FALSE
            ELSE TRUE
        END as is_email_valid,
        CASE
            WHEN phone IS NULL
            OR phone = ''
            OR phone = '000-000-0000'
            OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) < 10 THEN FALSE
            ELSE TRUE
        END as is_phone_valid,
        CASE
            WHEN address_line1 IS NOT NULL
            AND address_line1 != ''
            AND city IS NOT NULL
            AND city != ''
            AND state IS NOT NULL
            AND state != ''
            AND postal_code IS NOT NULL
            AND postal_code != '' THEN TRUE
            ELSE FALSE
        END as is_address_complete,
        -- Metadata
        {% if is_incremental() %}
            CASE
                WHEN _cdc_operation = 'INSERT' THEN _cdc_timestamp
                ELSE COALESCE(
                    (
                        SELECT
                            first_seen_at
                        FROM
                            {{ this }}
                        WHERE
                            customer_id = latest_customers.customer_id
                    ),
                    _cdc_timestamp
                )
            END as first_seen_at,
        {%- else -%}
            _cdc_timestamp as first_seen_at,  
        {%- endif -%}
        _cdc_timestamp as last_updated_at,
        -- Soft delete flag
        CASE
            WHEN _cdc_operation = 'DELETE' THEN TRUE
            ELSE FALSE
        END as is_deleted,
        -- Source metadata
        _source_system,
        _batch_id
    FROM
        latest_customers
    WHERE
        _cdc_operation != 'DELETE' -- Exclude deleted records from active dataset
)
SELECT
    *
FROM
    transformed_customers