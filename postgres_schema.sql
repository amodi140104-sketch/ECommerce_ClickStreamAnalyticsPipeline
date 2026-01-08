-- E-Commerce Analytics Database Schema
-- Run this to set up PostgreSQL tables

-- ============================================
-- RAW EVENTS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    user_name VARCHAR(100),
    user_segment VARCHAR(50),
    device VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    
    -- Product fields (nullable)
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    product_price DECIMAL(10, 2),
    product_category VARCHAR(50),
    
    -- Purchase fields (nullable)
    order_id VARCHAR(50),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    
    -- Session fields (nullable)
    session_duration_seconds INTEGER,
    exit_reason VARCHAR(50),
    
    -- Metadata
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS idx_timestamp ON raw_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_event_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_user_id ON raw_events(user_id);
CREATE INDEX IF NOT EXISTS idx_session_id ON raw_events(session_id);
CREATE INDEX IF NOT EXISTS idx_product_id ON raw_events(product_id);

-- ============================================
-- DAILY USER METRICS
-- ============================================
CREATE TABLE IF NOT EXISTS daily_user_metrics (
    metric_date DATE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    
    -- Activity metrics
    session_count INTEGER DEFAULT 0,
    total_events INTEGER DEFAULT 0,
    product_views INTEGER DEFAULT 0,
    cart_adds INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    
    -- Time metrics
    total_session_duration_seconds INTEGER DEFAULT 0,
    avg_session_duration_seconds DECIMAL(10, 2),
    
    -- Revenue metrics
    total_revenue DECIMAL(10, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS idx_user_metrics_date ON daily_user_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_user_metrics_user ON daily_user_metrics(user_id);

-- ============================================
-- DAILY PRODUCT METRICS
-- ============================================
CREATE TABLE IF NOT EXISTS daily_product_metrics (
    metric_date DATE NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200),
    product_category VARCHAR(50),
    
    -- View metrics
    unique_viewers INTEGER DEFAULT 0,
    total_views INTEGER DEFAULT 0,
    
    -- Conversion metrics
    cart_adds INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    units_sold INTEGER DEFAULT 0,
    
    -- Revenue metrics
    total_revenue DECIMAL(10, 2) DEFAULT 0,
    avg_price DECIMAL(10, 2),
    
    -- Calculated metrics
    view_to_cart_rate DECIMAL(5, 2),
    cart_to_purchase_rate DECIMAL(5, 2),
    overall_conversion_rate DECIMAL(5, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS idx_product_metrics_date ON daily_product_metrics(metric_date);
CREATE INDEX IF NOT EXISTS idx_product_category ON daily_product_metrics(product_category);

-- ============================================
-- USER COHORTS
-- ============================================
CREATE TABLE IF NOT EXISTS user_cohorts (
    cohort_date DATE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    
    -- First purchase info
    first_purchase_date DATE,
    first_purchase_amount DECIMAL(10, 2),
    
    -- Lifetime metrics
    total_purchases INTEGER DEFAULT 0,
    lifetime_revenue DECIMAL(10, 2) DEFAULT 0,
    days_since_first_purchase INTEGER,
    
    -- Engagement
    total_sessions INTEGER DEFAULT 0,
    last_session_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS idx_cohort_date ON user_cohorts(cohort_date);
CREATE INDEX IF NOT EXISTS idx_first_purchase_date ON user_cohorts(first_purchase_date);

-- ============================================
-- CONVERSION FUNNEL (Daily Snapshots)
-- ============================================
CREATE TABLE IF NOT EXISTS daily_conversion_funnel (
    metric_date DATE PRIMARY KEY,
    
    -- Funnel stages
    total_sessions INTEGER DEFAULT 0,
    product_views INTEGER DEFAULT 0,
    cart_adds INTEGER DEFAULT 0,
    checkouts_started INTEGER DEFAULT 0,
    purchases_completed INTEGER DEFAULT 0,
    
    -- Conversion rates
    view_rate DECIMAL(5, 2),
    cart_rate DECIMAL(5, 2),
    checkout_rate DECIMAL(5, 2),
    purchase_rate DECIMAL(5, 2),
    
    -- Drop-off rates
    view_to_cart_drop DECIMAL(5, 2),
    cart_to_checkout_drop DECIMAL(5, 2),
    checkout_to_purchase_drop DECIMAL(5, 2),
    
    -- Revenue
    total_revenue DECIMAL(12, 2),
    avg_order_value DECIMAL(10, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- HOURLY METRICS (for trend analysis)
-- ============================================
CREATE TABLE IF NOT EXISTS hourly_metrics (
    metric_timestamp TIMESTAMP NOT NULL,
    
    -- Volume metrics
    total_events INTEGER DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    total_sessions INTEGER DEFAULT 0,
    
    -- Event breakdown
    product_views INTEGER DEFAULT 0,
    cart_adds INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    
    -- Revenue
    total_revenue DECIMAL(10, 2) DEFAULT 0,
    
    -- Performance
    avg_events_per_second DECIMAL(8, 2)
);

-- Create index separately
CREATE INDEX IF NOT EXISTS idx_hourly_metrics_timestamp ON hourly_metrics(metric_timestamp);

-- ============================================
-- TOP PRODUCTS (Daily Rankings)
-- ============================================
CREATE TABLE IF NOT EXISTS top_products_daily (
    metric_date DATE NOT NULL,
    rank INTEGER NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200),
    product_category VARCHAR(50),
    
    -- Metrics
    views INTEGER DEFAULT 0,
    purchases INTEGER DEFAULT 0,
    revenue DECIMAL(10, 2) DEFAULT 0
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS idx_top_products_date ON top_products_daily(metric_date);
CREATE INDEX IF NOT EXISTS idx_top_products_id ON top_products_daily(product_id);

-- ============================================
-- VIEWS FOR EASY QUERYING
-- ============================================

-- Today's metrics summary
CREATE OR REPLACE VIEW v_today_summary AS
SELECT 
    COUNT(DISTINCT user_id) as active_users,
    COUNT(DISTINCT session_id) as total_sessions,
    COUNT(*) as total_events,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    SUM(CASE WHEN event_type = 'purchase' THEN total_amount ELSE 0 END) as revenue
FROM raw_events
WHERE DATE(timestamp) = CURRENT_DATE;

-- Last 7 days trend
CREATE OR REPLACE VIEW v_last_7_days_trend AS
SELECT 
    DATE(timestamp) as date,
    COUNT(DISTINCT user_id) as users,
    COUNT(DISTINCT session_id) as sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    SUM(CASE WHEN event_type = 'purchase' THEN total_amount ELSE 0 END) as revenue
FROM raw_events
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(timestamp)
ORDER BY date;

-- Top products (all time)
CREATE OR REPLACE VIEW v_top_products AS
SELECT 
    product_name,
    product_category,
    COUNT(*) as views,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    SUM(CASE WHEN event_type = 'purchase' THEN total_amount ELSE 0 END) as revenue
FROM raw_events
WHERE product_name IS NOT NULL
GROUP BY product_name, product_category
ORDER BY views DESC
LIMIT 20;

-- ============================================
-- FUNCTIONS
-- ============================================

-- Calculate conversion rate
CREATE OR REPLACE FUNCTION calculate_conversion_rate(
    p_date DATE
) RETURNS TABLE (
    stage VARCHAR(50),
    count INTEGER,
    conversion_rate DECIMAL(5, 2)
) AS $$
BEGIN
    RETURN QUERY
    WITH funnel AS (
        SELECT 
            SUM(CASE WHEN event_type = 'session_start' THEN 1 ELSE 0 END) as sessions,
            SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) as views,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as carts,
            SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END) as checkouts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
        FROM raw_events
        WHERE DATE(timestamp) = p_date
    )
    SELECT 'Sessions'::VARCHAR(50), sessions::INTEGER, 100.00::DECIMAL(5,2) FROM funnel
    UNION ALL
    SELECT 'Views', views::INTEGER, (views::DECIMAL / NULLIF(sessions, 0) * 100)::DECIMAL(5,2) FROM funnel
    UNION ALL
    SELECT 'Carts', carts::INTEGER, (carts::DECIMAL / NULLIF(sessions, 0) * 100)::DECIMAL(5,2) FROM funnel
    UNION ALL
    SELECT 'Checkouts', checkouts::INTEGER, (checkouts::DECIMAL / NULLIF(sessions, 0) * 100)::DECIMAL(5,2) FROM funnel
    UNION ALL
    SELECT 'Purchases', purchases::INTEGER, (purchases::DECIMAL / NULLIF(sessions, 0) * 100)::DECIMAL(5,2) FROM funnel;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- SAMPLE QUERIES
-- ============================================

-- Get today's summary
-- SELECT * FROM v_today_summary;

-- Get conversion funnel for today
-- SELECT * FROM calculate_conversion_rate(CURRENT_DATE);

-- Top 10 users by revenue
-- SELECT user_id, user_name, SUM(total_amount) as revenue
-- FROM raw_events
-- WHERE event_type = 'purchase'
-- GROUP BY user_id, user_name
-- ORDER BY revenue DESC
-- LIMIT 10;

-- Hourly traffic pattern
-- SELECT 
--     EXTRACT(HOUR FROM timestamp) as hour,
--     COUNT(*) as events,
--     COUNT(DISTINCT user_id) as users
-- FROM raw_events
-- WHERE DATE(timestamp) = CURRENT_DATE
-- GROUP BY EXTRACT(HOUR FROM timestamp)
-- ORDER BY hour;