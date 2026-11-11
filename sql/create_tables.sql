
-- =====================================================
-- WORLD HAPPINESS - POSTGRESQL DATA WAREHOUSE
-- =====================================================

-- 1. COUNTRY DIMENSION
CREATE TABLE IF NOT EXISTS dim_country (
    country_id BIGSERIAL PRIMARY KEY,
    country_name VARCHAR(150) NOT NULL UNIQUE
);

-- 2. DATE DIMENSION
-- Date represents the event emission date.
CREATE TABLE IF NOT EXISTS dim_date (
    date_id BIGSERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL
);

-- 3. RAW EVENTS TABLE
-- Stores the original Kafka message as received.
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id BIGSERIAL PRIMARY KEY,

    event_id TEXT UNIQUE,
    raw_message TEXT NOT NULL,

    country VARCHAR(150),
    observation_year INTEGER,
    event_time TIMESTAMPTZ,

    processing_status VARCHAR(30) NOT NULL DEFAULT 'PROCESSING',
    validation_errors JSONB,

    kafka_topic VARCHAR(150) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_processing_status CHECK (
    processing_status IN (
        'PROCESSING',
        'VALID',
        'INVALID_SCHEMA',
        'INVALID_VALUES',
        'PREDICTION_ERROR'
    )
),

    CONSTRAINT uq_kafka_message UNIQUE (
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
);

-- 4. FACT PREDICTIONS TABLE
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,

    raw_event_id BIGINT NOT NULL UNIQUE,
    country_id BIGINT NOT NULL,
    date_id BIGINT NOT NULL,

    actual_score DOUBLE PRECISION NOT NULL,
    predicted_score DOUBLE PRECISION NOT NULL,
    prediction_error DOUBLE PRECISION NOT NULL,

    prediction_timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_prediction_raw
        FOREIGN KEY (raw_event_id)
        REFERENCES raw_happiness_events(raw_event_id),

    CONSTRAINT fk_prediction_country
        FOREIGN KEY (country_id)
        REFERENCES dim_country(country_id),

    CONSTRAINT fk_prediction_date
        FOREIGN KEY (date_id)
        REFERENCES dim_date(date_id)
);

-- 5. INDEXES
CREATE INDEX IF NOT EXISTS idx_raw_status
    ON raw_happiness_events(processing_status);

CREATE INDEX IF NOT EXISTS idx_raw_country
    ON raw_happiness_events(country);

CREATE INDEX IF NOT EXISTS idx_predictions_country
    ON fact_predictions(country_id);

CREATE INDEX IF NOT EXISTS idx_predictions_date
    ON fact_predictions(date_id);
