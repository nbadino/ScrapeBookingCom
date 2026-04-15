-- Enable PostGIS if needed (optional, but good for locations)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Layer Raw: HTML/JSON grezzi + log scraping
CREATE TABLE scrape_runs (
    scrape_run_id      BIGSERIAL PRIMARY KEY,
    started_at         TIMESTAMPTZ NOT NULL,
    finished_at        TIMESTAMPTZ,
    target_site        TEXT NOT NULL DEFAULT 'booking.com',
    notes              TEXT,
    status             TEXT CHECK (status IN ('running','success','partial','failed'))
);

CREATE TABLE raw_responses (
    raw_response_id   BIGSERIAL PRIMARY KEY,
    scrape_run_id     BIGINT REFERENCES scrape_runs(scrape_run_id),
    url               TEXT NOT NULL,
    response_body     BYTEA,       -- o TEXT se HTML non troppo grande
    status_code       INTEGER,
    created_at        TIMESTAMPTZ NOT NULL,
    parse_success     BOOLEAN,
    parse_error       TEXT
);

CREATE TABLE app_jobs (
    job_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    options JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL CHECK (status IN ('waiting', 'delayed', 'active', 'completed', 'failed', 'cancelled')),
    progress JSONB,
    return_value JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_on TIMESTAMPTZ,
    finished_on TIMESTAMPTZ,
    cancellation_requested BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_app_jobs_claim ON app_jobs (status, run_at, created_at);

-- Layer Core: Schema relazionale normalizzato

-- Locations
CREATE TABLE countries (
    country_code CHAR(2) PRIMARY KEY,
    country_name TEXT
);

CREATE TABLE cities (
    city_id      BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    country_code CHAR(2) REFERENCES countries(country_code),
    nuts_code    TEXT,  -- NUTS (Nomenclature of Territorial Units for Statistics) code
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION
);

-- Hotels (Master & Versioned)
CREATE TABLE hotels (
    hotel_sk         BIGSERIAL PRIMARY KEY,
    booking_hotel_id BIGINT NOT NULL,
    first_seen_at    TIMESTAMPTZ NOT NULL,
    last_seen_at     TIMESTAMPTZ,
    url              TEXT,
    CONSTRAINT uq_booking_hotel UNIQUE (booking_hotel_id)
);

CREATE TABLE hotel_versions (
    hotel_version_id BIGSERIAL PRIMARY KEY,
    hotel_sk         BIGINT REFERENCES hotels(hotel_sk),
    valid_from       TIMESTAMPTZ NOT NULL,
    valid_to         TIMESTAMPTZ,
    name             TEXT,
    stars            NUMERIC(2,1),
    city_id          BIGINT REFERENCES cities(city_id),
    address          TEXT,
    zipcode          TEXT,
    lat              DOUBLE PRECISION,
    lon              DOUBLE PRECISION,
    squares          NUMERIC(2,1), -- rating squares for apartments
    property_type    TEXT,         -- hotel, b&b, apartment...
    amenities        JSONB,        -- wifi, parking, spa...
    description      TEXT,         -- hotel description
    images           JSONB,        -- array of image URLs
    raw_attributes   JSONB,        -- tutto quello che non sai dove mettere
    nuts3_code       TEXT,
    lau_code         TEXT
);

-- Room Types (Master & Versioned)
CREATE TABLE room_types (
    room_type_sk       BIGSERIAL PRIMARY KEY,
    hotel_sk           BIGINT REFERENCES hotels(hotel_sk),
    booking_room_id    BIGINT,       -- se esiste
    first_seen_at      TIMESTAMPTZ NOT NULL,
    last_seen_at       TIMESTAMPTZ
);

CREATE TABLE room_type_versions (
    room_type_version_id BIGSERIAL PRIMARY KEY,
    room_type_sk         BIGINT REFERENCES room_types(room_type_sk),
    valid_from           TIMESTAMPTZ NOT NULL,
    valid_to             TIMESTAMPTZ,
    name                 TEXT,
    description          TEXT,
    max_occupancy        SMALLINT,
    size_sqm             NUMERIC(5,2),
    bed_configuration    JSONB,      -- letti singoli, matrimoniali ecc
    view_type            TEXT,       -- mare, città…
    smoking_allowed      BOOLEAN,
    raw_attributes       JSONB
);

-- Rate Plans (Master & Versioned)
CREATE TABLE rate_plans (
    rate_plan_id     BIGSERIAL PRIMARY KEY,
    hotel_sk         BIGINT REFERENCES hotels(hotel_sk),
    booking_rate_id  BIGINT,     -- se Booking espone ID
    first_seen_at    TIMESTAMPTZ NOT NULL,
    last_seen_at     TIMESTAMPTZ,
    name             TEXT,       -- es. "Tariffa Non Rimborsabile"
    meal_plan        TEXT,       -- 'RO','BB','HB','FB','AI'
    payment_type     TEXT,       -- 'prepaid','pay_at_property'
    raw_attributes   JSONB
);

CREATE TABLE rate_plan_versions (
    rate_plan_version_id BIGSERIAL PRIMARY KEY,
    rate_plan_id         BIGINT REFERENCES rate_plans(rate_plan_id),
    valid_from           TIMESTAMPTZ NOT NULL,
    valid_to             TIMESTAMPTZ,
    cancellation_policy  JSONB,   -- JSON con finestra free cancel, penalty, ecc.
    other_conditions     JSONB
);

-- Italy administrative geodata imported from ISTAT 2026
CREATE TABLE nuts_regions (
    nuts_id TEXT PRIMARY KEY,
    level_code SMALLINT NOT NULL, -- 1=ripartizioni, 2=regioni, 3=province/CM
    nuts_name TEXT NOT NULL,
    country_code CHAR(2) NOT NULL DEFAULT 'IT',
    geom geometry(MultiPolygon, 4326),
    centroid_lat DOUBLE PRECISION,
    centroid_lon DOUBLE PRECISION,
    parent_code TEXT,
    area_km2 DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE lau_regions (
    lau_id TEXT PRIMARY KEY,
    lau_name TEXT NOT NULL,
    country_code CHAR(2) NOT NULL DEFAULT 'IT',
    nuts3_code TEXT,
    geom geometry(MultiPolygon, 4326),
    centroid_lat DOUBLE PRECISION,
    centroid_lon DOUBLE PRECISION,
    population INTEGER,
    area_km2 DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Searches (Experiments)
CREATE TABLE searches (
    search_id          BIGSERIAL PRIMARY KEY,
    scrape_run_id      BIGINT REFERENCES scrape_runs(scrape_run_id),
    monitor_run_date_id BIGINT,
    search_timestamp   TIMESTAMPTZ NOT NULL,
    checkin_date       DATE NOT NULL,
    length_of_stay     SMALLINT NOT NULL,
    occupancy_adults   SMALLINT NOT NULL,
    occupancy_children SMALLINT NOT NULL,
    rooms              SMALLINT NOT NULL DEFAULT 1,
    children_ages      JSONB,      -- opzionale
    currency           CHAR(3) NOT NULL,
    locale             TEXT,       -- es. 'it-IT'
    device_type        TEXT,       -- 'desktop','mobile'
    market_country     CHAR(2),    -- es. 'IT' (dove fai la ricerca)
    request_hash       TEXT,       -- per identificare richieste identiche
    UNIQUE (monitor_run_date_id)
);

-- Hotel Search Results (Visibility)
CREATE TABLE hotel_search_results (
    hotel_search_result_id BIGSERIAL PRIMARY KEY,
    search_id              BIGINT REFERENCES searches(search_id),
    hotel_sk               BIGINT REFERENCES hotels(hotel_sk),
    position_in_list       INTEGER,    -- ranking nei risultati
    is_available           BOOLEAN,    -- se compare almeno un'offerta prenotabile
    is_sold_out            BOOLEAN,    -- compare ma tutto sold out
    is_unlisted            BOOLEAN DEFAULT false,
    distance_center_km     NUMERIC(6,3),
    review_score           NUMERIC(3,1),
    review_count           INTEGER,
    booking_paga           BOOLEAN,
    scraped_at             TIMESTAMPTZ,
    claimed_at             TIMESTAMPTZ,
    claimed_by             TEXT,
    claim_expires_at       TIMESTAMPTZ,
    attempt_count          INTEGER NOT NULL DEFAULT 0,
    last_error             TEXT,
    last_error_at          TIMESTAMPTZ,
    raw_snippet            JSONB,      -- blocco riassuntivo (etichetta "scelta intelligente" ecc.)
    UNIQUE (search_id, hotel_sk)
);

-- Offers (Facts)
CREATE TABLE offers (
    offer_id              BIGSERIAL PRIMARY KEY,
    search_id             BIGINT REFERENCES searches(search_id),
    hotel_sk              BIGINT REFERENCES hotels(hotel_sk),
    room_type_sk          BIGINT REFERENCES room_types(room_type_sk),
    rate_plan_id          BIGINT REFERENCES rate_plans(rate_plan_id),

    checkin_date          DATE NOT NULL,
    length_of_stay        SMALLINT NOT NULL,

    currency              CHAR(3) NOT NULL,
    price_total           NUMERIC(12,2) NOT NULL,
    price_original_total  NUMERIC(12,2),      -- crossed-out se esiste
    price_per_night_avg   NUMERIC(12,2),      -- comodo per analisi

    rooms_left            SMALLINT,           -- "ultima camera" ecc
    is_refundable         BOOLEAN,
    free_cancellation_until TIMESTAMPTZ,
    breakfast_included    BOOLEAN,

    created_at            TIMESTAMPTZ NOT NULL,  -- timestamp scraping
    raw_conditions        JSONB,                 -- testo libero, etichette ecc.

    UNIQUE (search_id, hotel_sk, room_type_sk, rate_plan_id, price_total, currency)
);

-- Nightly Prices (Optional Breakdown)
CREATE TABLE offer_nightly_prices (
    nightly_price_id  BIGSERIAL PRIMARY KEY,
    offer_id          BIGINT REFERENCES offers(offer_id),
    date              DATE NOT NULL,
    price_nightly     NUMERIC(12,2) NOT NULL,
    currency          CHAR(3) NOT NULL,
    CONSTRAINT uq_offer_date UNIQUE (offer_id, date)
);

-- Time-series schema used by the Go scraper.
CREATE TABLE search_definitions (
    search_def_id BIGSERIAL PRIMARY KEY,
    checkin_date DATE NOT NULL,
    length_of_stay SMALLINT NOT NULL,
    occupancy_adults SMALLINT NOT NULL,
    occupancy_children SMALLINT NOT NULL,
    rooms SMALLINT NOT NULL DEFAULT 1,
    children_ages JSONB,
    currency CHAR(3) NOT NULL DEFAULT 'EUR',
    locale TEXT DEFAULT 'it-IT',
    device_type TEXT DEFAULT 'desktop',
    market_country CHAR(2) DEFAULT 'IT',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    CONSTRAINT uq_search_params UNIQUE (
        checkin_date, length_of_stay, occupancy_adults,
        occupancy_children, rooms, currency, locale,
        children_ages, device_type, market_country
    )
);

CREATE TABLE search_runs (
    search_run_id BIGSERIAL PRIMARY KEY,
    search_def_id BIGINT NOT NULL REFERENCES search_definitions(search_def_id),
    scrape_run_id BIGINT REFERENCES scrape_runs(scrape_run_id),
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_results INTEGER,
    total_available INTEGER,
    total_sold_out INTEGER,
    request_hash TEXT,
    CONSTRAINT uq_search_run_time UNIQUE (search_def_id, run_timestamp)
);

CREATE TABLE hotel_availability_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    search_run_id BIGINT NOT NULL REFERENCES search_runs(search_run_id) ON DELETE CASCADE,
    hotel_sk BIGINT NOT NULL REFERENCES hotels(hotel_sk),
    position_in_list INTEGER,
    is_available BOOLEAN NOT NULL DEFAULT true,
    is_sold_out BOOLEAN DEFAULT false,
    is_unlisted BOOLEAN DEFAULT false,
    review_score NUMERIC(3,1),
    review_count INTEGER,
    distance_center_km NUMERIC(6,3),
    booking_paga BOOLEAN,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_snapshot_search_hotel UNIQUE (search_run_id, hotel_sk)
);

CREATE TABLE offer_definitions (
    offer_def_id BIGSERIAL PRIMARY KEY,
    hotel_sk BIGINT NOT NULL REFERENCES hotels(hotel_sk),
    room_type_sk BIGINT REFERENCES room_types(room_type_sk),
    rate_plan_id BIGINT REFERENCES rate_plans(rate_plan_id),
    checkin_date DATE NOT NULL,
    length_of_stay SMALLINT NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'EUR',
    is_refundable BOOLEAN,
    breakfast_included BOOLEAN,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    CONSTRAINT uq_offer_config UNIQUE (
        hotel_sk, room_type_sk, rate_plan_id,
        checkin_date, length_of_stay, is_refundable, breakfast_included
    )
);

CREATE TABLE price_observations (
    observation_id BIGSERIAL PRIMARY KEY,
    offer_def_id BIGINT NOT NULL REFERENCES offer_definitions(offer_def_id) ON DELETE CASCADE,
    search_run_id BIGINT REFERENCES search_runs(search_run_id) ON DELETE CASCADE,
    price_total NUMERIC(12,2) NOT NULL,
    price_original_total NUMERIC(12,2),
    price_per_night_avg NUMERIC(12,2),
    rooms_left SMALLINT,
    free_cancellation_until TIMESTAMPTZ,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_observation_time UNIQUE (offer_def_id, observed_at)
);

CREATE INDEX idx_search_def_checkin ON search_definitions(checkin_date);
CREATE INDEX idx_search_def_last_used ON search_definitions(last_used_at);
CREATE INDEX idx_search_run_def ON search_runs(search_def_id);
CREATE INDEX idx_search_run_timestamp ON search_runs(run_timestamp);
CREATE INDEX idx_search_run_scrape ON search_runs(scrape_run_id);
CREATE INDEX idx_snapshot_run ON hotel_availability_snapshots(search_run_id);
CREATE INDEX idx_snapshot_hotel ON hotel_availability_snapshots(hotel_sk);
CREATE INDEX idx_snapshot_observed ON hotel_availability_snapshots(observed_at);
CREATE INDEX idx_offer_def_hotel ON offer_definitions(hotel_sk);
CREATE INDEX idx_offer_def_checkin ON offer_definitions(checkin_date);
CREATE INDEX idx_offer_def_room ON offer_definitions(room_type_sk);
CREATE INDEX idx_price_obs_offer ON price_observations(offer_def_id);
CREATE INDEX idx_price_obs_run ON price_observations(search_run_id);
CREATE INDEX idx_price_obs_time ON price_observations(observed_at);
CREATE INDEX idx_price_obs_price ON price_observations(price_total);

CREATE OR REPLACE FUNCTION get_or_create_search_definition(
    p_checkin_date DATE,
    p_length_of_stay BIGINT,
    p_occupancy_adults BIGINT,
    p_occupancy_children BIGINT,
    p_rooms BIGINT DEFAULT 1,
    p_currency CHAR(3) DEFAULT 'EUR',
    p_locale TEXT DEFAULT 'it-IT',
    p_children_ages JSONB DEFAULT NULL,
    p_device_type TEXT DEFAULT 'desktop',
    p_market_country CHAR(2) DEFAULT 'IT'
) RETURNS BIGINT AS $$
DECLARE
    v_search_def_id BIGINT;
BEGIN
    SELECT search_def_id INTO v_search_def_id
    FROM search_definitions
    WHERE checkin_date = p_checkin_date
      AND length_of_stay = p_length_of_stay::smallint
      AND occupancy_adults = p_occupancy_adults::smallint
      AND occupancy_children = p_occupancy_children::smallint
      AND rooms = p_rooms::smallint
      AND currency = p_currency
      AND locale = p_locale
      AND children_ages IS NOT DISTINCT FROM p_children_ages
      AND device_type = p_device_type
      AND market_country = p_market_country;

    IF v_search_def_id IS NULL THEN
        INSERT INTO search_definitions (
            checkin_date, length_of_stay, occupancy_adults,
            occupancy_children, rooms, currency, locale,
            children_ages, device_type, market_country
        ) VALUES (
            p_checkin_date, p_length_of_stay::smallint, p_occupancy_adults::smallint,
            p_occupancy_children::smallint, p_rooms::smallint, p_currency, p_locale,
            p_children_ages, p_device_type, p_market_country
        )
        ON CONFLICT (checkin_date, length_of_stay, occupancy_adults,
                     occupancy_children, rooms, currency, locale,
                     children_ages, device_type, market_country)
        DO UPDATE SET last_used_at = NOW()
        RETURNING search_def_id INTO v_search_def_id;
    ELSE
        UPDATE search_definitions
        SET last_used_at = NOW()
        WHERE search_def_id = v_search_def_id;
    END IF;

    RETURN v_search_def_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_or_create_offer_definition(
    p_hotel_sk BIGINT,
    p_room_type_sk BIGINT,
    p_rate_plan_id BIGINT,
    p_checkin_date DATE,
    p_length_of_stay SMALLINT,
    p_is_refundable BOOLEAN,
    p_breakfast_included BOOLEAN
) RETURNS BIGINT AS $$
DECLARE
    v_offer_def_id BIGINT;
BEGIN
    INSERT INTO offer_definitions (
        hotel_sk, room_type_sk, rate_plan_id,
        checkin_date, length_of_stay, is_refundable, breakfast_included
    ) VALUES (
        p_hotel_sk, p_room_type_sk, p_rate_plan_id,
        p_checkin_date, p_length_of_stay, p_is_refundable, p_breakfast_included
    )
    ON CONFLICT (hotel_sk, room_type_sk, rate_plan_id,
                 checkin_date, length_of_stay, is_refundable, breakfast_included)
    DO UPDATE SET last_seen_at = NOW()
    RETURNING offer_def_id INTO v_offer_def_id;

    RETURN v_offer_def_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW searches_v2 AS
SELECT
    sr.search_run_id as search_id,
    sr.run_timestamp as search_timestamp,
    sd.checkin_date,
    sd.length_of_stay,
    sd.occupancy_adults,
    sd.occupancy_children,
    sd.children_ages,
    sd.currency,
    sd.locale,
    sd.device_type,
    sd.market_country,
    sr.scrape_run_id,
    sr.total_results,
    sr.request_hash,
    sd.rooms
FROM search_runs sr
JOIN search_definitions sd ON sr.search_def_id = sd.search_def_id;

CREATE OR REPLACE VIEW offers_v2 AS
SELECT
    po.observation_id as offer_id,
    po.search_run_id as search_id,
    od.hotel_sk,
    od.room_type_sk,
    od.rate_plan_id,
    od.checkin_date,
    od.length_of_stay,
    od.currency,
    po.price_total,
    po.price_original_total,
    po.price_per_night_avg,
    po.rooms_left,
    od.is_refundable,
    od.breakfast_included,
    po.free_cancellation_until,
    po.observed_at as created_at,
    NULL::jsonb as raw_conditions
FROM price_observations po
JOIN offer_definitions od ON po.offer_def_id = od.offer_def_id;

CREATE OR REPLACE VIEW hotel_search_results_v2 AS
SELECT
    has.snapshot_id as hotel_search_result_id,
    has.search_run_id as search_id,
    has.hotel_sk,
    has.position_in_list,
    has.is_available,
    has.is_sold_out,
    has.is_unlisted,
    has.review_score,
    has.review_count,
    has.distance_center_km,
    has.booking_paga,
    has.last_observed_at as scraped_at
FROM hotel_availability_snapshots has;

CREATE MATERIALIZED VIEW IF NOT EXISTS latest_prices AS
SELECT DISTINCT ON (po.offer_def_id)
    po.observation_id,
    po.offer_def_id,
    od.hotel_sk,
    od.room_type_sk,
    od.rate_plan_id,
    od.checkin_date,
    od.length_of_stay,
    od.currency,
    po.price_total,
    po.price_original_total,
    po.price_per_night_avg,
    po.rooms_left,
    od.is_refundable,
    od.breakfast_included,
    po.free_cancellation_until,
    po.observed_at,
    po.search_run_id
FROM price_observations po
JOIN offer_definitions od ON po.offer_def_id = od.offer_def_id
ORDER BY po.offer_def_id, po.observed_at DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_prices_offer
    ON latest_prices(offer_def_id);

CREATE INDEX IF NOT EXISTS idx_latest_prices_hotel_date
    ON latest_prices(hotel_sk, checkin_date);

CREATE OR REPLACE FUNCTION refresh_latest_prices()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY latest_prices;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW price_history AS
SELECT
    od.hotel_sk,
    od.checkin_date,
    od.length_of_stay,
    od.room_type_sk,
    od.rate_plan_id,
    od.is_refundable,
    od.breakfast_included,
    po.price_total,
    po.price_per_night_avg,
    po.observed_at,
    po.search_run_id,
    sd.occupancy_adults,
    sd.occupancy_children,
    sr.run_timestamp,
    sd.rooms
FROM price_observations po
JOIN offer_definitions od ON po.offer_def_id = od.offer_def_id
JOIN search_runs sr ON po.search_run_id = sr.search_run_id
JOIN search_definitions sd ON sr.search_def_id = sd.search_def_id
ORDER BY od.hotel_sk, od.checkin_date, po.observed_at DESC;

CREATE OR REPLACE VIEW search_summary AS
SELECT
    sr.search_run_id,
    sr.run_timestamp,
    sd.checkin_date,
    sd.length_of_stay,
    sd.occupancy_adults,
    sd.occupancy_children,
    COUNT(DISTINCT has.hotel_sk) as hotels_found,
    COUNT(DISTINCT po.offer_def_id) as unique_offers,
    COUNT(po.observation_id) as total_observations,
    MIN(po.price_total) as min_price,
    AVG(po.price_total) as avg_price,
    MAX(po.price_total) as max_price,
    sd.rooms
FROM search_runs sr
JOIN search_definitions sd ON sr.search_def_id = sd.search_def_id
LEFT JOIN hotel_availability_snapshots has ON sr.search_run_id = has.search_run_id
LEFT JOIN price_observations po ON sr.search_run_id = po.search_run_id
GROUP BY sr.search_run_id, sr.run_timestamp, sd.checkin_date, sd.length_of_stay,
         sd.occupancy_adults, sd.occupancy_children, sd.rooms;

-- Sitemap URLs (from sitemap downloader)
CREATE TABLE sitemap_urls (
    sitemap_url_id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    lastmod TIMESTAMPTZ,
    country_code CHAR(2),
    sitemap_source TEXT,
    depth_level INTEGER,
    last_scraped_at TIMESTAMPTZ,
    details_scraped_at TIMESTAMPTZ,
    details_last_error TEXT,
    details_attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Monitoring Pipelines (Recurring Price Tracking)
CREATE TABLE monitoring_pipelines (
    monitor_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    schedule VARCHAR(100) NOT NULL, -- Cron syntax: "0 2 * * *"
    enabled BOOLEAN DEFAULT true,
    
    -- Filters for hotel selection
    filter_countries TEXT[], -- ['IT', 'FR']
    filter_regions TEXT[],   -- NUTS codes or city names
    filter_cities TEXT[],
    filter_min_stars NUMERIC(2,1),
    filter_max_stars NUMERIC(2,1),
    filter_min_square_meters NUMERIC(6,2),
    filter_max_square_meters NUMERIC(6,2),
    filter_property_types TEXT[],
    filter_amenities JSONB,
    filter_max_hotels INTEGER, -- Limit results per run
    
    -- Search parameters
    search_checkin_date_mode VARCHAR(20) CHECK (search_checkin_date_mode IN ('relative', 'absolute')),
    search_checkin_date_offset INTEGER, -- Days from today (if relative)
    search_checkin_date_offset_start INTEGER, -- Start days from today (if relative)
    search_checkin_date_offset_end INTEGER,   -- End days from today (if relative)
    search_checkin_date_fixed DATE,     -- Fixed date (if absolute)
    search_length_of_stay SMALLINT NOT NULL,
    search_rooms SMALLINT NOT NULL DEFAULT 1,
    search_adults SMALLINT NOT NULL DEFAULT 2,
    search_children SMALLINT NOT NULL DEFAULT 0,
    search_children_ages JSONB,
    search_currency CHAR(3) DEFAULT 'EUR',
    
    -- Execution settings
    concurrency_limit INTEGER DEFAULT 10,
    sample_size INTEGER, -- If set, randomly sample this many hotels
    enable_booking_paga_detection BOOLEAN DEFAULT false,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(255),
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    last_run_status VARCHAR(50),
    last_run_hotels_count INTEGER,
    last_run_offers_count INTEGER
);

-- Price Snapshots (Aggregated analytics)
CREATE TABLE price_snapshots (
    snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    monitor_id UUID REFERENCES monitoring_pipelines(monitor_id) ON DELETE CASCADE,
    search_id BIGINT REFERENCES searches(search_id),
    
    -- Aggregation dimensions
    snapshot_date DATE NOT NULL,
    city_id BIGINT REFERENCES cities(city_id),
    country_code CHAR(2),
    hotel_sk BIGINT REFERENCES hotels(hotel_sk), -- NULL for city/country aggregates
    
    -- Metrics
    avg_price NUMERIC(12,2),
    min_price NUMERIC(12,2),
    max_price NUMERIC(12,2),
    median_price NUMERIC(12,2),
    hotels_count INTEGER,
    offers_count INTEGER,
    availability_rate NUMERIC(5,2), -- % of hotels with offers
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Scraper Metrics Time-Series (Aggregate metrics snapshots every 30s)
CREATE TABLE scraper_metrics_timeseries (
    metric_id BIGSERIAL PRIMARY KEY,
    scrape_run_id BIGINT REFERENCES scrape_runs(scrape_run_id),
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Request metrics
    requests_per_second NUMERIC(10,2),
    avg_requests_per_second_10m NUMERIC(10,2), -- Rolling 10-minute average
    peak_requests_per_second NUMERIC(10,2),
    total_requests INTEGER,
    successful_requests INTEGER,
    failed_requests INTEGER,

    -- Performance metrics
    avg_latency_ms NUMERIC(10,2),
    median_latency_ms NUMERIC(10,2),
    p95_latency_ms NUMERIC(10,2),

    -- Error tracking
    error_rate NUMERIC(5,2), -- Percentage
    waf_blocked_count INTEGER,
    timeout_count INTEGER,

    -- Token pool health
    pool_size INTEGER,
    pool_health_score NUMERIC(5,2), -- 0-100
    token_consumption_rate NUMERIC(10,2), -- tokens/s

    -- Proxy metrics
    proxy_success_rate NUMERIC(5,2), -- Percentage
    active_proxies INTEGER,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Monitor Run History (Detailed execution tracking for monitors)
CREATE TABLE monitor_run_history (
    run_id BIGSERIAL PRIMARY KEY,
    monitor_id UUID REFERENCES monitoring_pipelines(monitor_id) ON DELETE CASCADE,
    monitor_run_id BIGINT,
    scrape_run_id BIGINT REFERENCES scrape_runs(scrape_run_id),

    -- Execution timing
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    execution_time_seconds INTEGER,

    -- Status and results
    status VARCHAR(50) CHECK (status IN ('queued', 'running', 'completed', 'failed', 'partial', 'cancelled')),
    hotels_found INTEGER,
    hotels_with_offers INTEGER,
    offers_total INTEGER,

    -- Price statistics
    avg_price NUMERIC(12,2),
    min_price NUMERIC(12,2),
    max_price NUMERIC(12,2),
    currency CHAR(3),

    -- Performance metrics
    total_requests INTEGER,
    avg_requests_per_second NUMERIC(10,2),
    peak_requests_per_second NUMERIC(10,2),
    error_count INTEGER,
    error_rate NUMERIC(5,2),

    -- Configuration snapshot (for audit trail)
    config_snapshot JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Legacy monitor execution tables kept for backward compatibility with the
-- existing monitor worker/controller flow.
CREATE TABLE monitor_runs (
    monitor_run_id BIGSERIAL PRIMARY KEY,
    monitor_id UUID REFERENCES monitoring_pipelines(monitor_id) ON DELETE CASCADE,
    bull_job_id TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'running'
        CHECK (status IN ('queued', 'running', 'completed', 'failed', 'partial', 'cancelled')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (bull_job_id)
);

CREATE TABLE monitor_run_dates (
    monitor_run_date_id BIGSERIAL PRIMARY KEY,
    monitor_run_id BIGINT REFERENCES monitor_runs(monitor_run_id) ON DELETE CASCADE,
    checkin_date DATE NOT NULL,
    nights SMALLINT NOT NULL,
    adults SMALLINT NOT NULL DEFAULT 2,
    children SMALLINT NOT NULL DEFAULT 0,
    children_ages JSONB,
    currency CHAR(3) NOT NULL DEFAULT 'EUR',
    processed_hotels INTEGER NOT NULL DEFAULT 0,
    failed_hotels INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (monitor_run_id, checkin_date)
);

ALTER TABLE monitor_run_history
    ADD CONSTRAINT fk_monitor_run_history_monitor_run
    FOREIGN KEY (monitor_run_id) REFERENCES monitor_runs(monitor_run_id) ON DELETE CASCADE;

ALTER TABLE monitor_run_history
    ADD CONSTRAINT uq_monitor_run_history_monitor_run UNIQUE (monitor_run_id);

-- Indexes
CREATE INDEX idx_sitemap_urls_country ON sitemap_urls(country_code);
CREATE INDEX idx_monitoring_pipelines_schedule ON monitoring_pipelines(enabled, next_run_at);
CREATE INDEX idx_price_snapshots_date ON price_snapshots(snapshot_date);
CREATE INDEX idx_price_snapshots_monitor ON price_snapshots(monitor_id, snapshot_date);
CREATE INDEX idx_price_snapshots_city ON price_snapshots(city_id, snapshot_date);
CREATE INDEX idx_price_snapshots_hotel ON price_snapshots(hotel_sk, snapshot_date);
CREATE INDEX idx_offers_hotel_date ON offers(hotel_sk, checkin_date);
CREATE INDEX idx_offers_checkin ON offers(checkin_date);
CREATE INDEX idx_hotel_search_results_search ON hotel_search_results(search_id);
CREATE INDEX idx_hotel_versions_validity ON hotel_versions(hotel_sk, valid_from, valid_to);
CREATE INDEX idx_hotel_versions_nuts3 ON hotel_versions(nuts3_code) WHERE nuts3_code IS NOT NULL;
CREATE INDEX idx_hotel_versions_lau ON hotel_versions(lau_code) WHERE lau_code IS NOT NULL;
CREATE INDEX idx_nuts_regions_geom ON nuts_regions USING GIST (geom);
CREATE INDEX idx_nuts_regions_country ON nuts_regions(country_code);
CREATE INDEX idx_nuts_regions_level ON nuts_regions(level_code);
CREATE INDEX idx_nuts_regions_parent ON nuts_regions(parent_code) WHERE parent_code IS NOT NULL;
CREATE INDEX idx_lau_regions_geom ON lau_regions USING GIST (geom);
CREATE INDEX idx_lau_regions_country ON lau_regions(country_code);
CREATE INDEX idx_lau_regions_nuts3 ON lau_regions(nuts3_code);
CREATE INDEX idx_lau_regions_country_lower_name ON lau_regions(country_code, lower(lau_name));
CREATE INDEX idx_sitemap_urls_details_pending ON sitemap_urls(country_code, details_scraped_at) WHERE details_scraped_at IS NULL;
CREATE INDEX idx_rate_plans_hotel_name ON rate_plans(hotel_sk, name);
CREATE INDEX idx_rate_plan_versions_active ON rate_plan_versions(rate_plan_id) WHERE valid_to IS NULL;
CREATE INDEX idx_room_types_hotel_booking_room_id ON room_types(hotel_sk, booking_room_id);
CREATE INDEX idx_room_type_versions_active_name ON room_type_versions(room_type_sk, name) WHERE valid_to IS NULL;
CREATE INDEX idx_hotel_versions_current ON hotel_versions(hotel_sk, valid_to, valid_from DESC);

-- Indexes for new metrics tables
CREATE INDEX idx_scraper_metrics_run_time ON scraper_metrics_timeseries(scrape_run_id, timestamp DESC);
CREATE INDEX idx_scraper_metrics_timestamp ON scraper_metrics_timeseries(timestamp DESC);
CREATE INDEX idx_monitor_run_history_monitor_date ON monitor_run_history(monitor_id, started_at DESC);
CREATE INDEX idx_monitor_run_history_status ON monitor_run_history(status, started_at DESC);
CREATE UNIQUE INDEX idx_monitor_run_history_monitor_run_id ON monitor_run_history(monitor_run_id) WHERE monitor_run_id IS NOT NULL;
CREATE INDEX idx_monitor_runs_monitor_status ON monitor_runs(monitor_id, status, started_at DESC);
CREATE INDEX idx_monitor_run_dates_run_status ON monitor_run_dates(monitor_run_id, status);
CREATE INDEX idx_monitor_run_dates_checkin ON monitor_run_dates(checkin_date);
