-- ──────────────────────────────────────────────────────────────────────────────
-- ClickHouse Iceberg Medallion Views
--
-- These are VIEWS (not tables) — they read Parquet files directly from the
-- Iceberg warehouse on every query.  No data is copied into ClickHouse storage.
-- Benefits:
--   ✅ Zero data duplication  ✅ Always reads the latest Flink output
--   ✅ New partitions (new Flink runs) are picked up automatically via **
--
-- user_files_path=/warehouse/ (set in config.d/custom.xml)
-- Paths below are relative to user_files_path.
-- ──────────────────────────────────────────────────────────────────────────────

-- ── Bronze layer ─────────────────────────────────────────────────────────────
CREATE VIEW IF NOT EXISTS bronze.water_sensors AS
SELECT id, ts, vc
FROM file('bronze/flink_demo/water_sensors/data/*.parquet', 'Parquet');

-- ── Silver layer ─────────────────────────────────────────────────────────────
-- Partitioned by status=HIGH|LOW|NORMAL → use ** glob to recurse sub-folders
CREATE VIEW IF NOT EXISTS silver.sensor_cleaned AS
SELECT id, ts, vc, status
FROM file('silver/flink_demo/sensor_cleaned/data/**/*.parquet', 'Parquet');

-- ── Gold layer ───────────────────────────────────────────────────────────────
-- Partitioned by sensor_id=s1|s2|… → use ** glob
CREATE VIEW IF NOT EXISTS gold.sensor_hourly_stats AS
SELECT sensor_id, time_bucket, record_count, min_vc, max_vc,
       avg_vc, high_count, low_count
FROM file('gold/flink_demo/sensor_hourly_stats/data/**/*.parquet', 'Parquet');

-- ── Verify ───────────────────────────────────────────────────────────────────
SELECT 'bronze.water_sensors'     AS view_name, count() AS rows FROM bronze.water_sensors
UNION ALL
SELECT 'silver.sensor_cleaned',   count() FROM silver.sensor_cleaned
UNION ALL
SELECT 'gold.sensor_hourly_stats', count() FROM gold.sensor_hourly_stats;
