-- ClickHouse bootstrap: create the three Medallion layer databases.
-- Iceberg tables are created MANUALLY after Flink has populated the warehouse
-- (see 01_iceberg_tables.sql.manual for the full CREATE TABLE statements).
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

