# Docker Stack — flink-basic

## Services

| Service | URL / Port | Built-in UI? | Purpose |
|---|---|---|---|
| `kafka-ui` | http://localhost:8083 | ✅ Full UI | Kafka topic browser |
| `mysql` | localhost:3306 | ❌ | Flink CDC source |
| **`trino`** | http://localhost:8080 | ✅ Monitoring UI at `/ui` | Iceberg SQL engine |
| **`dremio`** | http://localhost:9047 | ✅ Full SQL IDE + BI | Iceberg UI + virtual datasets |
| **`clickhouse`** | localhost:8123 / 9000 | ✅ SQL Playground at `/play` | OLAP engine |
| **`cloudbeaver`** | http://localhost:8978 | ✅ Full SQL IDE (web DBeaver) | Connects Trino + ClickHouse + MySQL |

---

## Iceberg Medallion Architecture

```
flink-iceberg-warehouse/                ← host bind-mount (./flink-iceberg-warehouse)
  bronze/flink_demo/water_sensors       ← _01_IcebergBatchWrite  (raw data)
  silver/flink_demo/sensor_cleaned      ← _03_IcebergETL STEP 1  (cleaned)
  gold/flink_demo/sensor_hourly_stats   ← _03_IcebergETL STEP 2  (aggregated)
```

All query-engine containers mount this as `/warehouse`.

---

## Prerequisites

```bash
mkdir -p ./flink-iceberg-warehouse/{bronze,silver,gold}
```

---

## Trino

### Built-in UIs
| URL | What it shows |
|---|---|
| http://localhost:8080/ui | **Monitoring dashboard** — running queries, cluster nodes, memory, query history |

> Trino's built-in UI is **read-only monitoring**, not a SQL editor.  
> Use **CloudBeaver** (http://localhost:8978) or DBeaver desktop for SQL editing.

### CLI
```bash
docker exec -it trino trino
```

### Sample SQL (3 catalogs — bronze / silver / gold)
```sql
SHOW CATALOGS;

SELECT * FROM bronze.flink_demo.water_sensors LIMIT 20;

SELECT status, COUNT(*) AS cnt
FROM silver.flink_demo.sensor_cleaned GROUP BY status;

-- Cross-layer JOIN
SELECT s.id, s.vc, s.status, g.avg_vc
FROM   silver.flink_demo.sensor_cleaned s
JOIN   gold.flink_demo.sensor_hourly_stats g ON s.id = g.sensor_id
WHERE  s.status = 'HIGH' ORDER BY s.ts;
```

### DBeaver / CloudBeaver Connection
- Driver: **Trino** · URL: `jdbc:trino://localhost:8080` · User: `admin` (no password)

---

## ClickHouse

### Built-in UIs
| URL | What it shows |
|---|---|
| http://localhost:8123/play | **SQL Playground** — full SQL editor in browser, instant results |

> Open http://localhost:8123/play and start querying immediately — no login needed.

### CLI
```bash
docker exec -it clickhouse clickhouse-client
```

### Sample SQL (databases: bronze / silver / gold)
```sql
SELECT * FROM bronze.water_sensors LIMIT 10;

SELECT status, count() AS cnt
FROM silver.sensor_cleaned GROUP BY status ORDER BY cnt DESC;

SELECT sensor_id, avg_vc, high_count
FROM gold.sensor_hourly_stats ORDER BY high_count DESC;

-- Cross-layer JOIN
SELECT s.id, s.ts, s.vc, g.avg_vc
FROM silver.sensor_cleaned s
JOIN gold.sensor_hourly_stats g ON s.id = g.sensor_id
WHERE s.status = 'HIGH';
```

### DBeaver / CloudBeaver Connection
- Driver: **ClickHouse** · Host: `localhost` · Port: `8123` · User: `default`

---

## Dremio

Full SQL IDE + BI dashboard + Iceberg reflections (automatic query acceleration).

### First-time setup (http://localhost:9047)
1. Create admin account.
2. **Add Source** → **NAS / Local File System** → Path: `/warehouse` → Name: `iceberg_warehouse`
3. Navigate to `bronze/flink_demo/water_sensors`, right-click → **Format as Iceberg Table**
4. Repeat for `silver/…/sensor_cleaned` and `gold/…/sensor_hourly_stats`
5. Use the built-in SQL editor or create Virtual Datasets (saved views).

---

## CloudBeaver (Web SQL IDE)

Browser-based DBeaver — single UI connecting to **all** databases.

### Setup (http://localhost:8978)
1. Open UI → finish the admin-account wizard.
2. **New Connection** → **Trino**
   - Host: `trino` · Port: `8080` · User: `admin`
3. **New Connection** → **ClickHouse**
   - Host: `clickhouse` · Port: `8123` · User: `default`
4. Optional: **New Connection** → **MySQL**
   - Host: `mysql` · Port: `3306` · User: `root` · Password: `123456`

---

## Start Everything

```bash
# 1. Prepare warehouse directories (one-time)
mkdir -p ./flink-iceberg-warehouse/{bronze,silver,gold}

# 2. Start all services
docker-compose up -d

# 3. Run Flink jobs to populate data
#    _01_IcebergBatchWrite → bronze/flink_demo/water_sensors
#    _03_IcebergETL        → silver/… + gold/…

# 4. Query
open http://localhost:8978    # CloudBeaver  — SQL IDE for Trino + ClickHouse
open http://localhost:8080/ui # Trino        — monitoring dashboard
open http://localhost:8123/play # ClickHouse — SQL Playground
open http://localhost:9047    # Dremio       — full BI + SQL IDE
```

## Services

| Service | URL / Port | Purpose |
|---|---|---|
| `kafka-ui` | http://localhost:8083 | Kafka topic browser |
| `mysql` | localhost:3306 | Flink CDC source |
| **`trino`** | http://localhost:8080 | Iceberg SQL engine (DBeaver-friendly) |
| **`dremio`** | http://localhost:9047 | Iceberg UI + virtual datasets |
| **`clickhouse`** | localhost:8123 (HTTP) / 9000 (native) | OLAP engine (ClickHouse-style) |

---

## Prerequisites

```bash
# Create warehouse subdirectories before first `docker compose up`
mkdir -p ./flink-iceberg-warehouse/{bronze,silver,gold}
```

---

## Iceberg Medallion Architecture

```
flink-iceberg-warehouse/          ← host bind-mount (./flink-iceberg-warehouse)
  bronze/flink_demo/water_sensors      ← raw data  (_01_IcebergBatchWrite)
  silver/flink_demo/sensor_cleaned     ← cleaned   (_03_IcebergETL STEP 1)
  gold/flink_demo/sensor_hourly_stats  ← aggregated (_03_IcebergETL STEP 2)
```

All three engines mount this directory as `/warehouse`.

---

## Trino

### Start
```bash
docker compose up -d trino
```

### CLI
```bash
docker exec -it trino trino
```

### Sample SQL
```sql
SHOW CATALOGS;
-- bronze, silver, gold

SHOW TABLES IN bronze.flink_demo;

SELECT * FROM bronze.flink_demo.water_sensors LIMIT 20;

SELECT status, COUNT(*) AS cnt
FROM silver.flink_demo.sensor_cleaned
GROUP BY status;

-- Cross-layer JOIN
SELECT s.id, s.vc, s.status, g.avg_vc
FROM   silver.flink_demo.sensor_cleaned s
JOIN   gold.flink_demo.sensor_hourly_stats g ON s.id = g.sensor_id
WHERE  s.status = 'HIGH'
ORDER  BY s.ts;
```

### DBeaver Connection
- Driver: **Trino**
- URL: `jdbc:trino://localhost:8080`
- User: `admin` (no password)

---

## Dremio

### Start
```bash
docker compose up -d dremio
```

### First-time Setup (Web UI → http://localhost:9047)
1. Create an admin account (any credentials).
2. **Add Source** → **NAS / Local File System**
   - Path: `/warehouse`
   - Name: `iceberg_warehouse`
3. Browse the folders:
   - `bronze/flink_demo/water_sensors`
   - `silver/flink_demo/sensor_cleaned`
   - `gold/flink_demo/sensor_hourly_stats`
4. Right-click each folder → **Format as Iceberg Table** → Save.
5. Use the SQL editor or create **Virtual Datasets** (saved views).

### JDBC / BI Tools
| Protocol | Connection String |
|---|---|
| Arrow Flight SQL | `jdbc:arrow-flight-sql://localhost:32010` |
| Native ODBC | host=`localhost` port=`31010` |

---

## ClickHouse

### Start
```bash
docker compose up -d clickhouse
```

The `initdb` script (`docker/clickhouse/initdb/01_iceberg_tables.sql`) runs
automatically on first start and creates the three Iceberg external tables.

### CLI
```bash
docker exec -it clickhouse clickhouse-client
```

### Sample SQL
```sql
-- Bronze
SELECT * FROM bronze.water_sensors LIMIT 10;

-- Silver — status distribution
SELECT status, count() AS cnt
FROM silver.sensor_cleaned
GROUP BY status
ORDER BY cnt DESC;

-- Gold — top sensors by HIGH alert count
SELECT sensor_id, record_count, avg_vc, high_count, low_count
FROM gold.sensor_hourly_stats
ORDER BY high_count DESC
LIMIT 10;

-- Cross-layer JOIN (Silver detail + Gold aggregate)
SELECT s.id, s.ts, s.vc, s.status, g.avg_vc, g.high_count
FROM silver.sensor_cleaned  s
JOIN gold.sensor_hourly_stats g ON s.id = g.sensor_id
WHERE s.status = 'HIGH'
ORDER BY s.ts;
```

### DBeaver Connection
- Driver: **ClickHouse** (or HTTP driver at `http://localhost:8123`)
- Host: `localhost` Port: `8123`
- User: `default` (no password by default)

---

## Start Everything

```bash
# 1. Prepare warehouse directories
mkdir -p ./flink-iceberg-warehouse/{bronze,silver,gold}

# 2. Start all services
docker compose up -d

# 3. Run Flink jobs to populate data
#    _01_IcebergBatchWrite  → bronze
#    _03_IcebergETL          → silver + gold

# 4. Query with any engine
docker exec -it trino trino                    # Trino CLI
open http://localhost:9047                     # Dremio Web UI
docker exec -it clickhouse clickhouse-client  # ClickHouse CLI
```
