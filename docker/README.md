# Local Dev Stack — Docker Setup

This folder contains support files for the `docker-compose.yml` at the project root.

---

## Architecture

```
Mac host (Flink JVM runs here)
│
│  localhost:9092 ──────► lakehouse-kafka   (existing container, apache/kafka:3.7.0)
│  localhost:3306 ──────► mysql             (this compose, mysql:8.0)
│  localhost:8083 ──────► kafka-ui          (this compose, provectuslabs/kafka-ui)
```

`kafka-ui` and `kafka-init-topics` use `network_mode: host` so that
`localhost:9092` resolves to the running `lakehouse-kafka` broker
(which advertises itself as `localhost:9092`).

---

## Services managed by this compose

| Service | Image | Local URL | Purpose |
|---|---|---|---|
| **kafka-ui** | `provectuslabs/kafka-ui:latest` | http://localhost:8083 | Web UI to browse topics & messages |
| **mysql** | `mysql:8.0` | `localhost:3306` | MySQL with binlog enabled for Flink CDC |
| **kafka-init-topics** | `apache/kafka:3.7.0` (one-shot) | — | Pre-creates tutorial Kafka topics |

> **Kafka broker** (`localhost:9092`) is provided by the pre-existing
> `lakehouse-kafka` container — no duplicate Kafka is started.

## Pre-existing lakehouse services (not managed here)

These are already running and can be used by the tutorial demos:

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| **Kafka** | `localhost:9092` | — | Message broker |
| **Flink UI** | http://localhost:8082 | — | Flink JobManager Web UI (1.18.1 cluster) |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin123` | S3-compatible object storage |
| **MinIO API** | `http://localhost:9000` | `minioadmin` / `minioadmin123` | Iceberg S3 storage endpoint |
| **Nessie** | `http://localhost:19120` | — | Iceberg REST catalog (`/iceberg` path) |

Buckets in MinIO: `bronze`, `silver`, `gold` (warehouse), `scratch`

---

## Quick Start

```bash
# 1. Start all services
cd /Volumes/Work/Flink/Project/basic
docker-compose up -d

# 2. Open Kafka UI
open http://localhost:8083

# 3. Connect to MySQL
mysql -h 127.0.0.1 -P 3306 -u root -p123456 cdc_test
```

---

## ⚠️  Hostname `bigdata01`

Several source files hard-code `bigdata01` as the Kafka/MySQL host
(e.g. `FlinkCDC.java`, `_03FinkCore_taopai.java`).
Add the following line to `/etc/hosts`:

```
127.0.0.1  bigdata01
```

```bash
sudo sh -c 'echo "127.0.0.1  bigdata01" >> /etc/hosts'
```

---

## Kafka Topics

The `kafka-init-topics` one-shot container pre-creates:

| Topic | Used by |
|---|---|
| `topic1` | `KafkaExactlyOnceDemo`, `SocketToKafkaTwoPhaseDemo`, `flinksql/day02` demos |
| `topic2` | `flinksql/day02/_002Demo` (output topic) |
| `topic-car` | `_01智慧交通项目之超速处理`, `_05_智慧交通超速车辆sql版本` |
| `first` | `day07/Demo03_sql` |

Topics are also auto-created on first use (Kafka default).

---

## MySQL / CDC

- **Host**: `localhost:3306` (also `bigdata01:3306` after `/etc/hosts` edit)
- **Database**: `cdc_test` — table `user_info(id, name, age)`, seeded with 4 rows
- **Root password**: `123456`
- Binlog enabled with `ROW` format + GTID — required by Flink CDC (Debezium).

Simulate CDC events:
```sql
USE cdc_test;
INSERT INTO user_info (name, age) VALUES ('Eve', 22);
UPDATE user_info SET age = 31 WHERE name = 'Alice';
DELETE FROM user_info WHERE name = 'Bob';
```

---

## Stop / Teardown

```bash
# Stop containers (keep MySQL data volume)
docker-compose down

# Full reset including MySQL data
docker-compose down -v
```
