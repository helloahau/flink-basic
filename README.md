# flink-basic

Java examples extracted from **11-flink** Yuque notes (`BD20250501`).

## Layout

```
src/main/java/          Full tutorial classes (by package)
src/main/java/com/bigdata/snippets/   Short code fragments from docs
src/main/resources/     XML/properties/shell notes from tutorials
sources.json            Mapping of each saved block to source doc
```

## Build

```bash
cd /Volumes/Work/Flink/Project/basic
mvn -q compile
```

Note: many files are **tutorial snippets** copied verbatim from docs. Not every class compiles together without edits (duplicate class names are suffixed with `_2`, `_3`, etc.).

## Local Dev Services

Start with `docker-compose up -d` (see `docker/README.md` for details).

| Service | Host | Port | Default Password |
|---|---|---|---|
| **Kafka** | `localhost` / `bigdata01` | `9092` | — |
| **Kafka UI** | `localhost` | `8083` | — |
| **MySQL** | `localhost` / `bigdata01` | `3306` | `root` / `123456` |
| **MinIO** | `localhost` | `9000` (API), `9001` (Console) | `minioadmin` / `minioadmin123` |
| **Nessie** (Iceberg REST catalog) | `localhost` | `19120` | — |
| **Flink JobManager UI** | `localhost` | `8082` | — |

> **MySQL CDC database**: `cdc_test` &nbsp;·&nbsp; table: `user_info`  
> Add `127.0.0.1  bigdata01` to `/etc/hosts` for demos that hard-code that hostname.

### 🖥️ How to access the Flink UI

The local environment runs a Flink 1.18.1 cluster (`lakehouse-flink-jm` + `lakehouse-flink-tm`).

**Web UI** → open **http://localhost:8082** in your browser.

> When you run demos directly from IntelliJ (local execution), **no Flink cluster is used**
> and the Web UI is not available. The JVM executes Flink jobs in-process (mini-cluster).
> To see the Web UI for a local job, add the following to the run config:
> ```java
> // Enables the built-in mini-cluster REST endpoint (port auto-assigned)
> env.getConfiguration().set(RestOptions.BIND_PORT, "8085");
> ```
> Then open http://localhost:8085 while the job is running.

## Iceberg Batch Demo Scenarios

Located in `src/main/java/com/bigdata/iceberg/`.
Run these **in order** (Demo 1 creates data that 2 & 3 depend on).

| Class | Scenario |
|---|---|
| `_01_IcebergBatchWrite` | Batch-write WaterSensor records to an Iceberg table (HadoopCatalog, local FS) |
| `_02_IcebergBatchRead`  | Batch-read the same table; shows full-scan, predicate pushdown, aggregation, column-pruning, snapshot metadata |
| `_03_IcebergETL`        | Full Bronze → Silver → Gold ETL/ELT pipeline using three Iceberg tables |

**Catalog & storage defaults** (no external services required):
- Catalog: `HadoopCatalog` (local filesystem)
- Warehouse: `/tmp/flink-iceberg-warehouse`

**Optional: switch to Nessie + MinIO** — both already running in the lakehouse stack.
Uncomment the **Option B** catalog block in each demo file:
- Nessie REST catalog: `http://localhost:19120/iceberg`
- MinIO S3 storage: `http://localhost:9000` (key: `minioadmin`, secret: `minioadmin123`)

## Re-extract from Yuque

```bash
python3 extract_from_yuque.py
```

Source HTML: `/Volumes/Work/Flink/Learn/` (section **11-flink**).
