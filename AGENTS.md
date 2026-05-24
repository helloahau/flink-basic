# AGENTS.md — flink-basic

## Project Purpose
Tutorial code extracted from the **11-flink** Yuque notebook (`BD20250501`).
Classes are verbatim copies of doc snippets; they exist to illustrate Flink concepts, not to form a single runnable application.

## Build
```bash
cd /Volumes/Work/Flink/Project/basic
mvn -q compile          # compile only (no runnable fat-jar target defined)
```
- Java 11, Flink 1.18.1, Kafka connector 3.1.0-1.18.
- `flink-streaming-java` and `flink-clients` are `provided` — do **not** add them as compile-scope.
- No test suite exists; `src/test/java` is empty.

## Re-extract from Yuque
```bash
python3 extract_from_yuque.py    # reads HTML from /Volumes/Work/Flink/Learn/
```
Source HTML lives at `/Volumes/Work/Flink/Learn/` (section 11-flink). `sources.json` records the mapping of every saved block to its originating Yuque doc slug and kind (`java-class`, `java-snippet`, `xml`, `shell`, `properties`).

## Repository Layout

| Path | Contents |
|---|---|
| `src/main/java/com/bigdata/` | Top-level full tutorial classes (misc days) |
| `src/main/java/com/bigdata/day01/`–`day07/` | Day-by-day progression (DataStream API → SQL → advanced) |
| `src/main/java/com/bigdata/flinksql/` | Flink Table/SQL API examples |
| `src/main/java/com/bigdata/cdc/` | Flink CDC demos (FlinkCDC, CdcSQLTest) |
| `src/main/java/com/bigdata/smart/` | 智慧交通 (smart traffic) project demos |
| `src/main/java/com/bigdata/twophase/` | Exactly-once / two-phase-commit Kafka demos |
| `src/main/java/com/bigdata/snippets/<slug>/` | Short non-compilable fragments from a doc |
| `src/main/resources/<slug>/` | XML / properties / shell notes from the same doc |
| `sources.json` | Block→file provenance index |

## Key Conventions
- **Duplicate demos** are suffixed `_2`, `_3` (e.g., `WordCount02.java`, `WordCount02_2.java`, `WordCount02_3.java`). They are independent variants, not refactors.
- **`snippets/` classes** are non-compilable fragments placed in per-slug sub-packages (e.g., `com.bigdata.snippets.wiq1t6e6i5on9bgi`). Do not try to fix import errors there.
- All POJO models use Lombok `@Data @NoArgsConstructor @AllArgsConstructor` (see `WaterSensor.java`, `LogBean.java`).
- The canonical hello-world pattern (every runnable class): env → source → transformation → sink → `env.execute()`.
- Chinese identifiers and comments are intentional (tutorial language).

## Common Flink Patterns Used
- `StreamExecutionEnvironment.getExecutionEnvironment()` with explicit `setRuntimeMode(STREAMING|BATCH)`.
- `keyBy` + `sum/reduce/aggregate` for stateful counting (see `day01/`, `day03/`).
- Watermarks and event-time windows in `day04/` / `day06/`.
- Keyed state (`ValueState`, `ListState`) in `day05/_01_KeyedStateDemo.java`.
- Two-phase commit sink for exactly-once Kafka output: `twophase/KafkaExactlyOnceDemo.java`.
- Flink SQL via `StreamTableEnvironment` in `flinksql/` and `sql/`.

## External Dependencies
- **Kafka**: local broker assumed at `localhost:9092` in most Kafka demos.
- **Flink CDC** (`cdc/`): requires a running MySQL instance; connection details are hardcoded in `FlinkCDC.java`.
- Source HTML for re-extraction: `/Volumes/Work/Flink/Learn/` (local Yuque mirror, not in repo).

