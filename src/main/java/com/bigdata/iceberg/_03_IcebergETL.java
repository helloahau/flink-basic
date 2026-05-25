package com.bigdata.iceberg;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @基本功能: 基于 Iceberg 的批处理 ETL / ELT 场景演示
 *
 * 完整数据流 (Medallion Architecture):
 *
 *   [Bronze 层] Iceberg water_sensors  (Raw, 由 _01 写入)
 *         │  ETL (Flink INSERT INTO ... SELECT): 清洗 + 打标签
 *         ▼
 *   [Silver 层] Iceberg sensor_cleaned  (status 标签, 去极值)
 *         │  ELT (Flink INSERT INTO ... SELECT): 聚合
 *         ▼
 *   [Gold 层]  Iceberg sensor_hourly_stats  (每时段聚合指标)
 *         │  验证/报表: Iceberg Java API 直接读取结果
 *         ▼
 *   Console output (BI 报表)
 *
 * 技术选型说明:
 *   - INSERT INTO (写入/转换): 使用 Flink Table API (SQL)
 *   - SELECT (验证/报表): 使用 Iceberg Java API (IcebergGenerics)
 *     → Flink batch mode 的 collect() 有 mini-cluster 生命周期 race condition，
 *       Iceberg Java API 直接读 Parquet，无此问题。
 *
 * 前置条件:
 *   先运行 _01_IcebergBatchWrite 确保 water_sensors 表有数据。
 **/
public class _03_IcebergETL {

    // ── Medallion Architecture: 每层独立子目录 ─────────────────────────
    //   flink-iceberg-warehouse/
    //     bronze/flink_demo/water_sensors      ← 原始数据 (由 _01 写入)
    //     silver/flink_demo/sensor_cleaned     ← 清洗 + 打标签
    //     gold/flink_demo/sensor_hourly_stats  ← 聚合指标
    private static final String WAREHOUSE_ROOT   = "./flink-iceberg-warehouse";
    private static final String WAREHOUSE_BRONZE = WAREHOUSE_ROOT + "/bronze";
    private static final String WAREHOUSE_SILVER = WAREHOUSE_ROOT + "/silver";
    private static final String WAREHOUSE_GOLD   = WAREHOUSE_ROOT + "/gold";

    public static void main(String[] args) throws Exception {
        HadoopCompat.initSimpleAuth();
        HadoopCompat.runWithSubject((PrivilegedExceptionAction<Void>) () -> {
            run();
            return null;
        });
    }

    private static void run() throws Exception {

        // ── 1. 环境准备 ──────────────────────────────────────────────
        // Flink env for INSERT jobs; Iceberg Java API for SELECT/verify
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ── 2. 三层 Catalog 注册 (每层独立 warehouse 路径) ─────────────
        //   iceberg_bronze → ./flink-iceberg-warehouse/bronze
        //   iceberg_silver → ./flink-iceberg-warehouse/silver
        //   iceberg_gold   → ./flink-iceberg-warehouse/gold
        for (String[] c : new String[][]{
                {"iceberg_bronze", WAREHOUSE_BRONZE},
                {"iceberg_silver", WAREHOUSE_SILVER},
                {"iceberg_gold",   WAREHOUSE_GOLD}}) {
            tableEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS " + c[0] + " WITH ("
                + "'type'         = 'iceberg',"
                + "'catalog-type' = 'hadoop',"
                + "'warehouse'    = '" + c[1] + "'"
                + ")"
            );
            // 确保每层的 flink_demo 数据库存在
            tableEnv.useCatalog(c[0]);
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS flink_demo");
        }

        // ====================================================================
        //  STEP 1 – ETL: Bronze → Silver  (Flink INSERT INTO ... SELECT)
        // ====================================================================
        System.out.println("\n====== STEP 1: ETL — Bronze → Silver (数据清洗) ======");
        System.out.printf("  Bronze: %s/flink_demo/water_sensors%n", WAREHOUSE_BRONZE);
        System.out.printf("  Silver: %s/flink_demo/sensor_cleaned%n", WAREHOUSE_SILVER);

        // 目标表建在 Silver Catalog
        tableEnv.useCatalog("iceberg_silver");
        tableEnv.useDatabase("flink_demo");
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS sensor_cleaned ("
            + "  id     STRING   COMMENT '传感器ID',"
            + "  ts     BIGINT   COMMENT '时间戳(ms)',"
            + "  vc     INT      COMMENT '水位(cm)',"
            + "  status STRING   COMMENT '状态: NORMAL / HIGH / LOW'"
            + ") PARTITIONED BY (status) "
            + "WITH ('format-version' = '2')"
        );

        // ETL: 跨 catalog 读 Bronze → 写 Silver
        // 源表用全限定名 iceberg_bronze.flink_demo.water_sensors
        tableEnv.executeSql(
            "INSERT INTO iceberg_silver.flink_demo.sensor_cleaned "
            + "SELECT id, ts, vc, "
            + "  CASE WHEN vc >= 50 THEN 'HIGH' WHEN vc <= 5 THEN 'LOW' ELSE 'NORMAL' END "
            + "FROM iceberg_bronze.flink_demo.water_sensors WHERE vc <= 120"
        ).await();   // ← wait for INSERT to commit before reading with Iceberg Java API

        // Verify with Iceberg Java API (no mini-cluster race condition)
        System.out.println(">>> Silver 层各状态行数：");
        try (HadoopCatalog cat = new HadoopCatalog(new Configuration(false), WAREHOUSE_SILVER)) {
            Table silver = cat.loadTable(TableIdentifier.of("flink_demo", "sensor_cleaned"));
            Map<String, Long> cnt = new HashMap<>();
            try (CloseableIterable<Record> recs = IcebergGenerics.read(silver).build()) {
                for (Record r : recs) {
                    String s = (String) r.getField("status");
                    cnt.merge(s, 1L, Long::sum);
                }
            }
            cnt.entrySet().stream().sorted(Map.Entry.comparingByKey())
               .forEach(e -> System.out.printf("  status=%-7s count=%d%n", e.getKey(), e.getValue()));
            System.out.println("  total cleaned records: " +
                silver.currentSnapshot().summary().get("added-records"));
        }

        // ====================================================================
        //  STEP 2 – ELT: Silver → Gold  (Flink INSERT INTO ... SELECT聚合)
        // ====================================================================
        System.out.println("\n====== STEP 2: ELT — Silver → Gold (聚合指标) ======");
        System.out.printf("  Gold: %s/flink_demo/sensor_hourly_stats%n", WAREHOUSE_GOLD);

        // 目标表建在 Gold Catalog
        tableEnv.useCatalog("iceberg_gold");
        tableEnv.useDatabase("flink_demo");
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS sensor_hourly_stats ("
            + "  sensor_id    STRING  COMMENT '传感器ID',"
            + "  time_bucket  BIGINT  COMMENT '时间分桶(s)',"
            + "  record_count BIGINT  COMMENT '样本数',"
            + "  min_vc       INT     COMMENT '最小水位',"
            + "  max_vc       INT     COMMENT '最大水位',"
            + "  avg_vc       DOUBLE  COMMENT '平均水位',"
            + "  high_count   BIGINT  COMMENT 'HIGH 次数',"
            + "  low_count    BIGINT  COMMENT 'LOW 次数'"
            + ") PARTITIONED BY (sensor_id) "
            + "WITH ('format-version' = '2')"
        );

        // ELT: 跨 catalog 读 Silver → 写 Gold
        tableEnv.executeSql(
            "INSERT INTO iceberg_gold.flink_demo.sensor_hourly_stats "
            + "SELECT id, ts/1000, COUNT(*), MIN(vc), MAX(vc),"
            + "  ROUND(AVG(CAST(vc AS DOUBLE)),2),"
            + "  SUM(CASE WHEN status='HIGH' THEN 1 ELSE 0 END),"
            + "  SUM(CASE WHEN status='LOW'  THEN 1 ELSE 0 END)"
            + " FROM iceberg_silver.flink_demo.sensor_cleaned GROUP BY id, ts/1000"
        ).await();   // wait for Gold INSERT to complete
        System.out.println(">>> Gold 层写入完成，records=" +
            getLatestRecordCount(WAREHOUSE_GOLD, "sensor_hourly_stats"));

        // ====================================================================
        //  STEP 3 – 报表查询: Iceberg Java API 直接读 Gold 表
        // ====================================================================
        System.out.println("\n====== STEP 3: Gold 层报表查询 ======");
        try (HadoopCatalog cat = new HadoopCatalog(new Configuration(false), WAREHOUSE_GOLD)) {
            Table gold = cat.loadTable(TableIdentifier.of("flink_demo", "sensor_hourly_stats"));

            System.out.println(">>> 各传感器综合指标 (Gold 层)：");
            System.out.printf("  %-10s %-8s %-8s %-8s %-8s %-8s %-8s%n",
                "sensor_id", "count", "min_vc", "max_vc", "avg_vc", "high#", "low#");
            // Group by sensor_id using Iceberg API
            Map<String, long[]> stats = new HashMap<>();   // key=sensor_id, val=[count,min,max,sum,high,low,cnt_for_avg]
            try (CloseableIterable<Record> recs = IcebergGenerics.read(gold).build()) {
                for (Record r : recs) {
                    String sid = (String) r.getField("sensor_id");
                    long[] v = stats.computeIfAbsent(sid, k -> new long[]{0,Long.MAX_VALUE,Long.MIN_VALUE,0,0,0,0});
                    v[0] += (Long) r.getField("record_count");
                    v[1] = Math.min(v[1], (Integer) r.getField("min_vc"));
                    v[2] = Math.max(v[2], (Integer) r.getField("max_vc"));
                    v[3] += (long)((Double) r.getField("avg_vc") * (Long) r.getField("record_count"));
                    v[4] += (Long) r.getField("high_count");
                    v[5] += (Long) r.getField("low_count");
                    v[6] += (Long) r.getField("record_count");
                }
            }
            stats.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    long[] v = e.getValue();
                    System.out.printf("  %-10s %-8d %-8d %-8d %-8.2f %-8d %-8d%n",
                        e.getKey(), v[0], v[1], v[2], v[6] > 0 ? (double)v[3]/v[6] : 0.0, v[4], v[5]);
                });

            System.out.println("\n>>> 需要关注的传感器 (有 HIGH 或 LOW 告警)：");
            stats.forEach((sid, v) -> {
                if (v[4] > 0 || v[5] > 0) {
                    System.out.printf("  %-10s HIGH告警=%d  LOW告警=%d%n", sid, v[4], v[5]);
                }
            });
        }

        // ====================================================================
        //  STEP 4 – 跨层 JOIN: Silver (明细) + Gold (聚合)
        // ====================================================================
        System.out.println("\n====== STEP 4: 跨 Iceberg 表 JOIN (Silver + Gold) ======");
        System.out.println(">>> HIGH 状态明细 + 该传感器最大水位：");

        // Build max_vc per sensor from Gold catalog
        Map<String, Integer> maxVcBySensor = new HashMap<>();
        try (HadoopCatalog goldCat = new HadoopCatalog(new Configuration(false), WAREHOUSE_GOLD)) {
            Table gold = goldCat.loadTable(TableIdentifier.of("flink_demo", "sensor_hourly_stats"));
            try (CloseableIterable<Record> gr = IcebergGenerics.read(gold).build()) {
                for (Record r : gr) {
                    String sid = (String) r.getField("sensor_id");
                    maxVcBySensor.merge(sid, (Integer) r.getField("max_vc"), Math::max);
                }
            }
        }

        // Read HIGH rows from Silver catalog and join
        try (HadoopCatalog silverCat = new HadoopCatalog(new Configuration(false), WAREHOUSE_SILVER)) {
            Table silver = silverCat.loadTable(TableIdentifier.of("flink_demo", "sensor_cleaned"));
            System.out.printf("  %-5s %-8s %-5s %-8s %-12s%n", "id", "ts", "vc", "status", "sensor_max_vc");
            try (CloseableIterable<Record> sr = IcebergGenerics.read(silver)
                    .where(Expressions.equal("status", "HIGH")).build()) {
                for (Record r : sr) {
                    String id = (String) r.getField("id");
                    System.out.printf("  %-5s %-8d %-5d %-8s %-12d%n",
                        id, r.getField("ts"), r.getField("vc"), r.getField("status"),
                        maxVcBySensor.getOrDefault(id, -1));
                }
            }
        }

        System.out.println("\nIceberg ETL/ELT 批处理演示完成！");
        System.out.printf("数据仓库目录: %s%n  Bronze: %s/flink_demo/water_sensors%n  Silver: %s/flink_demo/sensor_cleaned%n  Gold:   %s/flink_demo/sensor_hourly_stats%n",
            WAREHOUSE_ROOT, WAREHOUSE_BRONZE, WAREHOUSE_SILVER, WAREHOUSE_GOLD);
    }

    private static String getLatestRecordCount(String warehouse, String tableName) {
        try (HadoopCatalog cat = new HadoopCatalog(new Configuration(false), warehouse)) {
            Table t = cat.loadTable(TableIdentifier.of("flink_demo", tableName));
            return t.currentSnapshot().summary().getOrDefault("added-records", "?");
        } catch (Exception e) { return "?"; }
    }
}

