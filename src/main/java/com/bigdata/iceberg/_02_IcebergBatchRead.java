package com.bigdata.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;

import java.security.PrivilegedExceptionAction;

/**
 * @基本功能: Iceberg 批量读取示例 — 使用 Iceberg Java API 直接读取
 *
 * 场景说明:
 *   从 _01_IcebergBatchWrite 写入的 Iceberg 表中读取数据。
 *   使用 Iceberg 原生 Java API（IcebergGenerics），而非 Flink Table API。
 *   展示 Iceberg 作为独立存储格式的可移植性：任何 JVM 语言都可以直接读写。
 *
 *   读取方式:
 *     1. 全量扫描 (Full Scan)
 *     2. 谓词下推过滤 (Predicate Pushdown)
 *     3. 列裁剪 (Column Selection)
 *     4. 组合过滤
 *     5. Snapshot 历史 (Time Travel 基础)
 *     6. TableScan 文件级别统计
 *
 * 为什么不用 Flink Table API 做 SELECT?
 *   Flink batch mode 每条 executeSql("SELECT...") 启动独立 mini-cluster，
 *   collect() 迭代器在 mini-cluster 关闭后无法拉取结果（timing race）。
 *   Iceberg Java API 直接读 Parquet 文件，无此问题，且适合数据验证场景。
 *
 * 前置条件:
 *   先运行 _01_IcebergBatchWrite，确保表和数据已存在。
 **/
public class _02_IcebergBatchRead {

    /** Reads from the Bronze layer — must match _01_IcebergBatchWrite */
    private static final String WAREHOUSE = "./flink-iceberg-warehouse/bronze";

    public static void main(String[] args) throws Exception {
        HadoopCompat.initSimpleAuth();
        HadoopCompat.runWithSubject((PrivilegedExceptionAction<Void>) () -> {
            run();
            return null;
        });
    }

    private static void run() throws Exception {

        // ── 1. 打开 HadoopCatalog（纯 Iceberg API，无 Flink 依赖）──────
        Configuration hadoopConf = new Configuration(false);
        try (HadoopCatalog catalog = new HadoopCatalog(hadoopConf, WAREHOUSE)) {
            Table table = catalog.loadTable(TableIdentifier.of("flink_demo", "water_sensors"));

            // ── 2. 表元信息 ──────────────────────────────────────────────
            System.out.println("=== 表基本信息 ===");
            System.out.println("  Location : " + table.location());
            System.out.println("  Schema   : " + table.schema());
            System.out.println("  Snapshot : " + table.currentSnapshot().snapshotId()
                + " | files=" + table.currentSnapshot().summary().get("added-data-files")
                + " | records=" + table.currentSnapshot().summary().get("added-records"));

            // ── 3. 全量扫描 ──────────────────────────────────────────────
            System.out.println("\n========== ① 全量扫描 (Full Scan) ==========");
            System.out.printf("  %-5s %-8s %-5s%n", "id", "ts", "vc");
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record r : records) {
                    System.out.printf("  %-5s %-8d %-5d%n", r.get(0), r.get(1), r.get(2));
                }
            }

            // ── 4. 谓词下推 — vc > 50 ────────────────────────────────────
            System.out.println("\n========== ② 谓词下推过滤 (vc > 50) ==========");
            System.out.println("  Iceberg 在 Parquet 文件级别跳过不满足条件的 row-group：");
            try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                    .where(Expressions.greaterThan("vc", 50))
                    .build()) {
                for (Record r : records) {
                    System.out.printf("  id=%-3s ts=%-6d vc=%-5d  ← HIGH/EXTREME%n",
                        r.get(0), r.get(1), r.get(2));
                }
            }

            // ── 5. 列裁剪 ────────────────────────────────────────────────
            System.out.println("\n========== ③ 列裁剪 (Column Pruning: id only) ==========");
            System.out.println("  仅读取 id 列的 Parquet column chunk，ts/vc 不被解码：");
            Schema idOnly = table.schema().select("id");
            try (CloseableIterable<Record> records =
                         IcebergGenerics.read(table).project(idOnly).build()) {
                for (Record r : records) {
                    System.out.print("  " + r.get(0));
                }
                System.out.println();
            }

            // ── 6. 组合过滤 ──────────────────────────────────────────────
            System.out.println("\n========== ④ 组合过滤 (id='s1' AND vc > 10) ==========");
            try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                    .where(Expressions.and(
                        Expressions.equal("id", "s1"),
                        Expressions.greaterThan("vc", 10)))
                    .build()) {
                for (Record r : records) {
                    System.out.printf("  id=%s ts=%d vc=%d%n", r.get(0), r.get(1), r.get(2));
                }
            }

            // ── 7. Snapshot 历史 ─────────────────────────────────────────
            System.out.println("\n========== ⑤ Snapshot 历史（Time Travel 基础）==========");
            table.snapshots().forEach(snap -> System.out.printf(
                "  snap_id=%-22d op=%-10s files=%s records=%s%n",
                snap.snapshotId(),
                snap.summary().getOrDefault("operation", "?"),
                snap.summary().getOrDefault("added-data-files", "?"),
                snap.summary().getOrDefault("added-records", "?")));
            System.out.println("\n  Time Travel 用法 (Iceberg Java API):");
            System.out.println("  IcebergGenerics.read(table).useSnapshot(<snapshot_id>).build()");

            // ── 8. TableScan 文件统计 ─────────────────────────────────────
            System.out.println("\n========== ⑥ TableScan 文件级别统计 ==========");
            long[] stats = {0, 0};
            table.newScan().planFiles().forEach(task -> {
                stats[0]++;
                stats[1] += task.file().recordCount();
                System.out.printf("  file: %s | records=%d%n",
                    task.file().path().toString().replaceAll(".*/", ""),
                    task.file().recordCount());
            });
            System.out.printf("  合计: data files=%d, total records=%d%n", stats[0], stats[1]);
        }

        System.out.println("\nIceberg 批量读取演示完成！（Iceberg Java API，无需 Flink mini-cluster）");
    }
}
