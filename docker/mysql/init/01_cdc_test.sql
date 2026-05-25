-- ─────────────────────────────────────────────────────────────
-- Initialisation script for the `cdc_test` database.
-- Used by the Flink CDC demo: com.bigdata.cdc.FlinkCDC
--
-- This file is executed automatically when the MySQL container
-- starts for the first time (docker-entrypoint-initdb.d).
-- ─────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS `cdc_test`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `cdc_test`;

-- Table monitored by FlinkCDC.java (.tableList("cdc_test.user_info"))
CREATE TABLE IF NOT EXISTS `user_info` (
  `id`   INT          NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` VARCHAR(100) NOT NULL                COMMENT '姓名',
  `age`  INT                  DEFAULT NULL    COMMENT '年龄',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = 'Flink CDC demo table';

-- Seed data (CDC will capture these rows on initial snapshot)
INSERT INTO `user_info` (`name`, `age`) VALUES
  ('Alice',   30),
  ('Bob',     25),
  ('Charlie', 35),
  ('Diana',   28);

-- ─────────────────────────────────────────────────────────────
-- Grant replication privileges to root so Debezium can read
-- the binlog without extra user setup.
-- ─────────────────────────────────────────────────────────────
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'root'@'%';
FLUSH PRIVILEGES;

