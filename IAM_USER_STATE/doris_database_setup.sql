-- =====================================================
-- IAM用户流速统计数据库设置脚本 - Doris版本
-- 数据库服务器: 192.168.15.51:9030
-- 用户: root/123456
-- 数据库: packets_statistics
-- 表: nf_user_flow_statistics_v2 (用户级别，新表)
-- 表: nf_device_flow_statistics (设备级别)
-- =====================================================

-- 1. 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS packets_statistics;

-- 2. 使用数据库
USE packets_statistics;

-- 3. 创建用户级别流速统计表（新表）- Doris版本
CREATE TABLE IF NOT EXISTS nf_user_flow_statistics_v2 (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `machine_room` VARCHAR(100) NOT NULL COMMENT '机房',
    `device_ip` VARCHAR(45) NOT NULL COMMENT '设备IP',
    `device_type` VARCHAR(10) NOT NULL COMMENT '设备类型',
    `record_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    `user_name` VARCHAR(100) NOT NULL COMMENT '用户名',
    `user_ip` VARCHAR(45) NOT NULL COMMENT 'IP',
    `up_flow_rate` DECIMAL(12,3) NOT NULL DEFAULT "0" COMMENT '上行Mbps',
    `down_flow_rate` DECIMAL(12,3) NOT NULL DEFAULT "0" COMMENT '下行Mbps',
    `total_flow_rate` DECIMAL(12,3) NOT NULL DEFAULT "0" COMMENT '总流速Mbps',
    `session_count` INT NOT NULL DEFAULT "0" COMMENT '会话数',
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
DUPLICATE KEY(`id`, `machine_room`, `device_ip`, `device_type`, `record_time`)
COMMENT 'NF设备Top用户流速统计表V2（用户级别，新表）'
DISTRIBUTED BY HASH(`device_ip`) BUCKETS 32
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);

-- 4. 创建设备级别流速统计表（新增）- Doris版本
CREATE TABLE IF NOT EXISTS nf_device_flow_statistics (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `machine_room` VARCHAR(100) NOT NULL COMMENT '机房',
    `device_ip` VARCHAR(45) NOT NULL COMMENT '设备IP',
    `device_type` VARCHAR(10) NOT NULL COMMENT '设备类型',
    `record_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    `up_flow_rate` DECIMAL(12,3) NOT NULL DEFAULT "0" COMMENT '上行Mbps',
    `down_flow_rate` DECIMAL(12,3) NOT NULL DEFAULT "0" COMMENT '下行Mbps',
    `total_flow_rate` DECIMAL(12,3) NOT NULL DEFAULT "0" COMMENT '总流速Mbps',
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP
DUPLICATE KEY(`id`, `machine_room`, `device_ip`, `device_type`, `record_time`)
COMMENT 'NF设备流速统计表（设备级别）'
DISTRIBUTED BY HASH(`device_ip`) BUCKETS 16
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
);

-- 5. 创建机房统计视图（用户级别）
CREATE VIEW IF NOT EXISTS v_machine_room_user_stats AS
SELECT
    machine_room AS `机房`,
    device_type AS `设备类型`,
    COUNT(DISTINCT device_ip) AS `设备数量`,
    COUNT(*) AS `用户数量`,
    AVG(total_flow_rate) AS `平均流速_Mbps`,
    MAX(total_flow_rate) AS `最大流速_Mbps`,
    SUM(total_flow_rate) AS `总流速_Mbps`,
    DATE(record_time) AS `统计日期`
FROM nf_user_flow_statistics_v2
GROUP BY machine_room, device_type, DATE(record_time)
ORDER BY DATE(record_time) DESC, machine_room, device_type;

-- 6. 创建设备统计视图（用户级别）
CREATE VIEW IF NOT EXISTS v_device_user_stats AS
SELECT
    machine_room AS `机房`,
    device_type AS `设备类型`,
    device_ip AS `设备IP`,
    COUNT(*) AS `用户数量`,
    AVG(total_flow_rate) AS `平均流速_Mbps`,
    MAX(total_flow_rate) AS `最大流速_Mbps`,
    SUM(total_flow_rate) AS `总流速_Mbps`,
    DATE(record_time) AS `统计日期`
FROM nf_user_flow_statistics_v2
GROUP BY machine_room, device_type, device_ip, DATE(record_time)
ORDER BY DATE(record_time) DESC, machine_room, total_flow_rate DESC;

-- 7. 创建Top用户视图
CREATE VIEW IF NOT EXISTS v_top_users_flow AS
SELECT
    machine_room AS `机房`,
    device_type AS `设备类型`,
    device_ip AS `设备IP`,
    user_name AS `用户名`,
    user_ip AS `IP`,
    total_flow_rate AS `总流速_Mbps`,
    up_flow_rate AS `上行流速_Mbps`,
    down_flow_rate AS `下行流速_Mbps`,
    session_count AS `会话数`,
    record_time AS `记录时间`
FROM nf_user_flow_statistics_v2
ORDER BY DATE(record_time) DESC, machine_room, total_flow_rate DESC;

-- 8. 创建设备级别流速统计视图
CREATE VIEW IF NOT EXISTS v_device_flow_stats AS
SELECT
    machine_room AS `机房`,
    device_ip AS `设备IP`,
    device_type AS `设备类型`,
    up_flow_rate AS `上行Mbps`,
    down_flow_rate AS `下行Mbps`,
    total_flow_rate AS `总流速Mbps`,
    record_time AS `记录时间`
FROM nf_device_flow_statistics
ORDER BY DATE(record_time) DESC, machine_room, total_flow_rate DESC;

-- =====================================================
-- 常用查询语句 - Doris版本
-- =====================================================

-- 查询今日各机房统计（用户级别）
-- SELECT * FROM v_machine_room_user_stats WHERE `统计日期` = CURDATE();

-- 查询今日各设备统计（用户级别）
-- SELECT * FROM v_device_user_stats WHERE `统计日期` = CURDATE();

-- 查询今日Top用户
-- SELECT * FROM v_top_users_flow WHERE DATE(`记录时间`) = CURDATE();

-- 查询特定机房的Top用户
-- SELECT * FROM v_top_users_flow WHERE `机房` = 'A2' AND DATE(`记录时间`) = CURDATE();

-- 查询特定设备类型的统计
-- SELECT * FROM v_machine_room_user_stats WHERE `设备类型` = '25G' AND `统计日期` = CURDATE();

-- 查询今日设备级别流速统计
-- SELECT * FROM v_device_flow_stats WHERE DATE(`记录时间`) = CURDATE();

-- =====================================================
-- Doris特有的优化查询
-- =====================================================

-- 查看表的分区信息
-- SHOW PARTITIONS FROM nf_user_flow_statistics_v2;

-- 查看表的存储信息
-- SHOW DATA FROM nf_user_flow_statistics_v2;

-- 查看表结构
-- DESC nf_user_flow_statistics_v2;

-- 查看建表语句
-- SHOW CREATE TABLE nf_user_flow_statistics_v2;

-- 统计表行数（快速）
-- SELECT COUNT(*) FROM nf_user_flow_statistics_v2;

-- 按日期分组统计（利用Doris的列式存储优势）
-- SELECT 
--     DATE(record_time) as date,
--     machine_room,
--     COUNT(*) as user_count,
--     AVG(total_flow_rate) as avg_flow,
--     MAX(total_flow_rate) as max_flow
-- FROM nf_user_flow_statistics_v2 
-- WHERE record_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
-- GROUP BY DATE(record_time), machine_room
-- ORDER BY date DESC, machine_room;

-- 高效的Top N查询
-- SELECT machine_room, device_ip, user_name, total_flow_rate
-- FROM (
--     SELECT *, ROW_NUMBER() OVER (PARTITION BY machine_room ORDER BY total_flow_rate DESC) as rn
--     FROM nf_user_flow_statistics_v2
--     WHERE DATE(record_time) = CURDATE()
-- ) t WHERE rn <= 10;
