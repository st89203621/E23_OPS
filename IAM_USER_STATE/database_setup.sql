-- =====================================================
-- IAM用户流速统计数据库设置脚本
-- 数据库服务器: 192.168.13.4
-- 用户: root/123456
-- 数据库: packets_statistics
-- 表: nf_user_flow_statistics_v2 (用户级别，新表)
-- 表: nf_device_flow_statistics (设备级别)
-- =====================================================

-- 1. 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS packets_statistics
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_unicode_ci;

-- 2. 使用数据库
USE packets_statistics;

-- 3. 创建用户级别流速统计表（新表）
CREATE TABLE IF NOT EXISTS nf_user_flow_statistics_v2 (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    machine_room VARCHAR(100) NOT NULL COMMENT '机房',
    device_ip VARCHAR(45) NOT NULL COMMENT '设备IP',
    device_type VARCHAR(10) NOT NULL COMMENT '设备类型',
    user_name VARCHAR(100) NOT NULL COMMENT '用户名',
    user_ip VARCHAR(45) NOT NULL COMMENT 'IP',
    up_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '上行Mbps',
    down_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '下行Mbps',
    total_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '总流速Mbps',
    session_count INT NOT NULL DEFAULT 0 COMMENT '会话数',
    record_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    INDEX idx_device_ip (device_ip),
    INDEX idx_user_ip (user_ip),
    INDEX idx_machine_room (machine_room),
    INDEX idx_device_type (device_type),
    INDEX idx_record_time (record_time),
    INDEX idx_total_flow_rate (total_flow_rate),
    INDEX idx_device_room (device_ip, machine_room)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='NF设备Top用户流速统计表V2（用户级别，新表）';

-- 4. 创建设备级别流速统计表（新增）
CREATE TABLE IF NOT EXISTS nf_device_flow_statistics (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    machine_room VARCHAR(100) NOT NULL COMMENT '机房',
    device_ip VARCHAR(45) NOT NULL COMMENT '设备IP',
    device_type VARCHAR(10) NOT NULL COMMENT '设备类型',
    up_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '上行Mbps',
    down_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '下行Mbps',
    total_flow_rate DECIMAL(12,3) NOT NULL DEFAULT 0 COMMENT '总流速Mbps',
    record_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录时间',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    INDEX idx_device_ip (device_ip),
    INDEX idx_machine_room (machine_room),
    INDEX idx_device_type (device_type),
    INDEX idx_record_time (record_time),
    INDEX idx_total_flow_rate (total_flow_rate),
    INDEX idx_device_room (device_ip, machine_room)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='NF设备流速统计表（设备级别）';

-- 5. 创建机房统计视图（用户级别）
CREATE OR REPLACE VIEW v_machine_room_user_stats AS
SELECT
    machine_room AS '机房',
    device_type AS '设备类型',
    COUNT(DISTINCT device_ip) AS '设备数量',
    COUNT(*) AS '用户数量',
    AVG(total_flow_rate) AS '平均流速_Mbps',
    MAX(total_flow_rate) AS '最大流速_Mbps',
    SUM(total_flow_rate) AS '总流速_Mbps',
    DATE(record_time) AS '统计日期'
FROM nf_user_flow_statistics_v2
GROUP BY machine_room, device_type, DATE(record_time)
ORDER BY DATE(record_time) DESC, machine_room, device_type;

-- 6. 创建设备统计视图（用户级别）
CREATE OR REPLACE VIEW v_device_user_stats AS
SELECT
    machine_room AS '机房',
    device_type AS '设备类型',
    device_ip AS '设备IP',
    COUNT(*) AS '用户数量',
    AVG(total_flow_rate) AS '平均流速_Mbps',
    MAX(total_flow_rate) AS '最大流速_Mbps',
    SUM(total_flow_rate) AS '总流速_Mbps',
    DATE(record_time) AS '统计日期'
FROM nf_user_flow_statistics_v2
GROUP BY machine_room, device_type, device_ip, DATE(record_time)
ORDER BY DATE(record_time) DESC, machine_room, total_flow_rate DESC;

-- 7. 创建Top用户视图
CREATE OR REPLACE VIEW v_top_users_flow AS
SELECT
    machine_room AS '机房',
    device_type AS '设备类型',
    device_ip AS '设备IP',
    user_name AS '用户名',
    user_ip AS 'IP',
    total_flow_rate AS '总流速_Mbps',
    up_flow_rate AS '上行流速_Mbps',
    down_flow_rate AS '下行流速_Mbps',
    session_count AS '会话数',
    record_time AS '记录时间'
FROM nf_user_flow_statistics_v2
ORDER BY DATE(record_time) DESC, machine_room, total_flow_rate DESC;

-- 8. 创建设备级别流速统计视图
CREATE OR REPLACE VIEW v_device_flow_stats AS
SELECT
    machine_room AS '机房',
    device_ip AS '设备IP',
    device_type AS '设备类型',
    up_flow_rate AS '上行Mbps',
    down_flow_rate AS '下行Mbps',
    total_flow_rate AS '总流速Mbps',
    record_time AS '记录时间'
FROM nf_device_flow_statistics
ORDER BY DATE(record_time) DESC, machine_room, total_flow_rate DESC;

-- =====================================================
-- 常用查询语句
-- =====================================================

-- 查询今日各机房统计（用户级别）
-- SELECT * FROM v_machine_room_user_stats WHERE 统计日期 = CURDATE();

-- 查询今日各设备统计（用户级别）
-- SELECT * FROM v_device_user_stats WHERE 统计日期 = CURDATE();

-- 查询今日Top用户
-- SELECT * FROM v_top_users_flow WHERE DATE(记录时间) = CURDATE();

-- 查询特定机房的Top用户
-- SELECT * FROM v_top_users_flow WHERE 机房 = 'A2' AND DATE(记录时间) = CURDATE();

-- 查询特定设备类型的统计
-- SELECT * FROM v_machine_room_user_stats WHERE 设备类型 = '25G' AND 统计日期 = CURDATE();

-- 查询今日设备级别流速统计
-- SELECT * FROM v_device_flow_stats WHERE DATE(记录时间) = CURDATE();

-- 清空今日数据（谨慎使用）
-- DELETE FROM nf_user_flow_statistics_v2 WHERE DATE(record_time) = CURDATE();
-- DELETE FROM nf_device_flow_statistics WHERE DATE(record_time) = CURDATE();

-- 查看表结构
-- DESCRIBE nf_user_flow_statistics_v2;
-- DESCRIBE nf_device_flow_statistics;

-- 查看索引
-- SHOW INDEX FROM nf_user_flow_statistics_v2;
-- SHOW INDEX FROM nf_device_flow_statistics;

-- =====================================================
-- 数据维护语句
-- =====================================================

-- 删除7天前的数据（数据清理）
-- DELETE FROM nf_user_flow_statistics_v2 WHERE record_time < DATE_SUB(NOW(), INTERVAL 7 DAY);
-- DELETE FROM nf_device_flow_statistics WHERE record_time < DATE_SUB(NOW(), INTERVAL 7 DAY);

-- 查看用户级别数据量统计
-- SELECT
--     DATE(record_time) as '日期',
--     COUNT(*) as '记录数',
--     COUNT(DISTINCT machine_room) as '机房数',
--     COUNT(DISTINCT device_ip) as '设备数'
-- FROM nf_user_flow_statistics_v2
-- GROUP BY DATE(record_time)
-- ORDER BY DATE(record_time) DESC;

-- 查看设备级别数据量统计
-- SELECT
--     DATE(record_time) as '日期',
--     COUNT(*) as '记录数',
--     COUNT(DISTINCT machine_room) as '机房数',
--     COUNT(DISTINCT device_ip) as '设备数'
-- FROM nf_device_flow_statistics
-- GROUP BY DATE(record_time)
-- ORDER BY DATE(record_time) DESC;

-- 查看存储空间使用
-- SELECT
--     table_name AS '表名',
--     ROUND(((data_length + index_length) / 1024 / 1024), 2) AS '大小_MB'
-- FROM information_schema.tables
-- WHERE table_schema = 'packets_statistics'
-- AND table_name IN ('nf_user_flow_statistics_v2', 'nf_device_flow_statistics');
