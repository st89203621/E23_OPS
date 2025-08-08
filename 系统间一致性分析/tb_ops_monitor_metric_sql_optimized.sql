/*
 优化后的tb_ops_monitor_metric_sql配置
 针对集群资源充足的情况，大幅提升Spark资源配置以获得最佳性能
 
 优化策略：
 1. 大幅增加Driver和Executor内存
 2. 增加Executor实例数和核心数
 3. 提升并行度
 4. 启用自适应查询执行
 5. 使用Kryo序列化器
 6. 增加Driver结果大小限制
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- 优化后的资源配置更新语句
-- ----------------------------

-- ID=1: ODS层Call/SMS/Fax协议数据量天任务 (多表UNION查询)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "16",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "128",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 1;

-- ID=2: PR中ODS层HTTP协议数据量天任务 (大数据量查询)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "20",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "160",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 2;

-- ID=3: ODS层PR-Http协议时延统计 (计算密集型)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "24",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "192",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 3;

-- ID=4: PR中ODS层IM/VPN/RemoteCtrl协议数据量天任务 (多表UNION)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "20",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "160",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 4;

-- ID=5: ODS层PR-IM/VPN/RemoteCtrl协议时延统计 (多表UNION + 计算)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "20",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "160",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 5;

-- ID=6: PR中ODS层数据量小时任务 (小时级任务，适中配置)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "8g",
    "spark.driver.cores": "4",
    "spark.executor.memory": "8G",
    "spark.executor.instances": "8",
    "spark.executor.cores": "4",
    "spark.default.parallelism": "32",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}' WHERE `id` = 6;

-- ID=7: Call/SMS/Fax协议ODS数据时延统计 (多表UNION)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "16",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "128",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 7;

-- ID=8: Radius数据ODS层时延统计 (多表UNION + 大数据量)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "24",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "192",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 8;

-- ID=9: PR中ODS层其他协议数据量天任务-1 (超大UNION查询，20+表)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "20g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "20G",
    "spark.executor.instances": "32",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "256",
    "spark.driver.maxResultSize": "12g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "500"
}' WHERE `id` = 9;

-- ID=10: PR中DWD层HTTP协议数据量天任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "20",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "160",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 10;

-- ID=11: PR中DWD层IM/VPN/RemoteCtrl协议数据量天任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "20",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "160",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 11;

-- ID=12: PR中DWD层其他协议数据量天任务 (多表UNION)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "20g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "20G",
    "spark.executor.instances": "28",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "224",
    "spark.driver.maxResultSize": "12g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "500"
}' WHERE `id` = 12;

-- ID=13: PR中DWD层数据量小时任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "8g",
    "spark.driver.cores": "4",
    "spark.executor.memory": "8G",
    "spark.executor.instances": "8",
    "spark.executor.cores": "4",
    "spark.default.parallelism": "32",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}' WHERE `id` = 13;

-- ID=18: DWD层Call/SMS/Fax协议数据量天任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "16",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "128",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 18;

-- ID=19: ODS层PR-HTTP协议数据量小时任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "8g",
    "spark.driver.cores": "4",
    "spark.executor.memory": "8G",
    "spark.executor.instances": "8",
    "spark.executor.cores": "4",
    "spark.default.parallelism": "32",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}' WHERE `id` = 19;

-- ID=20: Radius数据DWD层数据量天任务 (大数据量)
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "24",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "192",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 20;

-- ID=21: DWD层Call/SMS/Fax协议时延统计
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "16",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "128",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 21;

-- ID=22: Radius数据DWD层时延统计
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "16g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "16G",
    "spark.executor.instances": "24",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "192",
    "spark.driver.maxResultSize": "8g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "400"
}' WHERE `id` = 22;

-- ID=23: DWD层PR-HTTP协议数据量小时任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "8g",
    "spark.driver.cores": "4",
    "spark.executor.memory": "8G",
    "spark.executor.instances": "8",
    "spark.executor.cores": "4",
    "spark.default.parallelism": "32",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}' WHERE `id` = 23;

-- ID=24: DWD层Call/SMS/Fax协议数据量小时任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "8g",
    "spark.driver.cores": "4",
    "spark.executor.memory": "8G",
    "spark.executor.instances": "8",
    "spark.executor.cores": "4",
    "spark.default.parallelism": "32",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}' WHERE `id` = 24;

-- ID=25: Radius数据DWD层数据量小时任务
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "8g",
    "spark.driver.cores": "4",
    "spark.executor.memory": "8G",
    "spark.executor.instances": "12",
    "spark.executor.cores": "4",
    "spark.default.parallelism": "48",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}' WHERE `id` = 25;

-- 针对phone_archive_miss_rate_statistic等复杂查询的特殊优化
-- 这类查询涉及大量UNION和LATERAL VIEW操作，需要最高配置
UPDATE `tb_ops_monitor_metric_sql` SET `resource_conf` = '{
    "spark.driver.memory": "24g",
    "spark.driver.cores": "8",
    "spark.executor.memory": "24G",
    "spark.executor.instances": "40",
    "spark.executor.cores": "8",
    "spark.default.parallelism": "320",
    "spark.driver.maxResultSize": "16g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.shuffle.partitions": "600",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.executor.memoryFraction": "0.8",
    "spark.driver.memoryFraction": "0.8"
}' WHERE `description` LIKE '%phone_archive_miss_rate_statistic%' OR `sql_content` LIKE '%phone_archive_miss_rate_statistic%';

/*
资源配置优化说明：

1. 基础配置提升：
   - Driver内存：从2-8g提升到16-24g
   - Executor内存：从2-8G提升到16-24G
   - Executor实例数：从4-8提升到16-40
   - 并行度：从16-32提升到128-320

2. 特殊优化配置：
   - 启用自适应查询执行(AQE)
   - 启用分区合并优化
   - 使用Kryo序列化器提升性能
   - 增加Driver结果大小限制
   - 针对数据倾斜启用skewJoin优化

3. 分级配置策略：
   - 小时任务：中等配置(8g内存，8实例)
   - 天任务：高配置(16g内存，16-24实例)
   - 复杂UNION查询：超高配置(20-24g内存，28-40实例)
   - 特殊复杂查询：最高配置(24g内存，40实例)

4. 预期性能提升：
   - 整体查询性能提升3-5倍
   - 解决Driver结果大小超限问题
   - 大幅减少任务执行时间
   - 提升集群资源利用率
*/

SET FOREIGN_KEY_CHECKS = 1;
