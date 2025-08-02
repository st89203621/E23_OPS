
CREATE TABLE ods_fixnet_radius(
    account String,
    ip String,
    action String,
    mac String,
    session_id String,
    capture_time bigint,
    insert_time bigint,
    data_id String
) WITH (
    'type' = 'kafka',
    'topic' = 'ods_fixnet_radius',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'ods_store',
    'format.type' = 'avro',
    'security.mode.flag' = 'false',
    'enable.auto.commit' = 'false',
    'auto.commit.interval.ms' = '6000',
    'auto.offset.reset' = 'latest',
    'session.timeout.ms' = '30000',
    'feature.max.poll.records' = '1000',
    'feature.fetch.max.bytes' = '11534336',
    'feature.max.partition.fetch.bytes' = '11534336',
    'feature.fetch.message.max.bytes' = '11534336',
    'feature.request.timeout.ms' = '120000'
);


CREATE TABLE v64_deye_dw_ods.ods_fixnet_radius_store(
    account string,
    ip string,
    action string,
    mac string,
    session_id string,
    capture_time bigint,
    insert_time bigint,
    data_id string,
    capture_day string,
    capture_hour string
)WITH (
    'type' = 'hive',
    'catalog' = 'default',
    'database' = 'v64_deye_dw_ods',
    'config.directory' = '/usr/hdp/current/hive-client/conf/',
    'version' = '3.1.0',
    'createTable' = 'false',
    'jdbc.url' = 'jdbc:hive2://172.16.80.12:10000',
    'yarn-site.xml' = '/usr/hdp/current/hadoop-client/conf/yarn-site.xml',
    'core-site.xml' = '/usr/hdp/current/hadoop-client/conf/core-site.xml',
    'hdfs-site.xml' = '/usr/hdp/current/hadoop-client/conf/hdfs-site.xml',
    'url' = '172.16.80.12:10000',
    'security.mode.flag' = 'false',
    'partitioned' = 'capture_day String,capture_hour String',
    'sink.partition-commit.delay' = '1 min',
    'sink.partition-commit.trigger' = 'process-time',
    'sink.partition-commit.policy.kind' = 'metastore,success-file',
    'partition.time-extractor.timestamp-pattern' = '$capture_day $capture_hour:00:00'
);


insert into
    v64_deye_dw_ods.ods_fixnet_radius_store /*+ OPTIONS('sink.partition-commit.delay'='1 min','sink.partition-commit.trigger' = 'process-time','sink.partition-commit.policy.kind'='metastore,success-file','partition.time-extractor.timestamp-pattern'='$capture_day $capture_hour:00:00') */
select
    account,
    ip,
    `action`,
    mac,
    session_id,
    capture_time,
    unix_timestamp() * 1000 as insert_time,
    data_id,
    from_unixtime(capture_time/1000, 'yyyy-MM-dd') as capture_day,
    from_unixtime(capture_time/1000, 'HH')  as capture_hour
from ods_fixnet_radius
