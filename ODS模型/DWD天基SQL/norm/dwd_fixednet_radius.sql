CREATE SCALAR FUNCTION deyeProduceRowKey WITH com.semptian.udf.fieldproduction.DeyeProduceRowKey;
CREATE SCALAR FUNCTION deyeRadiusCompleteFromHbase WITH com.semptian.udf.radius.DeyeRadiusCompleteFromHbase;
CREATE AGGREGATE FUNCTION deyeFixedRadiusDuration WITH com.semptian.udf.radius.DeyeFixedRadiusDuration;
CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeCompleteUpareaTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteUpareaTypeMap;
CREATE SCALAR FUNCTION deyeCompleteAuthTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteAuthTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeMacNorm WITH com.semptian.udf.normalization.DeyeMacNorm;


CREATE TABLE ods_fixnet_radius
(
    account String,
    ip String,
    action String,
    mac String,
    session_id String,
    capture_time Bigint,
    insert_time Bigint,
    data_id String
) WITH (
    'type' = 'kafka',
    'topic' = 'ods_fixnet_radius',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'dwd_clean',
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

CREATE TABLE dwd_fixnet_radius (
    data_id String,
    auth_account String,
    action String,
    internet_ip String,
    session_id String,
    login_time Bigint,
    logout_time Bigint,
    duration Bigint,
    capture_time Bigint,
    mac String,
    auth_type String,
    auth_type_map String,
    action_map String,
    data_type String,
    data_type_map String,
    uparea_id String,
    uparea_id_map String,
    user_name String,
    ip_type int,
    account_risk_level String,
    mark String,
    insert_time Bigint,
    flag int,
    norm_data_type String
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_fixnet_radius',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'security.mode.flag' = 'false',
    'format.type' = 'avro',
    'parallelism' = '4',
	'batch.size'='1048576',
	'linger.ms'='100',
	'acks'='1',
	'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800'
);

insert into dwd_fixnet_radius
select
    tmp.radius['data_id'] as data_id,
    tmp.account as auth_account,
    case
        when tmp.radius['action'] = 'login' then '0'
        else '1'
    end as `action`,
    tmp.radius['ip'] as internet_ip,
    tmp.radius['session_id'] as session_id,
    case
        when tmp.radius['action'] = 'logout' and tmp.radius['flag'] = '0' and new_login_time > 0 then new_login_time
        else cast(tmp.radius['login_time'] as bigint)
    end as login_time,
    cast(tmp.radius['logout_time'] as bigint) as logout_time,
    case
        when tmp.radius['action'] = 'logout' and tmp.radius['flag'] = '0' and new_login_time > 0 then cast(tmp.radius['logout_time'] as bigint) - new_login_time
        else cast(tmp.radius['duration'] as bigint)
    end as duration,
    cast(tmp.radius['capture_time'] as bigint) as capture_time,
    deyeMacNorm(tmp.radius['mac']) as mac,
    '1020001' as auth_type,
    deyeCompleteAuthTypeMap('1020001') as auth_type_map,
    tmp.radius['action'] as action_map,
    '2101' as data_type,
    deyeCompleteDataTypeMap('2101') as data_type_map,
    '210213' as uparea_id,
    deyeCompleteUpareaTypeMap('210213') as uparea_id_map,
    tmp.account as user_name,
    deyeProduceIpType(tmp.radius['ip']) as ip_type,
    '' as account_risk_level,
    '' as mark,
    unix_timestamp() * 1000 as insert_time,
    cast(tmp.radius['flag'] as int) as flag,
    '2101' as norm_data_type
from (
    select
        tmp1.account,
        t.radius,
        case
            when t.radius['action'] = 'logout' and t.radius['flag'] = '0' then deyeRadiusCompleteFromHbase(deyeProduceRowKey(tmp1.account), t.radius['session_id'], cast(t.radius['capture_time'] as bigint))
            else 0
        end as new_login_time
    from (
        select
            account,
            deyeFixedRadiusDuration(session_id, ip, capture_time, action, data_id, mac) as row_array
        from ods_fixnet_radius
        WHERE action in ('login', 'logout')
        group by tumble(proctime(), INTERVAL '3' MINUTES), account
    ) tmp1
    left join unnest(`row_array`) as t(radius) on true
) tmp;