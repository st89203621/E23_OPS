CREATE SCALAR FUNCTION deyeProduceRowKey WITH com.semptian.udf.fieldproduction.DeyeProduceRowKey;
CREATE SCALAR FUNCTION deyeRadiusCompleteFromHbaseV3 WITH com.semptian.udf.radius.DeyeRadiusCompleteFromHbaseV3;
CREATE AGGREGATE FUNCTION deyeMobileRadiusDuration WITH com.semptian.udf.radius.DeyeMobileRadiusDuration;
CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumber WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetCountryNameByPhoneNumber WITH com.semptian.udf.DeyeGetCountryNameByPhoneNumber;
CREATE SCALAR FUNCTION deyeCompleteBaseStation WITH com.semptian.udf.dimcomplete.DeyeCompleteBaseStation;
CREATE TABLE FUNCTION udf2UdtfV1 WITH com.semptian.udtf.common.Udf2UdtfV1;
CREATE SCALAR FUNCTION deyeCompleteUpareaTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteUpareaTypeMap;
CREATE SCALAR FUNCTION deyeCompleteAuthTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteAuthTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeGetMccAndMncByImsi WITH com.semptian.udf.mark.DeyeGetMccAndMncByImsi;
CREATE SCALAR FUNCTION deyeBase64Code WITH com.semptian.udf.fieldspecialhandle.DeyeBase64Code;
CREATE SCALAR FUNCTION deyeMobileStationNoDecode WITH com.semptian.udf.radius.DeyeMobileStationNoDecode;


CREATE TABLE ods_mobile_radius(
    capture_time Bigint,
    msg_type String,
    action String,
    user_name String,
    nas_ip_address String,
    nas_ipv6_address String,
    nas_identifier String,
    framed_ip_address String,
    internet_ip String,
    port_range String,
    framed_ip_netmask String,
    framed_ipv6_prefix String,
    session_timeout Integer,
    idle_timeout Integer,
    called_station_id String,
    calling_station_id String,
    input_octets Bigint,
    output_octets Bigint,
    session_id String,
    session_time Integer,
    input_packets Integer,
    output_packets Integer,
    terminate_causes String,
    input_gigawords Bigint,
    output_gigawords Bigint,
    imsi String,
    cg_address String,
    sgsn_address String,
    ggsn_address String,
    imsi_mcc_mnc String,
    ggsn_mcc_mnc String,
    session_stop_indicator String,
    cg_ipv6_address String,
    sgsn_ipv6_address String,
    ggsn_ipv6_address String,
    sgsn_mcc_mnc String,
    imeisv String,
    rat_type Integer,
    user_location_info String,
    ms_timezone Integer,
    user_location_info_time String,
    mapping_capture_time Bigint,
    data_id String,
    insert_time Bigint
) WITH (
    'type' = 'kafka',
    'topic' = 'ods_mobile_radius_mobilis',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'dwd_mobile_radius_mobilis',
    'format.type' = 'avro',
    'security.mode.flag' = 'false',
    'enable.auto.commit' = 'false',
    'auto.commit.interval.ms' = '6000',
    'auto.offset.reset' = 'latest',
    'session.timeout.ms' = '120000',
    'request.timeout.ms' = '120000',
    'feature.max.poll.records' = '1000',
    'feature.fetch.max.bytes' = '11534336',
    'feature.max.partition.fetch.bytes' = '11534336',
    'feature.fetch.message.max.bytes' = '11534336',
    'feature.request.timeout.ms' = '120000'
);

CREATE TABLE dwd_mobile_radius (
    data_id String,
    session_id String,
    calling_station_id String,
    called_station_id String,
    action String,
    framed_ip_address String,
    internet_ip String,
    port_range String,
    imeisv String,
    imsi String,
    login_time Bigint,
    logout_time Bigint,
    duration Bigint,
    rat_type Integer,
    user_location_info String,
    user_name String,
    base_station_address String,
    base_station_longitude Double,
    base_station_latitude Double,
    capture_time Bigint,
    auth_type String,
    auth_type_map String,
    auth_account String,
    action_map String,
    data_type String,
    data_type_map String,
    uparea_id String,
    uparea_id_map String,
    ip_type Integer,
    mac String,
    country_attribution String,
    country String,
    province_attribution String,
    province String,
    isp String,
    number_risk_level String,
    roam_type String,
    mark String,
    flag Integer,
    insert_time Bigint,
    norm_data_type String
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_mobile_radius_mobilis',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'security.mode.flag' = 'false',
    'format.type' = 'avro',
    'parallelism' = '32',
    'batch.size'='32768',
    'linger.ms'='100',
    'acks'='1',
    'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800'
);

insert into
    dwd_mobile_radius
select
    tmp.radius['data_id'] as data_id,
    tmp.radius['session_id'] as session_id,
    tmp.calling_station_id as calling_station_id,
    tmp.radius['called_station_id'] as called_station_id,
    case
        when tmp.radius['action'] = 'start' then '0'
        else '1'
    end as action,
    tmp.radius['framed_ip_address'] as framed_ip_address,
    tmp.radius['ip'] as internet_ip,
    tmp.radius['port_range'] as port_range,
    if(tmp.radius['imeisv'] = '-', '', tmp.radius['imeisv']) as imeisv,
    if(tmp.radius['imsi'] = '-', '', tmp.radius['imsi']) as imsi,
    case
        when tmp.radius['action'] = 'stop' and tmp.radius['flag'] = '0' and new_login_time > 0 then new_login_time
        else cast(tmp.radius['login_time'] as bigint)
    end as login_time,
    cast(tmp.radius['logout_time'] as bigint) as logout_time,
    case
        when tmp.radius['action'] = 'stop' and tmp.radius['flag'] = '0' and new_login_time > 0 then cast(tmp.radius['logout_time'] as bigint) - new_login_time
        else cast(tmp.radius['duration'] as bigint)
    end as duration,
    case
        when tmp.locations['network_type'] <> '0' then cast(tmp.locations['network_type'] as int)
        when tmp.radius['rat_type'] = '1' then 2
        when tmp.radius['rat_type'] = '2' then 1
        when tmp.radius['rat_type'] = '6' then 3
        else 0
    end as rat_type,
    if(tmp.radius['user_location_info'] = '-', '', deyeMobileStationNoDecode(tmp.radius['user_location_info'])) as user_location_info,
    if(tmp.radius['user_name'] = '-', '', deyeBase64Code(tmp.radius['user_name'],1)) as user_name,
    case
        when tmp.locations is not null then tmp.locations['address']
        else ''
    end as base_station_address,
    case
        when tmp.locations is not null then cast(tmp.locations['lon'] as double)
        else 0.0
    end as base_station_longitude,
    case
        when tmp.locations is not null then cast(tmp.locations['lat'] as double)
        else 0.0
    end as base_station_latitude,
    cast(tmp.radius['capture_time'] as bigint) as capture_time,
    '1020004' as auth_type,
    deyeCompleteAuthTypeMap('1020004') as auth_type_map,
    tmp.calling_station_id as auth_account,
    case
        when tmp.radius['action'] = 'start' then 'login'
        else 'logout'
    end as action_map,
    '2201' as data_type,
    deyeCompleteDataTypeMap('2201') as data_type_map,
    '210213' as uparea_id,
    deyeCompleteUpareaTypeMap('210213') as uparea_id_map,
    deyeProduceIpType(tmp.radius['ip']) as ip_type,
    '' as mac,
    deyeGetCountryCodeByPhoneNumber(tmp.calling_station_id) as country_attribution,
    deyeGetCountryNameByPhoneNumber(tmp.calling_station_id) as country,
    '' as province_attribution,
    '' as province,
    deyeGetMccAndMncByImsi(tmp.radius['imsi'],0) as isp,
    '' as number_risk_level,
    deyeGetRoamType(deyeGetCountryCodeByPhoneNumber(tmp.calling_station_id),'DZ')  as roam_type,
    '' as mark,
    cast(tmp.radius['flag'] as int) as flag,
    unix_timestamp() * 1000 as insert_time,
    '2201' as norm_data_type
from (
    select
        tmp2.calling_station_id,
        tmp2.radius,
        tmp2.locations,
        tmp3.new_login_time
    from (
        select
            tmp1.calling_station_id,
            t.radius,
            case
                when t.radius['user_location_info'] = '' or deyeMobileStationNoDecode(t.radius['user_location_info']) = '' then null
                else deyeCompleteBaseStation(deyeMobileStationNoDecode(t.radius['user_location_info']))
            end as locations
        from (
            select
                calling_station_id,
                deyeMobileRadiusDuration(session_id, capture_time, lower(action), called_station_id, imeisv, imsi, rat_type, user_location_info, user_name, data_id, framed_ip_address, internet_ip, port_range) as row_array
            from ods_mobile_radius
            where action in ('Start', 'Stop', 'start', 'stop')
            group by tumble(proctime(), INTERVAL '3' MINUTES), calling_station_id
        ) tmp1
        left join unnest(`row_array`) as t(radius) on true
    ) tmp2 ,lateral table(udf2UdtfV1(deyeRadiusCompleteFromHbaseV3(deyeProduceRowKey(tmp2.calling_station_id), tmp2.radius['session_id'], cast(tmp2.radius['capture_time'] as bigint),tmp2.radius['action'],tmp2.radius['flag']))) as tmp3(new_login_time)
)tmp;