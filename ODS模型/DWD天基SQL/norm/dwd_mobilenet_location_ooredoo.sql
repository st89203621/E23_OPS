CREATE SCALAR FUNCTION deyeProduceRowKey WITH com.semptian.udf.fieldproduction.DeyeProduceRowKey;
CREATE SCALAR FUNCTION deyeRadiusCompleteFromHbase WITH com.semptian.udf.radius.DeyeRadiusCompleteFromHbase;
CREATE AGGREGATE FUNCTION deyeMobileStationLocationV2 WITH com.semptian.udf.radius.DeyeMobileStationLocationV2;
CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumber WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetCountryNameByPhoneNumber WITH com.semptian.udf.DeyeGetCountryNameByPhoneNumber;
CREATE SCALAR FUNCTION deyeCompleteBaseStation WITH com.semptian.udf.dimcomplete.DeyeCompleteBaseStation;
CREATE SCALAR FUNCTION deyeCompleteUpareaTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteUpareaTypeMap;
CREATE SCALAR FUNCTION deyeCompleteAuthTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteAuthTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeGetMccAndMncByImsi WITH com.semptian.udf.mark.DeyeGetMccAndMncByImsi;
CREATE SCALAR FUNCTION deyeCompleteIspTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteIspTypeMap;
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
    'topic' = 'ods_mobile_radius_ooredoo',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'dwd_mobilenet_location_ooredoo',
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

CREATE TABLE dwd_mobilenet_location (
    phone_number String,
    session_id String,
    station_no String,
    network_type Integer,
    station_longitude Double,
    station_latitude Double,
    address String,
    imeisv String,
    imsi String,
    imsi_mcc_mnc String,
    ggsn_mcc_mnc String,
    framed_ip String,
    internet_ip String,
    port_range String,
    action String,
    user_name String,
    capture_time Bigint,
    capture_day String,
    capture_hour Integer,
    data_id String,
    insert_time Bigint,
    auth_type String,
    auth_type_map String,
    data_type String,
    data_type_map String,
    isp_id String,
    isp_id_map String,
    uparea_id String,
    uparea_id_map String,
    roming_type String,
    ip_type Integer,
    country_attribution_latitude String,
    province_attribution_latitude String,
    isp_latitude String,
    number_risk_level_latitude String,
    roam_latitude String,
    mark String,
    norm_data_type String
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_mobilenet_location_ooredoo',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'security.mode.flag' = 'false',
    'format.type' = 'avro',
    'parallelism' = '4',
    'batch.size'='32768',
    'linger.ms'='100',
    'acks'='1',
    'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800'
);

insert into dwd_mobilenet_location
select tmp.phone_number as phone_number
    ,tmp.radius['session_id'] as session_id
    ,if(tmp.radius['user_location_info'] = '-', '', deyeMobileStationNoDecode(tmp.radius['user_location_info'])) as station_no
    ,case when tmp.locations['network_type'] <> '0' then cast(tmp.locations['network_type'] as int)
          when tmp.radius['rat_type'] = '1' then 2
          when tmp.radius['rat_type'] = '2' then 1
          when tmp.radius['rat_type'] = '6' then 3
          else 0
     end as rat_type
    ,case when tmp.locations is not null then cast(tmp.locations['lon'] as double)
          else 0.0
     end as station_longitude
    ,case when tmp.locations is not null then cast(tmp.locations['lat'] as double)
         else 0.0
     end as station_latitude
    ,case when tmp.locations is not null then tmp.locations['address']
          else ''
     end as address
    ,if(tmp.radius['imeisv'] = '-', '', tmp.radius['imeisv']) as imeisv
    ,if(tmp.radius['imsi'] = '-', '', tmp.radius['imsi']) as imsi
    ,if(tmp.radius['imsi_mcc_mnc'] = '-', '', tmp.radius['imsi_mcc_mnc']) as imsi_mcc_mnc
    ,if(tmp.radius['ggsn_mcc_mnc'] = '-', '', tmp.radius['ggsn_mcc_mnc']) as ggsn_mcc_mnc
    ,tmp.radius['framed_ip_address'] as framed_ip
    ,tmp.radius['ip'] as internet_ip
    ,tmp.radius['port_range'] as port_range
    ,case when tmp.radius['action'] in ('start', 'stop') then tmp.radius['action'] else 'update' end as action
    ,if(tmp.radius['user_name'] = '-', '', deyeBase64Code(tmp.radius['user_name'],1)) as user_name
    ,cast(tmp.radius['capture_time'] as bigint) as capture_time
    ,from_unixtime(cast(tmp.radius['capture_time'] as bigint)/1000,'yyyy-MM-dd') as capture_day
    ,cast(from_unixtime(cast(tmp.radius['capture_time'] as bigint)/1000,'HH') as int) as capture_hour
    ,tmp.radius['data_id'] as data_id
    ,unix_timestamp() * 1000 as insert_time
    ,'1020004' as auth_type
    ,deyeCompleteAuthTypeMap('1020004') as auth_type_map
    ,'2202' as data_type
    ,deyeCompleteDataTypeMap('2202') as data_type_map
    ,deyeGetMccAndMncByImsi(tmp.radius['imsi'],0) as isp_id
    ,deyeCompleteIspTypeMap(deyeGetMccAndMncByImsi(tmp.radius['imsi'],0)) as isp_id_map
    ,'210213' as uparea_id
    ,deyeCompleteUpareaTypeMap('210213') as uparea_id_map
    ,deyeGetRoamType(deyeGetCountryCodeByPhoneNumber(tmp.phone_number),'DZ') as roming_type
    ,deyeProduceIpType(tmp.radius['ip']) as ip_type
    ,deyeGetCountryCodeByPhoneNumber(tmp.phone_number) as country_attribution_latitude
    ,'' as province_attribution_latitude
    ,'' as isp_latitude
    ,'' as number_risk_level_latitude
    ,deyeGetRoamType(deyeGetCountryCodeByPhoneNumber(tmp.phone_number),'DZ') as roam_latitude
    ,'' as mark
    ,'2202' as norm_data_type
from (
    select tmp1.phone_number
       ,t.radius
       ,case
            when t.radius['user_location_info'] = '' or deyeMobileStationNoDecode(t.radius['user_location_info'])='' then null
            else deyeCompleteBaseStation(deyeMobileStationNoDecode(t.radius['user_location_info']))
        end as locations
    from (
        select calling_station_id as phone_number
            ,deyeMobileStationLocationV2(lower(session_id), capture_time, lower(action), imeisv, imsi, rat_type,
               user_location_info, user_name, data_id, framed_ip_address, internet_ip, port_range,
               imsi_mcc_mnc, ggsn_mcc_mnc) as row_array
        from ods_mobile_radius
        group by tumble(proctime(), INTERVAL '2' MINUTES), calling_station_id
    ) tmp1
    left join unnest(`row_array`) as t(radius) on true
) tmp;