CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeVerifyIp WITH com.semptian.udf.verify.DeyeVerifyIp;
CREATE SCALAR FUNCTION deyeVerifyTimestamp WITH com.semptian.udf.timestamphandle.DeyeVerifyTimestamp;
CREATE SCALAR FUNCTION deyeCompleteActionTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteActionTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeCompleteChildTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteChildTypeMap;
CREATE SCALAR FUNCTION deyeGetDurationRange WITH com.semptian.udf.mark.DeyeGetDurationRange;
CREATE SCALAR FUNCTION deyeGetIspByPhoneNumber WITH com.semptian.udf.mark.DeyeGetIspByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.mark.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeGetSensitiveArea WITH com.semptian.udf.mark.DeyeGetSensitiveArea;
CREATE SCALAR FUNCTION deyeGetTerroristArea WITH com.semptian.udf.mark.DeyeGetTerroristArea;
CREATE SCALAR FUNCTION deyeGetTimeSpan WITH com.semptian.udf.mark.DeyeGetTimeSpan;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumberV2 WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumberV2;
CREATE SCALAR FUNCTION deyeProduceSpamFlag WITH com.semptian.udf.fieldproduction.DeyeProduceSpamFlag;
CREATE TABLE FUNCTION deyeIpAttributionComp WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionComp;
CREATE TABLE FUNCTION deyeIpAttributionCompV2 WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionCompV2;
CREATE SCALAR FUNCTION deyePhoneNumberNorm WITH com.semptian.udf.normalization.DeyePhoneNumberNorm;
CREATE TABLE FUNCTION udf2Udtf WITH com.semptian.udtf.common.Udf2Udtf;
CREATE SCALAR FUNCTION deyeNetworkTypeParse WITH com.semptian.udf.fieldspecialhandle.DeyeNetworkTypeParse;
CREATE SCALAR FUNCTION deyeGetMurmurHash WITH com.semptian.udf.DeyeGetMurmurHash;

CREATE TABLE deye_ods_voip (
    call_id String,
    a_number String,
    a_sip_uri String,
    a_area_code String,
    a_imsi String,
    a_imei String,
    a_neid String,
    a_mcc String,
    a_mnc String,
    a_begin_region String,
    a_begin_cell String,
    a_end_region String,
    a_end_cell String,
    a_point_code String,
    b_number String,
    b_format_number String,
    b_area_code String,
    b_sip_uri String,
    b_orginal_forword_number String,
    b_imsi String,
    b_neid String,
    b_point_code String,
    `action` String,
    start_time Bigint,
    end_time Bigint,
    duration Integer,
    file_path String,
    extend_column String,
    call_type Integer,
    sip_code String,
    isp_id Integer,
    strsrc_ip String,
    strdst_ip String,
    strrtp_ip String,
    third_numbergroup String,
    capture_time Bigint,
    insert_time Bigint,
    data_id String,
    data_type Integer,
    fault_flag Integer
) WITH (
    'type' = 'kafka',
    'topic' = 'deye_ods_voip',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'dwd_call',
    'security.mode.flag' = 'false',
    'session.timeout.ms' = '30000',
    'enable.auto.commit' = 'false',
    'auto.offset.reset' = 'latest',
    'feature.max.poll.records' = '1000',
    'feature.fetch.max.bytes' = '11534336',
    'feature.max.partition.fetch.bytes' = '11534336',
    'feature.fetch.message.max.bytes' = '11534336',
    'feature.request.timeout.ms' = '120000',
    'format.type' = 'avro',
    'parallelism' = '8'
);

CREATE TABLE dwd_call (
    data_type String,
    child_type String,
    strsrc_ip String,
    strdst_ip String,
    src_ip_address String,
    src_ip_country String,
    src_ip_province String,
    src_ip_city String,
    dst_ip_address String,
    dst_ip_country String,
    dst_ip_province String,
    dst_ip_city String,
    src_ip_longitude Double,
    src_ip_latitude Double,
    dst_ip_longitude Double,
    dst_ip_latitude Double,
    src_ip_type Integer,
    dst_ip_type Integer,
    strrtp_ip String,
    sender_phone String,
    receiver_phone String,
    third_numbergroup String,
    start_time Bigint,
    end_time Bigint,
    continue_time Integer,
    calling_atrribution String,
    called_atrribution String,
    file_path String,
    `action` String,
    collect_place String,
    protocol String,
    capture_time Bigint,
    capture_day String,
    capture_hour Integer,
    insert_time Bigint,
    data_id String,
    data_type_map String,
    child_type_map String,
    action_map String,
    mark String,
    calling_imei String,
    calling_imsi String,
    called_imei String,
    called_imsi String,
    calling_country_attribution String,
    called_country_attribution String,
    calling_province_attribution String,
    called_province_attribution String,
    calling_country_area_code String,
    called_country_area_code String,
    calling_isp String,
    called_isp String,
    time_span String,
    calling_number_risk_level String,
    called_number_risk_level String,
    roam_type String,
    duration_range String,
    call_type String,
    call_type_map String,
    calling_terrorist_area String,
    called_terrorist_area String,
    calling_sensitive_area String,
    called_sensitive_area String,
    norm_data_type String,
    spam_flag Integer,
    calling_network_type String,
    called_network_type String,
    call_id Bigint,
    calling_roam String,
    called_roam String
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_call',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'security.mode.flag' = 'false',
    'format.type' = 'avro',
    'batch.size'='1048576',
    'acks'='1',
    'linger.ms'='100',
    'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800',
    'parallelism' = '4'
);

CREATE TABLE dwd_pr_tmp (
    data_id String,
    data_type String,
    sender_phone String,
    receiver_phone String,
    capture_time Bigint,
    capture_day String,
    capture_hour Integer,
    insert_time Bigint
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_pr_tmp',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'security.mode.flag' = 'false',
    'format.type' = 'json',
    'batch.size'='1048576',
    'acks'='1',
    'linger.ms'='100',
    'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800',
    'parallelism' = '2'
);

INSERT INTO
    dwd_pr_tmp
SELECT
    data_id as data_id,
    CAST(data_type AS VARCHAR) as data_type,
    T21.a_number_l as sender_phone,
    T22.b_number_l as receiver_phone,
    capture_time as capture_time,
    from_unixtime(capture_time/1000, 'yyyy-MM-dd') as capture_day,
    CAST(from_unixtime(capture_time/1000, 'H') AS INT) as capture_hour,
    unix_timestamp()*1000 as insert_time
FROM
    deye_ods_voip,
  LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(a_number))) as T21(a_number_l),
  LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(b_number))) as T22(b_number_l)
WHERE
    (
    (data_type = 109)
  --AND (deyeVerifyTimestamp(capture_time, -2) = '1')
  --AND (deyeVerifyTimestamp(capture_time, 1) = '0')
  AND (deyeVerifyIp(strsrc_ip, '') = '1')
  AND (deyeVerifyIp(strdst_ip, '') = '1')
    )
  AND
  (
  CHAR_LENGTH(T21.a_number_l) > 30
  OR CHAR_LENGTH(T22.b_number_l) > 30
  );


insert into dwd_call
select
    tmp.data_type as data_type,
    '1099999' as child_type,
    tmp.strsrc_ip as strsrc_ip,
    tmp.strdst_ip as strdst_ip,
    tmp.src_ip_address as src_ip_address,
    tmp.src_ip_country as src_ip_country,
    tmp.src_ip_province as src_ip_province,
    tmp.src_ip_city as src_ip_city,
    tmp.dst_ip_address as dst_ip_address,
    tmp.dst_ip_country as dst_ip_country,
    tmp.dst_ip_province as dst_ip_province,
    tmp.dst_ip_city as dst_ip_city,
    tmp.src_ip_longitude as src_ip_longitude,
    tmp.src_ip_latitude as src_ip_latitude,
    tmp.dst_ip_longitude as dst_ip_longitude,
    tmp.dst_ip_latitude as dst_ip_latitude,
    tmp.src_ip_type as src_ip_type,
    tmp.dst_ip_type as dst_ip_type,
    tmp.strrtp_ip as strrtp_ip,
    tmp.sender_phone as sender_phone,
    tmp.receiver_phone as receiver_phone,
    tmp.third_numbergroup as third_numbergroup,
    tmp.start_time as start_time,
    tmp.end_time as end_time,
    tmp.continue_time as continue_time,
    tmp.a_belong_country_code as calling_atrribution,
    tmp.b_belong_country_code as called_atrribution,
    tmp.file_path as file_path,
    tmp.action as `action`,
    '' as collect_place,
    '' as protocol,
    tmp.capture_time as capture_time,
    from_unixtime(capture_time/1000, 'yyyy-MM-dd') as capture_day,
    CAST(from_unixtime(capture_time/1000, 'H') AS INT) as capture_hour,
    unix_timestamp()*1000 as insert_time,
    tmp.data_id as data_id,
    tmp.data_type_map as data_type_map,
    tmp.child_type_map as child_type_map,
    tmp.action_map as action_map,
    '' as mark,
    tmp.calling_imei as calling_imei,
    tmp.calling_imsi as calling_imsi,
    '' as called_imei,
    tmp.called_imsi as called_imsi,
    tmp.a_belong_country_code as calling_country_attribution,
    tmp.b_belong_country_code as called_country_attribution,
    '' as calling_province_attribution,
    '' as called_province_attribution,
    tmp.calling_country_area_code as calling_country_area_code,
    tmp.called_country_area_code as called_country_area_code,
    tmp.calling_isp as calling_isp,
    tmp.called_isp as called_isp,
    tmp.time_span as time_span,
    '' as calling_number_risk_level,
    '' as called_number_risk_level,
    coalesce(
        nullif(deyeGetRoamType(tmp.a_belong_country_code,tmp.calling_country_area_code),''),
        nullif(deyeGetRoamType(tmp.b_belong_country_code,tmp.called_country_area_code),''),
        ''
        ) as roam_type,
    tmp.duration_range as duration_range,
    '' as call_type,
    '' as call_type_map,
    deyeGetTerroristArea(tmp.a_belong_country_code,tmp.calling_country_area_code,'') as calling_terrorist_area,
    deyeGetTerroristArea(tmp.b_belong_country_code,tmp.called_country_area_code,'') as called_terrorist_area,
    deyeGetSensitiveArea(tmp.a_belong_country_code,tmp.calling_country_area_code,'') as calling_sensitive_area,
    deyeGetSensitiveArea(tmp.b_belong_country_code,tmp.called_country_area_code,'') as called_sensitive_area,
    tmp.norm_data_type as norm_data_type,
    tmp.spam_flag as spam_flag,
    tmp.calling_network_type as calling_network_type,
    tmp.called_network_type as called_network_type,
    tmp.call_id as call_id,
    deyeGetRoamType(tmp.a_belong_country_code,tmp.calling_country_area_code) as calling_roam,
    deyeGetRoamType(tmp.b_belong_country_code,tmp.called_country_area_code) as called_roam
from (
    select
        cast(data_type as varchar) as data_type,
        strsrc_ip,
        strdst_ip,
        T1.src_ip_address_l as src_ip_address,
        T1.src_ip_country_l as src_ip_country,
        T1.src_ip_province_l as src_ip_province,
        T1.src_ip_city_l as src_ip_city,
        T2.dst_ip_address_l as dst_ip_address,
        T2.dst_ip_country_l as dst_ip_country,
        T2.dst_ip_province_l as dst_ip_province,
        T2.dst_ip_city_l as dst_ip_city,
        T1.src_ip_longitude_l as src_ip_longitude,
        T1.src_ip_latitude_l as src_ip_latitude,
        T2.dst_ip_longitude_l as dst_ip_longitude,
        T2.dst_ip_latitude_l as dst_ip_latitude,
        deyeProduceIpType(strsrc_ip, '') as src_ip_type,
        deyeProduceIpType(strdst_ip, '') as dst_ip_type,
        strrtp_ip,
        T21.a_number_l as sender_phone,
        T22.b_number_l as receiver_phone,
        deyePhoneNumberNorm(third_numbergroup,'.') as third_numbergroup,
        start_time,
        end_time,
        duration as continue_time,
        file_path,
        deyeCompleteActionTypeMap(`action`,CAST(data_type AS VARCHAR))[1] as `action`,
        capture_time,
        data_id,
        deyeCompleteDataTypeMap(CAST(data_type AS VARCHAR), true)[2] as data_type_map,
        deyeCompleteChildTypeMap('1099999')[2] as child_type_map,
        deyeCompleteActionTypeMap(`action`,CAST(data_type AS VARCHAR))[2] as action_map,
        a_imei as calling_imei,
        a_imsi as calling_imsi,
        b_imsi as called_imsi,
        T1.src_ip_country_code_l as calling_country_area_code,
        T2.dst_ip_country_code_l as called_country_area_code,
        deyeGetIspByPhoneNumber(T21.a_number_l) as calling_isp,
        deyeGetIspByPhoneNumber(T22.b_number_l) as called_isp,
        deyeGetTimeSpan(capture_time) as time_span,
        deyeGetDurationRange(duration) as duration_range,
        deyeCompleteDataTypeMap(CAST(data_type AS VARCHAR), true)[1] as norm_data_type,
        deyeProduceSpamFlag(T21.a_number_l, T22.b_number_l) as spam_flag,
        deyeNetworkTypeParse(a_sip_uri) as calling_network_type,
        deyeNetworkTypeParse(b_sip_uri) as called_network_type,
        deyeGetMurmurHash(call_id) as call_id,
        deyeGetCountryCodeByPhoneNumberV2(a_number,'',T1.src_ip_country_code_l) as a_belong_country_code,
        deyeGetCountryCodeByPhoneNumberV2(b_number,b_format_number,T2.dst_ip_country_code_l) as b_belong_country_code
    from
        deye_ods_voip,
        LATERAL TABLE(deyeIpAttributionCompV2(strsrc_ip)) as T1(
            src_ip_address_l,
            src_ip_country_l,
            src_ip_province_l,
            src_ip_city_l,
            src_ip_longitude_l,
            src_ip_latitude_l,
            src_ip_country_code_l
        ),
        LATERAL TABLE(deyeIpAttributionCompV2(strdst_ip)) as T2(
            dst_ip_address_l,
            dst_ip_country_l,
            dst_ip_province_l,
            dst_ip_city_l,
            dst_ip_longitude_l,
            dst_ip_latitude_l,
            dst_ip_country_code_l
        ),
        LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(a_number))) as T21(a_number_l),
        LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(b_number))) as T22(b_number_l)
    where
        data_type = 109
        and (a_number <> '' and b_number <> '')
        and deyeVerifyIp(strsrc_ip, '') = '1'
        and deyeVerifyIp(strdst_ip, '') = '1'
        and CHAR_LENGTH(T21.a_number_l) <= 30
        and CHAR_LENGTH(T22.b_number_l) <= 30
) tmp;
