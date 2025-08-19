CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeVerifyIp WITH com.semptian.udf.verify.DeyeVerifyIp;
CREATE SCALAR FUNCTION deyeVerifyTimestamp WITH com.semptian.udf.timestamphandle.DeyeVerifyTimestamp;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeGetIspByPhoneNumber WITH com.semptian.udf.mark.DeyeGetIspByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.mark.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeGetSensitiveArea WITH com.semptian.udf.mark.DeyeGetSensitiveArea;
CREATE SCALAR FUNCTION deyeGetTerroristArea WITH com.semptian.udf.mark.DeyeGetTerroristArea;
CREATE SCALAR FUNCTION deyeGetTimeSpan WITH com.semptian.udf.mark.DeyeGetTimeSpan;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumberV2 WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumberV2;
CREATE SCALAR FUNCTION deyeProduceSpamFlag WITH com.semptian.udf.fieldproduction.DeyeProduceSpamFlag;
CREATE SCALAR FUNCTION deyeGetFaxFileSize WITH com.semptian.udf.call.DeyeGetFaxFileSize;
CREATE TABLE FUNCTION deyeIpAttributionComp WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionComp;
CREATE TABLE FUNCTION deyeIpAttributionCompV2 WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionCompV2;
CREATE SCALAR FUNCTION deyePhoneNumberNorm WITH com.semptian.udf.normalization.DeyePhoneNumberNorm;
CREATE TABLE FUNCTION udf2Udtf WITH com.semptian.udtf.common.Udf2Udtf;
CREATE SCALAR FUNCTION deyeSipNumberNorm WITH com.semptian.udf.normalization.DeyeSipNumberNorm;
CREATE SCALAR FUNCTION deyeNetworkTypeParse WITH com.semptian.udf.fieldspecialhandle.DeyeNetworkTypeParse;

CREATE TABLE deye_ods_voip_fax (
    strsrc_ip String,
    strdst_ip String,
    sender_from_url String,
    receiver_to_url String,
    sender_sip_number String,
    receiver_sip_number String,
    call_id String,
    sender_nickname String,
    receiver_nickname String,
    start_time Bigint,
    end_time Bigint,
    duration Integer,
    media_type String,
    media_protocol String,
    media_format String,
    file_path String,
    file_size Integer,
    capture_time Bigint,
    insert_time Bigint,
    data_id String,
    data_type Integer,
    fault_flag Integer
) WITH (
    'type' = 'kafka',
    'topic' = 'deye_ods_voip_fax',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'dwd_fax',
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

CREATE TABLE dwd_fax (
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
    sender_from_url String,
    receiver_to_url String,
    sender_sip_number String,
    receiver_sip_number String,
    call_id String,
    sender_nickname String,
    receiver_nickname String,
    start_time Long,
    end_time Long,
    duration Integer,
    media_type String,
    media_protocol String,
    media_format String,
    file_path String,
    file_size Integer,
    capture_time Long,
    capture_day String,
    capture_hour Integer,
    insert_time Long,
    data_id String,
    data_type String,
    data_type_map String,
    sender_attribution String,
    receiver_attribution String,
    mark String,
    calling_country_attribution String,
    calling_province_attribution String,
    called_country_attribution String,
    called_province_attribution String,
    calling_country_area_code String,
    called_country_area_code String,
    calling_isp String,
    called_isp String,
    time_span String,
    calling_number_risk_level String,
    called_number_risk_level String,
    roam_type String,
    fax_content String,
    calling_terrorist_area String,
    called_terrorist_area String,
    calling_sensitive_area String,
    called_sensitive_area String,
    norm_data_type String,
    spam_flag Integer,
    calling_network_type String,
    called_network_type String,
    calling_roam String,
    called_roam String
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_fax',
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
    T21.sender_sip_number_l as sender_sip_number,
    T22.receiver_sip_number_l as receiver_sip_number,
    capture_time as capture_time,
    from_unixtime(capture_time/1000, 'yyyy-MM-dd') as capture_day,
    CAST(from_unixtime(capture_time/1000, 'H') AS INT) as capture_hour,
    unix_timestamp()*1000 as insert_time
FROM
    deye_ods_voip_fax,
  LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(sender_sip_number))) as T21(sender_sip_number_l),
  LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(receiver_sip_number))) as T22(receiver_sip_number_l)
WHERE
    (
    (data_type = 210)
  --AND (deyeVerifyTimestamp(capture_time, -2) = '1')
  --AND (deyeVerifyTimestamp(capture_time, 1) = '0')
  AND (deyeVerifyIp(strsrc_ip, '') = '1')
  AND (deyeVerifyIp(strdst_ip, '') = '1')
    )
  AND
    (
    CHAR_LENGTH(T21.sender_sip_number_l) > 30
   OR CHAR_LENGTH(T22.receiver_sip_number_l) > 30
    );


insert into dwd_fax
select
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
    tmp.sender_from_url as sender_from_url,
    tmp.receiver_to_url as receiver_to_url,
    tmp.sender_sip_number as sender_sip_number,
    tmp.receiver_sip_number as receiver_sip_number,
    tmp.call_id as call_id,
    tmp.sender_nickname as sender_nickname,
    tmp.receiver_nickname as receiver_nickname,
    tmp.start_time as start_time,
    tmp.end_time as end_time,
    tmp.duration as duration,
    tmp.media_type as media_type,
    tmp.media_protocol as media_protocol,
    tmp.media_format as media_format,
    tmp.file_path as file_path,
    tmp.file_size as file_size,
    tmp.capture_time as capture_time,
    from_unixtime(tmp.capture_time/1000, 'yyyy-MM-dd') as capture_day,
    CAST(from_unixtime(tmp.capture_time/1000, 'H') AS INT) as capture_hour,
    unix_timestamp()*1000 as insert_time,
    tmp.data_id as data_id,
    tmp.data_type as data_type,
    tmp.data_type_map as data_type_map,
    '' as sender_attribution,
    '' as receiver_attribution,
    '' as mark,
    tmp.a_belong_country_code as calling_country_attribution,
    '' as calling_province_attribution,
    tmp.b_belong_country_code as called_country_attribution,
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
    '' as fax_content,
    deyeGetTerroristArea(tmp.a_belong_country_code,tmp.calling_country_area_code,'') as calling_terrorist_area,
    deyeGetTerroristArea(tmp.b_belong_country_code,tmp.called_country_area_code,'') as called_terrorist_area,
    deyeGetSensitiveArea(tmp.a_belong_country_code,tmp.calling_country_area_code,'') as calling_sensitive_area,
    deyeGetSensitiveArea(tmp.b_belong_country_code,tmp.called_country_area_code,'') as called_sensitive_area,
    tmp.norm_data_type as norm_data_type,
    tmp.spam_flag as spam_flag,
    tmp.calling_network_type as calling_network_type,
    tmp.called_network_type as called_network_type,
    deyeGetRoamType(tmp.a_belong_country_code,tmp.calling_country_area_code) as calling_roam,
    deyeGetRoamType(tmp.b_belong_country_code,tmp.called_country_area_code) as called_roam
from (
    select
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
        deyeSipNumberNorm(sender_from_url) as sender_from_url,
        deyeSipNumberNorm(receiver_to_url) as receiver_to_url,
        T21.sender_sip_number_l as sender_sip_number,
        T22.receiver_sip_number_l as receiver_sip_number,
        call_id,
        sender_nickname,
        receiver_nickname,
        start_time,
        end_time,
        duration,
        media_type,
        media_protocol,
        media_format,
        file_path,
        CAST(deyeGetFaxFileSize(file_path, '2/', 'http://192.168.80.244/mass/') as INT) as file_size,
        capture_time,
        data_id,
        CAST(data_type AS VARCHAR) as data_type,
        deyeCompleteDataTypeMap(CAST(data_type AS VARCHAR), true)[2] as data_type_map,
        deyeGetCountryCodeByPhoneNumberV2(sender_sip_number,'',T1.src_ip_country_code_l) as a_belong_country_code,
        deyeGetCountryCodeByPhoneNumberV2(receiver_sip_number,'',T2.dst_ip_country_code_l) as b_belong_country_code,
        T1.src_ip_country_code_l as calling_country_area_code,
        T2.dst_ip_country_code_l as called_country_area_code,
        deyeGetIspByPhoneNumber(T21.sender_sip_number_l) as calling_isp,
        deyeGetIspByPhoneNumber(T22.receiver_sip_number_l) as called_isp,
        deyeGetTimeSpan(capture_time) as time_span,
        deyeCompleteDataTypeMap(CAST(data_type AS VARCHAR), true)[1] as norm_data_type,
        deyeProduceSpamFlag(T21.sender_sip_number_l, T22.receiver_sip_number_l) as spam_flag,
        deyeNetworkTypeParse(sender_from_url) as calling_network_type,
        deyeNetworkTypeParse(receiver_to_url) as called_network_type
    from
        deye_ods_voip_fax,
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
        LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(sender_sip_number))) as T21(sender_sip_number_l),
        LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(receiver_sip_number))) as T22(receiver_sip_number_l)
    where
        data_type = 210
        and deyeVerifyIp(strsrc_ip, '') = '1'
        and deyeVerifyIp(strdst_ip, '') = '1'
        and char_length(T21.sender_sip_number_l)<= 30
        and char_length(T22.receiver_sip_number_l)<= 30
) tmp;