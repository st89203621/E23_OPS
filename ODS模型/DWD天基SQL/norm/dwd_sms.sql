CREATE SCALAR FUNCTION deyeVerifyTimestamp WITH com.semptian.udf.timestamphandle.DeyeVerifyTimestamp;
CREATE SCALAR FUNCTION deyeCompleteActionTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteActionTypeMap;
CREATE SCALAR FUNCTION deyeCompleteIspTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteIspTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeCompleteChildTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteChildTypeMap;
CREATE SCALAR FUNCTION deyeGetIspByPhoneNumber WITH com.semptian.udf.mark.DeyeGetIspByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetSensitiveArea WITH com.semptian.udf.mark.DeyeGetSensitiveArea;
CREATE SCALAR FUNCTION deyeGetTerroristArea WITH com.semptian.udf.mark.DeyeGetTerroristArea;
CREATE SCALAR FUNCTION deyeGetTimeSpan WITH com.semptian.udf.mark.DeyeGetTimeSpan;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumberV2 WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumberV2;
CREATE SCALAR FUNCTION deyeProduceLangType WITH com.semptian.udf.fieldproduction.DeyeProduceLangType;
CREATE SCALAR FUNCTION deyeProduceSpamFlag WITH com.semptian.udf.fieldproduction.DeyeProduceSpamFlag;
CREATE SCALAR FUNCTION deyePhoneNumberNorm WITH com.semptian.udf.normalization.DeyePhoneNumberNorm;
CREATE TABLE FUNCTION udf2Udtf WITH com.semptian.udtf.common.Udf2Udtf;
CREATE TABLE FUNCTION udf2UdtfV2 WITH com.semptian.udtf.common.Udf2UdtfV2;
CREATE SCALAR FUNCTION deyeSmsTextParse WITH com.semptian.udf.fieldspecialhandle.DeyeSmsTextParse;
CREATE SCALAR FUNCTION deyeSmsTextParseV2 WITH com.semptian.udf.fieldspecialhandle.DeyeSmsTextParseV2;
CREATE SCALAR FUNCTION deyeNetworkTypeParse WITH com.semptian.udf.fieldspecialhandle.DeyeNetworkTypeParse;
CREATE SCALAR FUNCTION deyeGetMurmurHash WITH com.semptian.udf.DeyeGetMurmurHash;
CREATE SCALAR FUNCTION deyePhoneNumberNormV2 WITH com.semptian.udf.normalization.DeyePhoneNumberNormV2;
CREATE SCALAR FUNCTION deyeGetCountryByPointCode WITH com.semptian.udf.call.DeyeGetCountryByPointCode;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.mark.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeBeforeAiDealSmsFilter WITH com.semptian.udf.filter.BeforeAiDealSmsFilter;
CREATE SCALAR FUNCTION deyeWhitelistFilter WITH com.semptian.udf.filter.DeyeWhitelistFilter;
CREATE SCALAR FUNCTION deyeGetElementInfoUdf WITH com.semptian.udtf.normalization.DeyeGetElementInfoUdf;
CREATE SCALAR FUNCTION smsPhoneNumberComplete WITH com.semptian.udf.call.SmsPhoneNumberComplete;
CREATE SCALAR FUNCTION dyeSmsFilterDeal WITH com.semptian.udf.call.DyeSmsFilterDeal;
CREATE TABLE FUNCTION deyeGetElementInfo WITH com.semptian.udtf.normalization.DeyeGetElementInfo;
CREATE TABLE FUNCTION deyePhoneNumberNormHandle WITH com.semptian.udtf.normalization.DeyePhoneNumberNormHandle;

CREATE TABLE deye_ods_call (
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
    sms_lang String,
    sms_text String,
    third_numbergroup String,
    capture_time Bigint,
    insert_time Bigint,
    data_id String,
    data_type Integer
) WITH (
    'type' = 'kafka',
    'topic' = 'deye_ods_call',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'group.id' = 'dwd_sms',
    'security.mode.flag' = 'false',
    'session.timeout.ms' = '30000',
    'enable.auto.commit' = 'false',
    'auto.offset.reset' = 'latest',
    'feature.max.poll.records' = '1000',
    'feature.fetch.max.bytes' = '11534336',
    'feature.max.partition.fetch.bytes' = '11534336',
    'feature.fetch.message.max.bytes' = '11534336',
    'feature.request.timeout.ms' = '120000',
    'format.type' = 'avro'
);

CREATE TABLE dwd_sms (
    data_id String,
    data_type String,
    child_type String,
    auth_type String,
    imsi String,
    calling_imei String,
    calling_imsi String,
    called_imei String,
    called_imsi String,
    calling_number String,
    called_number String,
    third_numbergroup String,
    start_time Long,
    end_time Long,
    last_time Integer,
    send_time Long,
    calling_atrribution String,
    called_atrribution String,
    longitude Double,
    latitude Double,
    location_id String,
    phone_attribution String,
    called_phone_attribution String,
    `action` String,
    file_id String,
    file_path String,
    sms_lang String,
    sms_text String,
    call_id Long,
    call_flag Integer,
    record_flag Integer,
    online_flag Integer,
    call_code Integer,
    base_station_id String,
    bs_attribution String,
    ci String,
    station_type String,
    called_bs_attribution String,
    isp_id String,
    clue_id String,
    point_code String,
    imei String,
    mcc String,
    mnc String,
    lac String,
    cell_id String,
    capture_time Long,
    capture_day String,
    capture_hour Integer,
    link_layer_id Long,
    in_unit String,
    out_unit String,
    insert_time Long,
    data_type_map String,
    child_type_map String,
    action_map String,
    isp_id_map String,
    mark String,
    file_size Integer,
    calling_province_attribution String,
    called_province_attribution String,
    calling_isp String,
    called_isp String,
    time_span String,
    calling_number_risk_level String,
    called_number_risk_level String,
    calling_terrorist_area String,
    called_terrorist_area String,
    calling_sensitive_area String,
    called_sensitive_area String,
    sms_type String,
    proper_noun String,
    norm_data_type String,
    spam_flag Integer,
    calling_network_type String,
    called_network_type String,
    b_point_code String,
    roam_type String,
    calling_roam String,
    called_roam String,
    a_point_country String,
    b_point_country String
) WITH (
    'type' = 'kafka',
    'topic' = 'v64_dwd_sms',
    'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
    'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
    'security.mode.flag' = 'false',
    'format.type' = 'avro',
    'batch.size'='1048576',
    'acks'='1',
    'linger.ms'='100',
    'parallelism'='16',
    'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800'
);

CREATE TABLE dwd_pr_tmp (
    data_id String,
    data_type String,
    calling_number String,
    called_number String,
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
    'parallelism'='2' ,
    'linger.ms'='100',
    'buffer.memory'='33554432',
    'retries'='1',
    'feature.max.request.size' = '52428800'
  );

INSERT INTO
    dwd_pr_tmp
SELECT
    data_id as data_id,
    CAST(data_type AS VARCHAR) as data_type,
    T21.a_number_l as calling_number,
    T21.b_number_l as called_number,
    capture_time as capture_time,
    from_unixtime(capture_time/1000, 'yyyy-MM-dd') as capture_day,
    CAST(from_unixtime(capture_time/1000, 'H') AS INT) as capture_hour,
    unix_timestamp()*1000 as insert_time
FROM
    deye_ods_call,
    LATERAL TABLE(DeyePhoneNumberNormHandle(a_number, b_number, b_format_number)) as T21(a_number_l, b_number_l)
WHERE
    (
    (data_type = 200)
  AND (deyeVerifyTimestamp(capture_time, -2) = '1')
  AND (deyeVerifyTimestamp(capture_time, 1) = '0')
    )
  AND
    (
  CHAR_LENGTH(T21.a_number_l) > 30
  OR CHAR_LENGTH(T21.b_number_l) > 30
    );


insert into dwd_sms
select
    tmp.data_id as data_id,
    tmp.data_type as data_type,
    '2009999' as child_type,
    '1020004' as auth_type,
    tmp.imsi as imsi,
    tmp.calling_imei as calling_imei,
    tmp.calling_imsi as calling_imsi,
    '' as called_imei,
    tmp.called_imsi as called_imsi,
    tmp.calling_number as calling_number,
    tmp.called_number as called_number,
    tmp.third_numbergroup as third_numbergroup,
    tmp.start_time as start_time,
    tmp.end_time as end_time,
    tmp.last_time as last_time,
    tmp.send_time as send_time,
    tmp.a_belong_country_code as calling_atrribution,
    tmp.b_belong_country_code as called_atrribution,
    CAST(0 AS DOUBLE) as longitude,
    CAST(0 AS DOUBLE) as latitude,
    '' as location_id,
    '' as phone_attribution,
    '' as called_phone_attribution,
    tmp.action as `action`,
    '' as file_id,
    '' as file_path,
    tmp.sms_lang as sms_lang,
    CASE WHEN tmp.sms_text = '$#' THEN '' ELSE tmp.sms_text END as sms_text,
    tmp.call_id as call_id,
    tmp.call_flag as call_flag,
    0 as record_flag,
    0 as online_flag,
    tmp.call_code as call_code,
    '' as base_station_id,
    '' as bs_attribution,
    '' as ci,
    '' as station_type,
    '' as called_bs_attribution,
    tmp.isp_id as isp_id,
    '' as clue_id,
    tmp.point_code as point_code,
    tmp.imei as imei,
    tmp.mcc as mcc,
    tmp.mnc as mnc,
    '' as lac,
    '' as cell_id,
    tmp.capture_time as capture_time,
    from_unixtime(tmp.capture_time/1000, 'yyyy-MM-dd') as capture_day,
    CAST(from_unixtime(tmp.capture_time/1000, 'H') AS INT) as capture_hour,
    CAST(0 AS BIGINT) as link_layer_id,
    '' as in_unit,
    '' as out_unit,
    unix_timestamp()*1000 as insert_time,
    tmp.data_type_map as data_type_map,
    tmp.child_type_map as child_type_map,
    tmp.action_map as action_map,
    tmp.isp_id_map as isp_id_map,
    '' as mark,
    tmp.file_size as file_size,
    '' as calling_province_attribution,
    '' as called_province_attribution,
    tmp.calling_isp as calling_isp,
    tmp.called_isp as called_isp,
    tmp.time_span as time_span,
    '' as calling_number_risk_level,
    '' as called_number_risk_level,
    deyeGetTerroristArea(tmp.a_belong_country_code,'','') as calling_terrorist_area,
    deyeGetTerroristArea(tmp.b_belong_country_code,'','') as called_terrorist_area,
    deyeGetSensitiveArea(tmp.a_belong_country_code,'','') as calling_sensitive_area,
    deyeGetSensitiveArea(tmp.b_belong_country_code,'','') as called_sensitive_area,
    case
        when tmp.sms_type='130005' then deyeGetElementInfoUdf('http://192.168.80.142:8022/api/sms_classify', tmp.sms_text, 'data')
        else tmp.sms_type
    end as sms_type,
    '' as proper_noun,
    tmp.norm_data_type as norm_data_type,
    tmp.spam_flag as spam_flag,
    tmp.calling_network_type as calling_network_type,
    tmp.called_network_type as called_network_type,
    tmp.b_point_code as b_point_code,
    --coalesce(
    --    nullif(deyeGetRoamType(tmp.a_belong_country_code, tmp.a_point_country), ''),
    --    nullif(deyeGetRoamType(tmp.b_belong_country_code, tmp.b_point_country), ''),
    --    ''
    --) as roam_type,
    --deyeGetRoamType(tmp.a_belong_country_code, tmp.a_point_country) as calling_roam,
    --deyeGetRoamType(tmp.b_belong_country_code, tmp.b_point_country) as called_roam,
    coalesce(
        nullif(deyeGetRoamType(tmp.a_belong_country_code, ''), ''),
        nullif(deyeGetRoamType(tmp.b_belong_country_code, ''), ''),
        ''
    ) as roam_type,
    deyeGetRoamType(tmp.a_belong_country_code, '') as calling_roam,
    deyeGetRoamType(tmp.b_belong_country_code, '') as called_roam,
    '' as a_point_country,
    '' as b_point_country
    --tmp.a_point_country as a_point_country,
    --tmp.b_point_country as b_point_country
from(
    select
        compre_tab.data_id as data_id,
        CAST(compre_tab.data_type AS VARCHAR) as data_type,
        compre_tab.a_imsi as imsi,
        compre_tab.a_imei as calling_imei,
        compre_tab.a_imsi as calling_imsi,
        compre_tab.b_imsi as called_imsi,
        T21.a_number_l as calling_number,
        T21.b_number_l as called_number,
        deyePhoneNumberNorm(compre_tab.third_numbergroup,'.') as third_numbergroup,
        compre_tab.start_time,
        compre_tab.end_time,
        compre_tab.duration as last_time,
        CAST(T7.send_time AS BIGINT) as send_time,
        deyeCompleteActionTypeMap(compre_tab.`action`,CAST(compre_tab.data_type AS VARCHAR))[1] as `action`,
        deyeProduceLangType(T7.sms_text) as sms_lang,
        T7.sms_text as sms_text,
        deyeGetMurmurHash(compre_tab.call_id) as call_id,
        compre_tab.call_type as call_flag,
        COALESCE(CAST(compre_tab.sip_code AS INT),0) as call_code,
        CAST(compre_tab.isp_id AS VARCHAR) as isp_id,
        compre_tab.a_point_code as point_code,
        compre_tab.a_imei as imei,
        compre_tab.a_mcc as mcc,
        compre_tab.a_mnc as mnc,
        compre_tab.capture_time,
        deyeCompleteDataTypeMap(CAST(compre_tab.data_type AS VARCHAR),true)[2] as data_type_map,
        deyeCompleteChildTypeMap('2009999')[2] as child_type_map,
        deyeCompleteActionTypeMap(compre_tab.`action`,CAST(compre_tab.data_type AS VARCHAR))[2] as action_map,
        CASE
            WHEN compre_tab.isp_id = 5 THEN 'Ooredoo'
            WHEN compre_tab.isp_id = 6 THEN 'Mobilis'
            WHEN compre_tab.isp_id = 7 THEN 'Djezzy'
            ELSE 'Autre'
        END as isp_id_map,
        CHAR_LENGTH(T7.sms_text) as file_size,
        deyeGetIspByPhoneNumber(T21.a_number_l) as calling_isp,
        deyeGetIspByPhoneNumber(T21.b_number_l) as called_isp,
        deyeGetTimeSpan(compre_tab.capture_time) as time_span,
        deyeCompleteDataTypeMap(CAST(compre_tab.data_type AS VARCHAR),true)[1] as norm_data_type,
        deyeProduceSpamFlag(T21.a_number_l, T21.b_number_l) as spam_flag,
        deyeNetworkTypeParse(compre_tab.a_sip_uri) as calling_network_type,
        deyeNetworkTypeParse(compre_tab.b_sip_uri) as called_network_type,
        compre_tab.b_point_code as b_point_code,
        --T5.a_point_country as a_point_country,
        --T6.b_point_country as b_point_country,
        --deyeGetCountryCodeByPhoneNumberV2(compre_tab.a_number,'',T5.a_point_country) as a_belong_country_code,
        --deyeGetCountryCodeByPhoneNumberV2(compre_tab.b_number,compre_tab.b_format_number,T6.b_point_country) as b_belong_country_code,
        deyeGetCountryCodeByPhoneNumberV2(T21.a_number_l) as a_belong_country_code,
        deyeGetCountryCodeByPhoneNumberV2(T21.b_number_l) as b_belong_country_code,
        dyeSmsFilterDeal(T21.a_number_l,T21.b_number_l,T7.sms_text,compre_tab.`action`) as sms_type
    from (
        select
            filter_tab.*
        from (
            select
                *,
                row_number() over(
                    partition by md5(concat_ws('|',a_number,b_number,a_imsi,b_imsi,action,a_point_code,b_point_code,sms_text,cast(capture_time as varchar)))
                    order by proctime()
                ) as row_num
            from deye_ods_call
            where
                data_type = 200
                and action in ('771','772')
                and deyeVerifyTimestamp(capture_time, -2) = '1'
                and deyeVerifyTimestamp(capture_time, 1) = '0'
        ) filter_tab
        where filter_tab.row_num = 1
    ) compre_tab,
        --LATERAL TABLE(DeyePhoneNumberNormHandle(compre_tab.a_number, compre_tab.b_number, compre_tab.b_format_number)) as T21(a_number_l, b_number_l),
        LATERAL TABLE(DeyePhoneNumberNormHandle(smsPhoneNumberComplete(compre_tab.a_imsi,compre_tab.a_number), smsPhoneNumberComplete(compre_tab.b_imsi,compre_tab.b_number), compre_tab.b_format_number)) as T21(a_number_l, b_number_l),
        --LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(smsPhoneNumberComplete(compre_tab.a_imsi,compre_tab.a_number)))) as T21(a_number_l),
        --LATERAL TABLE(udf2Udtf(deyePhoneNumberNorm(smsPhoneNumberComplete(compre_tab.b_imsi,compre_tab.b_number)))) as T22(b_number_l),
        --lateral table(udf2Udtf(deyeGetCountryByPointCode(compre_tab.a_point_code))) as T5(a_point_country),
        --lateral table(udf2Udtf(deyeGetCountryByPointCode(compre_tab.b_point_code))) as T6(b_point_country),
        lateral table(udf2UdtfV2(deyeSmsTextParseV2(compre_tab.sms_lang,compre_tab.sms_text))) as T7(send_time,sms_text)
    where deyeWhitelistFilter(T21.a_number_l)
        and deyeWhitelistFilter(T21.b_number_l)
        and char_length(T21.a_number_l) <= 30
        and char_length(T21.b_number_l) <= 30
) tmp