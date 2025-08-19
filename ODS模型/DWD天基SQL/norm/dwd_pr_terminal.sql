CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeVerifyIp WITH com.semptian.udf.verify.DeyeVerifyIp;
CREATE SCALAR FUNCTION deyeTimestampConverter WITH com.semptian.udf.timestamphandle.DeyeTimestampConverter;
CREATE SCALAR FUNCTION deyeUserNameNorm WITH com.semptian.udf.normalization.DeyeUserNameNorm;
CREATE SCALAR FUNCTION deyeMacNorm WITH com.semptian.udf.normalization.DeyeMacNorm;
CREATE SCALAR FUNCTION deyePasswordNorm WITH com.semptian.udf.normalization.DeyePasswordNorm;
CREATE SCALAR FUNCTION deyeIpNorm WITH com.semptian.udf.normalization.DeyeIpNorm;
CREATE SCALAR FUNCTION deyeVerifyTimestamp WITH com.semptian.udf.timestamphandle.DeyeVerifyTimestamp;
CREATE SCALAR FUNCTION deyeCompleteActionTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteActionTypeMap;
CREATE SCALAR FUNCTION deyeCompleteUpareaTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteUpareaTypeMap;
CREATE SCALAR FUNCTION deyeCompleteIspTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteIspTypeMap;
CREATE SCALAR FUNCTION deyeMobilePhoneNorm WITH com.semptian.udf.normalization.DeyeMobilePhoneNorm;
CREATE SCALAR FUNCTION deyeCompleteAuthTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteAuthTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeCompleteChildTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteChildTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataSourceMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataSourceMap;
CREATE SCALAR FUNCTION deyeCompleteProtocolTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteProtocolTypeMap;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.mark.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeGetSensitiveArea WITH com.semptian.udf.mark.DeyeGetSensitiveArea;
CREATE SCALAR FUNCTION deyeGetTerroristArea WITH com.semptian.udf.mark.DeyeGetTerroristArea;
CREATE SCALAR FUNCTION deyeGetTimeSpan WITH com.semptian.udf.mark.DeyeGetTimeSpan;
CREATE TABLE FUNCTION deyeIpAttributionComp WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionComp;
CREATE TABLE FUNCTION deyeIpAttributionCompV2 WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionCompV2;
CREATE TABLE FUNCTION deyeAuthAccountHandle WITH com.semptian.udtf.normalization.DeyeAuthAccountHandle;
CREATE TABLE FUNCTION deyeTimeHandle WITH com.semptian.udtf.normalization.DeyeTimeHandle;
CREATE SCALAR FUNCTION deyeGetIspByPhoneNumber WITH com.semptian.udf.mark.DeyeGetIspByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumber WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumber;
CREATE SCALAR FUNCTION deyeProduceImportantTargetFlag WITH com.semptian.udf.fieldproduction.DeyeProduceImportantTargetFlag;
CREATE SCALAR FUNCTION deyeOsNameNorm WITH com.semptian.udf.normalization.DeyeOsNameNorm;

CREATE TABLE ods_pr_source_0040_hardwarestring (
  data_type String,
  data_source String,
  protocol_type String,
  child_type String,
  `action` String,
  auth_type String,
  auth_account String,
  proxy_type String,
  proxy_address String,
  proxy_provider String,
  proxy_account String,
  strsrc_ip String,
  strdst_ip String,
  src_port Integer,
  dst_port Integer,
  strsrc_ip_v6 String,
  strdst_ip_v6 String,
  src_port_v6 Integer,
  dst_port_v6 Integer,
  src_ip_area String,
  dst_ip_area String,
  username String,
  password String,
  user_type String,
  user_id String,
  terminal_mac String,
  terminal_imsi String,
  terminal_imei String,
  app_pkg_name String,
  other_imei String,
  relate_imei String,
  android_id String,
  blue_mac String,
  ap_base_mac String,
  longitude Double,
  latitude Double,
  base_station_id String,
  ownership_land String,
  internet_land String,
  uparea_id String,
  lang_type String,
  abstract String,
  subject String,
  text String,
  attach_text String,
  keywords String,
  attach_names String,
  attach_sizes String,
  attach_md5s String,
  attach_num Integer,
  attach_download_path String,
  file_size Integer,
  file_path String,
  file_id String,
  tool_type String,
  tool_name String,
  isp_id String,
  device_id String,
  trace_id String,
  session_id String,
  icp_provider String,
  rule_version String,
  record_id String,
  soft_name String,
  favorite_tags String,
  host_url_tag String,
  brand String,
  model String,
  stream_end_time Bigint,
  up_teid String,
  down_teid String,
  application_layer_protocol String,
  tunnel_type String,
  imsi String,
  imei String,
  associate_mobile String,
  mobile_phone String,
  local_action String,
  mac String,
  hardware_type String,
  hardware_sign String,
  os_version String,
  os_name String,
  capture_time Bigint,
  insert_time Bigint,
  data_id String,
  extend_column String
) WITH (
  'type' = 'kafka',
  'topic' = 'ods_pr_source_0040_hardwarestring',
  'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
  'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
  'group.id' = 'dwd_pr_terminal',
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
CREATE TABLE dwd_pr_terminal (
  data_type String,
  data_type_map String,
  child_type String,
  child_type_map String,
  `action` String,
  action_map String,
  auth_account String,
  auth_type String,
  auth_type_map String,
  data_source String,
  data_source_map String,
  mobile_phone String,
  uparea_id String,
  uparea_id_map String,
  username String,
  password String,
  user_id String,
  imei String,
  imsi String,
  other_imei String,
  relate_imei String,
  base_station_id String,
  isp_id String,
  isp_id_map String,
  internet_land String,
  ownership_land String,
  application_layer_protocol String,
  down_teid String,
  tunnel_type String,
  up_teid String,
  protocol_type String,
  protocol_type_map String,
  strsrc_ip String,
  strsrc_ip_v6 String,
  strdst_ip String,
  strdst_ip_v6 String,
  dst_port Integer,
  dst_port_v6 Integer,
  src_port Integer,
  src_port_v6 Integer,
  src_ip_type Integer,
  src_ip_country String,
  src_ip_province String,
  src_ip_city String,
  src_ip_address String,
  src_ip_longitude Double,
  src_ip_latitude Double,
  dst_ip_type Integer,
  dst_ip_country String,
  dst_ip_province String,
  dst_ip_city String,
  dst_ip_address String,
  dst_ip_longitude Double,
  dst_ip_latitude Double,
  hardware_sign String,
  hardware_type String,
  local_action String,
  mac String,
  os_name String,
  os_version String,
  terminal_type String,
  terminal_type_map String,
  android_id String,
  terminal_imei String,
  terminal_imsi String,
  terminal_mac String,
  file_id String,
  file_path String,
  file_size Integer,
  attach_num Integer,
  attach_names String,
  attach_sizes String,
  attach_download_path String,
  attach_md5s String,
  attach_text String,
  subject String,
  keywords String,
  text String,
  lang_type String,
  tool_name String,
  tool_type String,
  data_id String,
  capture_time Bigint,
  capture_day String,
  capture_hour Integer,
  insert_time Bigint,
  mark String,
  device_id String,
  trace_id String,
  user_type String,
  ap_base_mac String,
  blue_mac String,
  app_pkg_name String,
  proxy_account String,
  proxy_address String,
  proxy_provider String,
  proxy_type String,
  latitude Double,
  longitude Double,
  abstract String,
  brand String,
  favorite_tags String,
  host_url_tag String,
  icp_provider String,
  session_id String,
  soft_name String,
  stream_end_time Bigint,
  model String,
  entity_list String,
  province_attribution String,
  time_span String,
  number_risk_level String,
  radius_risk String,
  roam_type String,
  terrorist_area String,
  sensitive_area String,
  `domain` String,
  format_virtual_account String,
  format_username String,
  norm_data_type String,
  important_target_flag Integer
) WITH (
  'type' = 'kafka',
  'topic' = 'v64_dwd_pr_terminal',
  'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
  'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
  'security.mode.flag' = 'false',
  'format.type' = 'avro',
  'batch.size'='1048576',
  'acks'='1',
  'linger.ms'='100',
  'buffer.memory'='33554432',
  'retries'='1',
  'feature.max.request.size' = '52428800'
);

CREATE TABLE dwd_pr_tmp (
  data_type String,
  data_type_map String,
  auth_account String,
  mobile_phone String,
  username String,
  password String,
  data_id String,
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
      'feature.max.request.size' = '52428800'
      );

INSERT INTO
    dwd_pr_tmp
SELECT
    data_type as data_type,
    deyeCompleteDataTypeMap(data_type, true)[2] as data_type_map,
    T3.auth_account_l as auth_account,
    deyeMobilePhoneNorm(associate_mobile, mobile_phone) as mobile_phone,
    deyeUserNameNorm(username) as username,
    deyePasswordNorm(password) as password,
    data_id as data_id,
    capture_time as capture_time,
    T9.capture_day_l as capture_day,
    T9.capture_hour_l as capture_hour,
    deyeTimestampConverter(data_id) as insert_time
FROM
    ods_pr_source_0040_hardwarestring,
  LATERAL TABLE(
    deyeAuthAccountHandle(
      auth_type,
      auth_account,
      data_type,
      deyeIpNorm(strsrc_ip),
      deyeIpNorm(strsrc_ip_v6),
      deyeIpNorm(strdst_ip),
      deyeIpNorm(strdst_ip_v6),
      capture_time
    )
  ) as T3(auth_type_l, auth_account_l),
  LATERAL TABLE(deyeTimeHandle(capture_time)) as T9(capture_day_l, capture_hour_l)
WHERE
    (
    (data_type = '142')
  AND (deyeVerifyTimestamp(capture_time, -2) = '1')
  AND (deyeVerifyTimestamp(capture_time, 1) = '0')
  AND (
    deyeVerifyIp(strsrc_ip, strsrc_ip_v6) = '1'
    )
  AND (
    deyeVerifyIp(strdst_ip, strdst_ip_v6) = '1'
    )
    )
  AND (
    CHAR_LENGTH(T3.auth_account_l) > 128
   OR CHAR_LENGTH(deyeMobilePhoneNorm(associate_mobile, mobile_phone)) > 30
   OR CHAR_LENGTH(deyeUserNameNorm(username)) > 512
   OR CHAR_LENGTH(deyePasswordNorm(password)) > 512
    );

INSERT INTO
  dwd_pr_terminal
SELECT
  data_type as data_type,
  deyeCompleteDataTypeMap(data_type, true)[2] as data_type_map,
  deyeCompleteChildTypeMap(child_type, data_type)[1] as child_type,
  deyeCompleteChildTypeMap(child_type, data_type)[2] as child_type_map,
  deyeCompleteActionTypeMap(`action`)[1] as `action`,
  deyeCompleteActionTypeMap(`action`)[2] as action_map,
  T3.auth_account_l as auth_account,
  T3.auth_type_l as auth_type,
  deyeCompleteAuthTypeMap(T3.auth_type_l) as auth_type_map,
  data_source as data_source,
  deyeCompleteDataSourceMap(data_source) as data_source_map,
  deyeMobilePhoneNorm(associate_mobile, mobile_phone) as mobile_phone,
  uparea_id as uparea_id,
  deyeCompleteUpareaTypeMap(uparea_id) as uparea_id_map,
  deyeUserNameNorm(username) as username,
  deyePasswordNorm(password) as password,
  user_id as user_id,
  imei as imei,
  imsi as imsi,
  other_imei as other_imei,
  relate_imei as relate_imei,
  base_station_id as base_station_id,
  CASE
    WHEN auth_type = '1020004' THEN deyeGetIspByPhoneNumber(T3.auth_account_l)
    ELSE ''
  END as isp_id,
  deyeCompleteIspTypeMap(CASE WHEN(auth_type = '1020004') THEN deyeGetIspByPhoneNumber(T3.auth_account_l) ELSE '' END) as isp_id_map,
  internet_land as internet_land,
  CASE
    WHEN auth_type = '1020004' THEN deyeGetCountryCodeByPhoneNumber(T3.auth_account_l)
    ELSE ''
  END as ownership_land,
  application_layer_protocol as application_layer_protocol,
  down_teid as down_teid,
  tunnel_type as tunnel_type,
  up_teid as up_teid,
  protocol_type as protocol_type,
  deyeCompleteProtocolTypeMap(protocol_type) as protocol_type_map,
  deyeIpNorm(strsrc_ip) as strsrc_ip,
  deyeIpNorm(strsrc_ip_v6) as strsrc_ip_v6,
  deyeIpNorm(strdst_ip) as strdst_ip,
  deyeIpNorm(strdst_ip_v6) as strdst_ip_v6,
  dst_port as dst_port,
  dst_port_v6 as dst_port_v6,
  src_port as src_port,
  src_port_v6 as src_port_v6,
  deyeProduceIpType(strsrc_ip, strsrc_ip_v6) as src_ip_type,
  T1.src_ip_country_l as src_ip_country,
  T1.src_ip_province_l as src_ip_province,
  T1.src_ip_city_l as src_ip_city,
  T1.src_ip_address_l as src_ip_address,
  T1.src_ip_longitude_l as src_ip_longitude,
  T1.src_ip_latitude_l as src_ip_latitude,
  deyeProduceIpType(strdst_ip, strdst_ip_v6) as dst_ip_type,
  T2.dst_ip_country_l as dst_ip_country,
  T2.dst_ip_province_l as dst_ip_province,
  T2.dst_ip_city_l as dst_ip_city,
  T2.dst_ip_address_l as dst_ip_address,
  T2.dst_ip_longitude_l as dst_ip_longitude,
  T2.dst_ip_latitude_l as dst_ip_latitude,
  hardware_sign as hardware_sign,
  hardware_type as hardware_type,
  local_action as local_action,
  deyeMacNorm(mac) as mac,
  deyeOsNameNorm(os_name) as os_name,
  os_version as os_version,
  '' as terminal_type,
  '' as terminal_type_map,
  android_id as android_id,
  terminal_imei as terminal_imei,
  terminal_imsi as terminal_imsi,
  terminal_mac as terminal_mac,
  file_id as file_id,
  file_path as file_path,
  file_size as file_size,
  attach_num as attach_num,
  attach_names as attach_names,
  attach_sizes as attach_sizes,
  attach_download_path as attach_download_path,
  attach_md5s as attach_md5s,
  attach_text as attach_text,
  subject as subject,
  keywords as keywords,
  text as text,
  SPLIT_INDEX(IF(LEFT(lang_type,1)='[' AND RIGHT(lang_type,1)=']', SUBSTR(lang_type,2,CHAR_LENGTH(lang_type)-2), lang_type),' ',0) as lang_type,
  deyeCompleteChildTypeMap(child_type, data_type)[2] as tool_name,
  deyeCompleteChildTypeMap(child_type, data_type)[1] as tool_type,
  data_id as data_id,
  capture_time as capture_time,
  T9.capture_day_l as capture_day,
  T9.capture_hour_l as capture_hour,
  deyeTimestampConverter(data_id) as insert_time,
  '' as mark,
  device_id as device_id,
  trace_id as trace_id,
  user_type as user_type,
  ap_base_mac as ap_base_mac,
  blue_mac as blue_mac,
  app_pkg_name as app_pkg_name,
  proxy_account as proxy_account,
  proxy_address as proxy_address,
  proxy_provider as proxy_provider,
  proxy_type as proxy_type,
  latitude as latitude,
  longitude as longitude,
  abstract as abstract,
  brand as brand,
  favorite_tags as favorite_tags,
  host_url_tag as host_url_tag,
  icp_provider as icp_provider,
  session_id as session_id,
  soft_name as soft_name,
  stream_end_time as stream_end_time,
  model as model,
  '' as entity_list,
  '' as province_attribution,
  deyeGetTimeSpan(capture_time) as time_span,
  '' as number_risk_level,
  '' as radius_risk,
  '' as roam_type,
  deyeGetTerroristArea(
    deyeGetCountryCodeByPhoneNumber(T3.auth_account_l),
    'DZ',
    T1.src_ip_country_code_l
  ) as terrorist_area,
  deyeGetSensitiveArea(
    deyeGetCountryCodeByPhoneNumber(T3.auth_account_l),
    'DZ',
    T1.src_ip_country_code_l
  ) as sensitive_area,
  '' AS `domain`,
  '' AS format_virtual_account,
  LOWER(username) AS format_username,
  deyeCompleteDataTypeMap(data_type, true)[1] as norm_data_type,
  deyeProduceImportantTargetFlag(data_type,T3.auth_account_l,T3.auth_type_l,deyeIpNorm(strsrc_ip),deyeIpNorm(strsrc_ip_v6),deyeIpNorm(strdst_ip),deyeIpNorm(strdst_ip_v6)) as important_target_flag
FROM
  ods_pr_source_0040_hardwarestring,
  LATERAL TABLE(deyeIpAttributionCompV2(IF(strsrc_ip='', strsrc_ip_v6,strsrc_ip))) as T1(
    src_ip_address_l,
    src_ip_country_l,
    src_ip_province_l,
    src_ip_city_l,
    src_ip_longitude_l,
    src_ip_latitude_l,
    src_ip_country_code_l
  ),
  LATERAL TABLE(deyeIpAttributionCompV2(IF(strdst_ip='', strdst_ip_v6,strdst_ip))) as T2(
    dst_ip_address_l,
    dst_ip_country_l,
    dst_ip_province_l,
    dst_ip_city_l,
    dst_ip_longitude_l,
    dst_ip_latitude_l,
    dst_ip_country_code_l
  ),
  LATERAL TABLE(
    deyeAuthAccountHandle(
      auth_type,
      auth_account,
      data_type,
      deyeIpNorm(strsrc_ip),
      deyeIpNorm(strsrc_ip_v6),
      deyeIpNorm(strdst_ip),
      deyeIpNorm(strdst_ip_v6),
      capture_time
    )
  ) as T3(auth_type_l, auth_account_l),
  LATERAL TABLE(deyeTimeHandle(capture_time)) as T9(capture_day_l, capture_hour_l)
WHERE
  (
    (data_type = '142')
    AND (deyeVerifyTimestamp(capture_time, -2) = '1')
    AND (deyeVerifyTimestamp(capture_time, 1) = '0')
    AND (
      deyeVerifyIp(strsrc_ip, strsrc_ip_v6) = '1'
    )
    AND (
      deyeVerifyIp(strdst_ip, strdst_ip_v6) = '1'
    )
  )
  AND (
    CHAR_LENGTH(T3.auth_account_l) <= 128
  AND CHAR_LENGTH(deyeMobilePhoneNorm(associate_mobile, mobile_phone)) <= 30
  AND CHAR_LENGTH(deyeUserNameNorm(username)) <= 512
  AND CHAR_LENGTH(deyePasswordNorm(password)) <= 512
    );