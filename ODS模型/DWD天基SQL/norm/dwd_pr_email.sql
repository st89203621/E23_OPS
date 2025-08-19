CREATE SCALAR FUNCTION deyeProduceIpType WITH com.semptian.udf.fieldproduction.DeyeProduceIpType;
CREATE SCALAR FUNCTION deyeVerifyIp WITH com.semptian.udf.verify.DeyeVerifyIp;
CREATE SCALAR FUNCTION deyeTimestampConverter WITH com.semptian.udf.timestamphandle.DeyeTimestampConverter;
CREATE SCALAR FUNCTION deyeUserNameNorm WITH com.semptian.udf.normalization.DeyeUserNameNorm;
CREATE SCALAR FUNCTION deyeMacNorm WITH com.semptian.udf.normalization.DeyeMacNorm;
CREATE SCALAR FUNCTION deyeProduceRelativeUrl WITH com.semptian.udf.fieldproduction.DeyeProduceRelativeUrl;
CREATE SCALAR FUNCTION deyeProduceSimHash WITH com.semptian.udf.fieldproduction.DeyeProduceSimHash;
CREATE SCALAR FUNCTION deyeSubDomainNorm WITH com.semptian.udf.normalization.DeyeSubDomainNorm;
CREATE SCALAR FUNCTION deyePasswordNorm WITH com.semptian.udf.normalization.DeyePasswordNorm;
CREATE SCALAR FUNCTION deyeIpNorm WITH com.semptian.udf.normalization.DeyeIpNorm;
CREATE SCALAR FUNCTION deyeSuperDomainNorm WITH com.semptian.udf.normalization.DeyeSuperDomainNorm;
CREATE SCALAR FUNCTION deyeVerifyTimestamp WITH com.semptian.udf.timestamphandle.DeyeVerifyTimestamp;
CREATE SCALAR FUNCTION deyeProduceMailCategory WITH com.semptian.udf.fieldproduction.DeyeProduceMailCategory;
CREATE SCALAR FUNCTION deyeCompleteActionTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteActionTypeMap;
CREATE SCALAR FUNCTION deyeCompleteUpareaTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteUpareaTypeMap;
CREATE SCALAR FUNCTION deyeCompleteIspTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteIspTypeMap;
CREATE SCALAR FUNCTION deyeMobilePhoneNorm WITH com.semptian.udf.normalization.DeyeMobilePhoneNorm;
CREATE SCALAR FUNCTION deyeCompleteAuthTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteAuthTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataTypeMap;
CREATE SCALAR FUNCTION deyeCompleteChildTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteChildTypeMap;
CREATE SCALAR FUNCTION deyeCompleteDataSourceMap WITH com.semptian.udf.dimcomplete.DeyeCompleteDataSourceMap;
CREATE SCALAR FUNCTION deyeCompleteProtocolTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteProtocolTypeMap;
CREATE SCALAR FUNCTION deyeCompleteTerminalTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteTerminalTypeMap;
CREATE SCALAR FUNCTION deyeCompleteBrowseTypeMap WITH com.semptian.udf.dimcomplete.DeyeCompleteBrowseTypeMap;
CREATE SCALAR FUNCTION deyeGetRoamType WITH com.semptian.udf.mark.DeyeGetRoamType;
CREATE SCALAR FUNCTION deyeGetSensitiveArea WITH com.semptian.udf.mark.DeyeGetSensitiveArea;
CREATE SCALAR FUNCTION deyeGetTerroristArea WITH com.semptian.udf.mark.DeyeGetTerroristArea;
CREATE SCALAR FUNCTION deyeGetTimeSpan WITH com.semptian.udf.mark.DeyeGetTimeSpan;
CREATE TABLE FUNCTION deyeIpAttributionComp WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionComp;
CREATE TABLE FUNCTION deyeIpAttributionCompV2 WITH com.semptian.udtf.dimcomplete.DeyeIpAttributionCompV2;
CREATE TABLE FUNCTION deyeAuthAccountHandle WITH com.semptian.udtf.normalization.DeyeAuthAccountHandle;
CREATE TABLE FUNCTION deyeMailAccountHandle WITH com.semptian.udtf.normalization.DeyeMailAccountHandle;
CREATE TABLE FUNCTION deyeDomainHandle WITH com.semptian.udtf.normalization.DeyeDomainHandle;
CREATE TABLE FUNCTION deyeTimeHandle WITH com.semptian.udtf.normalization.DeyeTimeHandle;
CREATE TABLE FUNCTION deyeMainAccountHandle WITH com.semptian.udtf.normalization.DeyeMainAccountHandle;
CREATE TABLE FUNCTION deyeUserAgentHandle WITH com.semptian.udtf.normalization.DeyeUserAgentHandle;
CREATE SCALAR FUNCTION deyeGetIspByPhoneNumber WITH com.semptian.udf.mark.DeyeGetIspByPhoneNumber;
CREATE SCALAR FUNCTION deyeGetCountryCodeByPhoneNumber WITH com.semptian.udf.DeyeGetCountryCodeByPhoneNumber;
CREATE SCALAR FUNCTION deyeUrlDecode WITH com.semptian.udf.fieldspecialhandle.DeyeUrlDecode;
CREATE SCALAR FUNCTION deyeVirtualAccountNorm WITH com.semptian.udf.normalization.DeyeVirtualAccountNorm;
CREATE SCALAR FUNCTION deyeProduceImportantTargetFlag WITH com.semptian.udf.fieldproduction.DeyeProduceImportantTargetFlag;
CREATE SCALAR FUNCTION deyeOsNameNorm WITH com.semptian.udf.normalization.DeyeOsNameNorm;
CREATE SCALAR FUNCTION deyeBrowseTypeNorm WITH com.semptian.udf.normalization.DeyeBrowseTypeNorm;
CREATE SCALAR FUNCTION deyeAccountFilter WITH com.semptian.udf.filter.DeyeAccountFilter;

CREATE TABLE ods_pr_source_0002_email (
  data_type String,
  data_source String,
  protocol_type String,
  child_type String,
  `action` String,
  auth_type String,
  auth_account String,
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
  host String,
  proxy_type String,
  proxy_address String,
  proxy_provider String,
  proxy_account String,
  longitude Double,
  latitude Double,
  base_station_id String,
  ownership_land String,
  internet_land String,
  uparea_id String,
  mail_from String,
  mail_to String,
  mail_cc String,
  mail_bcc String,
  mail_date Bigint,
  text String,
  spam_flag Integer,
  forgery_flag Integer,
  encrypt_flag Integer,
  spam_score Double,
  lang_type String,
  abstract String,
  attach_names String,
  attach_sizes String,
  attach_md5s String,
  attach_text String,
  attach_num Integer,
  keywords String,
  attach_download_path String,
  file_path String,
  file_size Integer,
  file_id String,
  file_md5 String,
  subject String,
  charset String,
  tool_type String,
  tool_name String,
  device_id String,
  trace_id String,
  session_id String,
  stream_end_time Bigint,
  content_type String,
  up_teid String,
  down_teid String,
  application_layer_protocol String,
  tunnel_type String,
  isp_id String,
  imsi String,
  imei String,
  associate_mobile String,
  mobile_phone String,
  username String,
  password String,
  `domain` String,
  url String,
  local_action String,
  mac String,
  hardware_type String,
  hardware_sign String,
  os_name String,
  os_version String,
  browse_type String,
  browse_version String,
  user_agent String,
  icp_provider String,
  rule_version String,
  record_id String,
  soft_name String,
  favorite_tags String,
  host_url_tag String,
  software String,
  mail_tool_type String,
  app_version String,
  remark String,
  from_nickname String,
  to_nickname String,
  forgery_user String,
  message_id String,
  important_level String,
  capture_time Bigint,
  insert_time Bigint,
  data_id String,
  extend_column String
) WITH (
  'type' = 'kafka',
  'topic' = 'ods_pr_source_0002_email',
  'bootstrap.servers' = 'hdp-01:6667,hdp-02:6667,hdp-03:6667,hdp-04:6667,hdp-07:6667,hdp-05:6667,hdp-06:6667,hdp-08:6667',
  'zookeeper.connect' = 'hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181,hdp-07:2181,hdp-05:2181,hdp-06:2181',
  'group.id' = 'dwd_pr_email',
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
CREATE TABLE dwd_pr_email (
  data_type String,
  data_type_map String,
  child_type String,
  child_type_map String,
  `action` String,
  action_map String,
  auth_account String,
  auth_type String,
  auth_type_map String,
  main_account String,
  data_source String,
  data_source_map String,
  mobile_phone String,
  uparea_id String,
  uparea_id_map String,
  username String,
  password String,
  imei String,
  imsi String,
  cell_id String,
  lac String,
  mcc String,
  mnc String,
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
  host String,
  hardware_sign String,
  hardware_type String,
  local_action String,
  mac String,
  os_name String,
  os_version String,
  terminal_type String,
  terminal_type_map String,
  browse_type String,
  browse_type_map String,
  browse_version String,
  file_id String,
  file_path String,
  file_size Integer,
  file_md5 String,
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
  url String,
  `domain` String,
  relative_url String,
  sub_domain String,
  super_domain String,
  app_name String,
  app_type String,
  tool_name String,
  tool_type String,
  from_nickname String,
  to_nickname String,
  from_account String,
  from_account_remark String,
  to_account String,
  to_account_remark String,
  data_id String,
  capture_time Bigint,
  capture_day String,
  capture_hour Integer,
  insert_time Bigint,
  mark String,
  device_id String,
  trace_id String,
  app_version String,
  charset String,
  mail_date Bigint,
  mail_category String,
  mail_from String,
  mail_from_remark String,
  mail_to String,
  mail_to_remark String,
  mail_cc String,
  mail_cc_remark String,
  mail_bcc String,
  mail_bcc_remark String,
  mail_tool_type String,
  encrypt_flag Integer,
  forgery_flag Integer,
  spam_flag Integer,
  spam_score Double,
  spam_mark_time Bigint,
  user_agent String,
  proxy_account String,
  proxy_address String,
  proxy_provider String,
  proxy_type String,
  abstract String,
  favorite_tags String,
  host_url_tag String,
  icp_provider String,
  message_id String,
  remark String,
  session_id String,
  soft_name String,
  software String,
  content_type String,
  stream_end_time Bigint,
  forgery_user String,
  sim_hash String,
  important_level String,
  entity_list String,
  province_attribution String,
  time_span String,
  number_risk_level String,
  radius_risk String,
  mail_risk String,
  roam_type String,
  terrorist_area String,
  sensitive_area String,
  mail_classify String,
  mail_anomaly String,
  mail_importance String,
  proper_noun String,
  sub_protocol String,
  sub_protocol_map String,
  format_virtual_account String,
  format_username String,
  norm_data_type String,
  important_target_flag Integer
) WITH (
  'type' = 'kafka',
  'topic' = 'v64_dwd_pr_email',
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
  mail_from String,
  mail_to String,
  mail_cc String,
  mail_bcc String,
  from_nickname String,
  from_account String,
  to_account String,
  to_nickname String,
  charset String,
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
    T4.mail_from_l as mail_from,
    T5.mail_to_l as mail_to,
    T6.mail_cc_l as mail_cc,
    T7.mail_bcc_l as mail_bcc,
    from_nickname as from_nickname,
    T10.from_account_l as from_account,
    to_nickname as to_nickname,
    T10.to_account_l as to_account,
    charset as charset,
    data_id as data_id,
    capture_time as capture_time,
    T9.capture_day_l as capture_day,
    T9.capture_hour_l as capture_hour,
    deyeTimestampConverter(data_id) as insert_time
FROM
    ods_pr_source_0002_email,
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
  LATERAL TABLE(deyeMailAccountHandle(mail_from)) as T4(mail_from_l, mail_from_remark_l),
  LATERAL TABLE(deyeMailAccountHandle(mail_to)) as T5(mail_to_l, mail_to_remark_l),
  LATERAL TABLE(deyeMailAccountHandle(mail_cc)) as T6(mail_cc_l, mail_cc_remark_l),
  LATERAL TABLE(deyeMailAccountHandle(mail_bcc)) as T7(mail_bcc_l, mail_bcc_remark_l),
  LATERAL TABLE(deyeTimeHandle(capture_time)) as T9(capture_day_l, capture_hour_l),
  LATERAL TABLE(
    deyeMainAccountHandle(
      data_type,
      action,
      username,
      ARRAY [ T4.mail_from_l, T4.mail_from_remark_l ],
      ARRAY [ T5.mail_to_l, T5.mail_to_remark_l, T6.mail_cc_l, T6.mail_cc_remark_l, T7.mail_bcc_l, T7.mail_bcc_remark_l]
    )
  ) as T10(
    main_account_l,
    from_account_l,
    from_account_remark_l,
    to_account_l,
    to_account_remark_l
  )
WHERE
    (
    (data_type = '101')
  AND (deyeVerifyTimestamp(capture_time, -2) = '1')
  AND (deyeVerifyTimestamp(capture_time, 1) = '0')
  AND (
    deyeVerifyIp(
    strsrc_ip,
    strsrc_ip_v6
    ) = '1'
    )
  AND (
    deyeVerifyIp(
    strdst_ip,
    strdst_ip_v6
    ) = '1'
    )
    )
  AND (
    CHAR_LENGTH(T3.auth_account_l) > 128
   OR CHAR_LENGTH(deyeMobilePhoneNorm(associate_mobile, mobile_phone)) > 30
   OR CHAR_LENGTH(deyeUserNameNorm(username)) > 512
   OR CHAR_LENGTH(deyePasswordNorm(password)) > 512
   OR CHAR_LENGTH(T4.mail_from_l) > 256
   OR NOT deyeAccountFilter(T5.mail_to_l, 256)
   OR NOT deyeAccountFilter(T6.mail_cc_l, 256)
   OR NOT deyeAccountFilter(T7.mail_bcc_l, 256)
   OR CHAR_LENGTH(from_nickname) > 256
   OR CHAR_LENGTH(T10.from_account_l) > 256
   OR NOT deyeAccountFilter(to_nickname, 256)
   OR NOT deyeAccountFilter(T10.to_account_l, 256)
   OR CHAR_LENGTH(charset) > 128
    );

INSERT INTO
  dwd_pr_email
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
  T10.main_account_l as main_account,
  data_source as data_source,
  deyeCompleteDataSourceMap(data_source) as data_source_map,
  deyeMobilePhoneNorm(associate_mobile, mobile_phone) as mobile_phone,
  uparea_id as uparea_id,
  deyeCompleteUpareaTypeMap(uparea_id) as uparea_id_map,
  deyeUserNameNorm(username) as username,
  deyePasswordNorm(password) as password,
  imei as imei,
  imsi as imsi,
  '' as cell_id,
  '' as lac,
  '' as mcc,
  '' as mnc,
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
  case
    when position(':' in host)>0 then substring(host,1,position(':' in host) -1)
    else host
  end as host,
  hardware_sign as hardware_sign,
  hardware_type as hardware_type,
  local_action as local_action,
  deyeMacNorm(mac) as mac,
  IF(user_agent<>'',T11.os_type_l,deyeOsNameNorm(os_name)) as os_name,
  os_version as os_version,
  IF(user_agent<>'',T11.terminal_type_l,'') as terminal_type,
  deyeCompleteTerminalTypeMap(IF(user_agent<>'',T11.terminal_type_l,'')) as terminal_type_map,
  IF(user_agent<>'',T11.browse_type_l,deyeBrowseTypeNorm(browse_type)) as browse_type,
  deyeCompleteBrowseTypeMap(IF(user_agent<>'',T11.browse_type_l,deyeBrowseTypeNorm(browse_type))) as browse_type_map,
  browse_version as browse_version,
  file_id as file_id,
  file_path as file_path,
  file_size as file_size,
  file_md5 as file_md5,
  attach_num as attach_num,
  attach_names as attach_names,
  attach_sizes as attach_sizes,
  attach_download_path as attach_download_path,
  attach_md5s as attach_md5s,
  IF(CHAR_LENGTH(attach_text) > 3145728,CONCAT(SUBSTR(attach_text,0,3145728),'...'),attach_text) as attach_text,
  subject as subject,
  keywords as keywords,
  IF(CHAR_LENGTH(text) > 3145728,CONCAT(SUBSTR(text,0,3145728),'...'),text) as text,
  SPLIT_INDEX(IF(LEFT(lang_type,1)='[' AND RIGHT(lang_type,1)=']', SUBSTR(lang_type,2,CHAR_LENGTH(lang_type)-2), lang_type),' ',0) as lang_type,
  deyeUrlDecode(url) as url,
  T8.domain_l as `domain`,
  deyeUrlDecode(deyeProduceRelativeUrl(url)) as relative_url,
  deyeSubDomainNorm(T8.domain_l) as sub_domain,
  deyeSuperDomainNorm(T8.domain_l) as super_domain,
  '' as app_name,
  '' as app_type,
  IF(child_type IN ('1010001','1010002','1010003','1010005'),deyeCompleteChildTypeMap('1019999', data_type)[2],deyeCompleteChildTypeMap(child_type, data_type)[2]) as tool_name,
  IF(child_type IN ('1010001','1010002','1010003','1010005'),'1019999',deyeCompleteChildTypeMap(child_type, data_type)[1]) as tool_type,
  from_nickname as from_nickname,
  to_nickname as to_nickname,
  T10.from_account_l as from_account,
  T10.from_account_remark_l as from_account_remark,
  T10.to_account_l as to_account,
  T10.to_account_remark_l as to_account_remark,
  data_id as data_id,
  capture_time as capture_time,
  T9.capture_day_l as capture_day,
  T9.capture_hour_l as capture_hour,
  deyeTimestampConverter(data_id) as insert_time,
  '' as mark,
  device_id as device_id,
  trace_id as trace_id,
  app_version as app_version,
  charset as charset,
  mail_date as mail_date,
  deyeProduceMailCategory(T4.mail_from_l) as mail_category,
  T4.mail_from_l as mail_from,
  T4.mail_from_remark_l as mail_from_remark,
  T5.mail_to_l as mail_to,
  T5.mail_to_remark_l as mail_to_remark,
  T6.mail_cc_l as mail_cc,
  T6.mail_cc_remark_l as mail_cc_remark,
  T7.mail_bcc_l as mail_bcc,
  T7.mail_bcc_remark_l as mail_bcc_remark,
  mail_tool_type as mail_tool_type,
  encrypt_flag as encrypt_flag,
  forgery_flag as forgery_flag,
  spam_flag as spam_flag,
  spam_score as spam_score,
  insert_time as spam_mark_time,
  user_agent as user_agent,
  proxy_account as proxy_account,
  proxy_address as proxy_address,
  proxy_provider as proxy_provider,
  proxy_type as proxy_type,
  abstract as abstract,
  favorite_tags as favorite_tags,
  host_url_tag as host_url_tag,
  icp_provider as icp_provider,
  message_id as message_id,
  remark as remark,
  session_id as session_id,
  soft_name as soft_name,
  software as software,
  content_type as content_type,
  stream_end_time as stream_end_time,
  forgery_user as forgery_user,
  deyeProduceSimHash(mail_from, subject,data_id) as sim_hash,
  important_level as important_level,
  '' as entity_list,
  '' as province_attribution,
  deyeGetTimeSpan(capture_time) as time_span,
  '' as number_risk_level,
  '' as radius_risk,
  '' as mail_risk,
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
  CASE
    WHEN spam_flag = 1 THEN '90001'
    ELSE ''
  END AS mail_classify,
  '' as mail_anomaly,
  '' as mail_importance,
  '' as proper_noun,
  CASE
      WHEN child_type in ('1010001', '1010002','1010003','1010005') THEN child_type
      WHEN application_layer_protocol='03' THEN '1010001'
      WHEN application_layer_protocol='04' THEN '1010002'
      WHEN application_layer_protocol='05' THEN '1010005'
      ELSE '1019999'
      END AS sub_protocol,
  CASE
      WHEN child_type = '1010001' THEN 'SMTP'
      WHEN child_type = '1010002' THEN 'POP3'
      WHEN child_type = '1010003' THEN 'WEBMAIL'
      WHEN child_type = '1010005' THEN 'IMAP'
      WHEN application_layer_protocol='03' THEN 'SMTP'
      WHEN application_layer_protocol='04' THEN 'POP3'
      WHEN application_layer_protocol='05' THEN 'IMAP'
      ELSE deyeCompleteChildTypeMap('1019999', data_type)[2]
  END AS sub_protocol_map,
  deyeVirtualAccountNorm(CONCAT_WS(',',T5.mail_to_l,T6.mail_cc_l,T7.mail_bcc_l,T4.mail_from_l)) AS format_virtual_account,
  LOWER(username) as format_username,
  deyeCompleteDataTypeMap(data_type, true)[1] as norm_data_type,
  deyeProduceImportantTargetFlag(data_type,T3.auth_account_l,T3.auth_type_l,deyeIpNorm(strsrc_ip),deyeIpNorm(strsrc_ip_v6),deyeIpNorm(strdst_ip),deyeIpNorm(strdst_ip_v6)) as important_target_flag
FROM
  ods_pr_source_0002_email,
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
  LATERAL TABLE(deyeMailAccountHandle(mail_from)) as T4(mail_from_l, mail_from_remark_l),
  LATERAL TABLE(deyeMailAccountHandle(mail_to)) as T5(mail_to_l, mail_to_remark_l),
  LATERAL TABLE(deyeMailAccountHandle(mail_cc)) as T6(mail_cc_l, mail_cc_remark_l),
  LATERAL TABLE(deyeMailAccountHandle(mail_bcc)) as T7(mail_bcc_l, mail_bcc_remark_l),
  LATERAL TABLE(deyeDomainHandle(`domain`, url)) as T8(domain_l),
  LATERAL TABLE(deyeTimeHandle(capture_time)) as T9(capture_day_l, capture_hour_l),
  LATERAL TABLE(
    deyeMainAccountHandle(
      data_type,
      action,
      username,
      ARRAY [ T4.mail_from_l, T4.mail_from_remark_l ],
      ARRAY [ T5.mail_to_l, T5.mail_to_remark_l, T6.mail_cc_l, T6.mail_cc_remark_l, T7.mail_bcc_l, T7.mail_bcc_remark_l]
    )
  ) as T10(
    main_account_l,
    from_account_l,
    from_account_remark_l,
    to_account_l,
    to_account_remark_l
  ),
  LATERAL TABLE(deyeUserAgentHandle(user_agent)) as T11(browse_type_l, os_type_l, terminal_type_l)
WHERE
(
    (data_type = '101')
  AND (deyeUserNameNorm(username) <> '' or T4.mail_from_l <> '' or T5.mail_to_l <> '' or T6.mail_cc_l <> '' or T7.mail_bcc_l <> '')
  AND `action` <> '50'
  AND (deyeVerifyTimestamp(capture_time, -2) = '1')
  AND (deyeVerifyTimestamp(capture_time, 1) = '0')
  AND (
    deyeVerifyIp(
    strsrc_ip,
    strsrc_ip_v6
    ) = '1'
    )
  AND (
    deyeVerifyIp(
    strdst_ip,
    strdst_ip_v6
    ) = '1'
    )
  AND if(child_type = '1010001' and action = '32' and (T4.mail_from_l = '' or T5.mail_to_l = ''), 1 = 0, 1 = 1)
    )
  AND (
    CHAR_LENGTH(T3.auth_account_l) <= 128
  AND CHAR_LENGTH(deyeMobilePhoneNorm(associate_mobile, mobile_phone)) <= 30
  AND CHAR_LENGTH(deyeUserNameNorm(username)) <= 512
  AND CHAR_LENGTH(deyePasswordNorm(password)) <= 512
  AND CHAR_LENGTH(T4.mail_from_l) <= 256
  AND deyeAccountFilter(T5.mail_to_l, 256)
  AND deyeAccountFilter(T6.mail_cc_l, 256)
  AND deyeAccountFilter(T7.mail_bcc_l, 256)
  AND CHAR_LENGTH(from_nickname) <= 256
  AND CHAR_LENGTH(T10.from_account_l) <= 256
  AND deyeAccountFilter(to_nickname, 256)
  AND deyeAccountFilter(T10.to_account_l, 256)
  AND CHAR_LENGTH(charset) <= 128
);