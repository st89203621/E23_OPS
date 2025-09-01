SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO test_dw_ods.ods_nf_other_log_store_tmp_083101 
SELECT  data_type,
  user_group,
  user_name,
  strsrc_ip,
  strdst_ip,
  src_port,
  dst_port,
  ip_type,
  protocol,
  file_name,
  file_type,
  url,
  domain,
  dns,
  net_action,
  dev_id,
  terminal_type,
  mac,
  app_type,
  app_name,
  is_webapp,
  imei,
  imsi,
  mcc,
  mnc,
  lac,
  cell_id,
  phone_number,
  url_data,
  text,
  line_id,
  policy,
  site,
  trace_t,
  nf_capture_time as capture_time,  -- 使用别名
  insert_time,
  data_id,
  uparea_id,
  capture_day,
  capture_hour
FROM
(
        SELECT  nf.*,
               radius.account,
               nf.capture_time as nf_capture_time,      -- NF的capture_time
               radius.capture_time as radius_capture_time, -- Radius的capture_time
               ROW_NUMBER() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY nf.capture_time - radius.capture_time) AS rn
        FROM
        (
                SELECT  *
                FROM v64_deye_dw_ods.ods_nf_other_log_store
                WHERE capture_day = '2025-08-30'
                AND capture_hour BETWEEN '00' AND '23'
                AND user_name <> strsrc_ip
                AND uparea_id = '220214'
        ) nf
        JOIN
        (
                SELECT  internet_ip,
                       user_name as account,
                       capture_time
                FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
                WHERE capture_day in ('2025-08-29', '2025-08-30')
                AND action = 'Start' 
        ) radius
        ON nf.strsrc_ip = radius.internet_ip
        WHERE nf.capture_time >= radius.capture_time
) tmp
WHERE rn = 1 and lower(user_name) <> lower(account);