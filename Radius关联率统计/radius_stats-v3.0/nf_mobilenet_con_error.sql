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
  capture_time,
  insert_time,
  data_id,
  uparea_id,
  capture_day,
  capture_hour
FROM v64_deye_dw_ods.ods_nf_other_log_store nf
WHERE capture_day = '2025-08-30'
AND capture_hour BETWEEN '00' AND '23'
AND (user_name LIKE '213%' or user_name = strsrc_ip)
AND uparea_id = '220214'
AND NOT EXISTS (
        select 1 from (
        SELECT  internet_ip
               ,cast(split(port_range,'-')[0] AS int) AS start_port
               ,cast(split(port_range,'-')[1] AS int) AS end_port
        FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
        WHERE capture_day = '2025-08-30'
        AND action in ('Start', 'Stop')
        GROUP BY internet_ip,port_range) radius
        where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
);