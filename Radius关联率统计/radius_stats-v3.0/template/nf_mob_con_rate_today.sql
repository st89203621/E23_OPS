SELECT  'nf移网关联率'                                    AS cal_type
       ,'{cal_day}'                                  AS cal_day
       ,'{start_hour}-{end_hour}'                    AS cal_hour
	   ,'{uparea_id}'                                           AS uparea_id
       ,SUM(if(user_name LIKE '213%',1,0))          AS connect_account
       ,SUM(1)                                       AS total_account
       ,SUM(if(user_name LIKE '213%',1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '{cal_day}'
	AND insert_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND capture_time >= unix_timestamp('{cal_day} {start_hour}:00:00') * 1000
	AND capture_time < unix_timestamp('{cal_day} {end_hour}:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND (user_name LIKE '213%' or user_name = strsrc_ip)
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '{cal_day}'
	AND insert_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND capture_time >= unix_timestamp('{cal_day} {start_hour}:00:00') * 1000
	AND capture_time < unix_timestamp('{cal_day} {end_hour}:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND (user_name LIKE '213%' or user_name = strsrc_ip)
	AND uparea_id = '{uparea_id}'
) nf
where EXISTS (
	select 1 from (
	SELECT  internet_ip
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day = '{cal_day}'
	AND action in ('Start', 'Stop')
	GROUP BY internet_ip,port_range) radius
	where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
);
