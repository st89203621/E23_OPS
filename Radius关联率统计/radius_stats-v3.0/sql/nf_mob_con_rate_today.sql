SELECT  'nf移网关联率'                                    AS cal_type
       ,'2025-07-09'                                  AS cal_day
       ,'00-09'                    AS cal_hour
	   ,'230215'                                           AS uparea_id
       ,SUM(if(user_name LIKE '213%',1,0))          AS connect_account
       ,SUM(1)                                       AS total_account
       ,SUM(if(user_name LIKE '213%',1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-07-09'
	AND insert_hour BETWEEN '00' AND '09'
	AND capture_time >= unix_timestamp('2025-07-09 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-07-09 09:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND (user_name LIKE '213%' or user_name = strsrc_ip)
	AND uparea_id = '230215'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-07-09'
	AND insert_hour BETWEEN '00' AND '09'
	AND capture_time >= unix_timestamp('2025-07-09 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-07-09 09:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND (user_name LIKE '213%' or user_name = strsrc_ip)
	AND uparea_id = '230215'
) nf
where EXISTS (
	select 1 from (
	SELECT  internet_ip
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day = '2025-07-09'
	AND action in ('Start', 'Stop')
	GROUP BY internet_ip,port_range) radius
	where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
);
