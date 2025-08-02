set hive.exec.dynamic.partition.mode=nonstrict;
insert into v64_deye_dw_ods.ods_nf_con_fail_tmp
SELECT  *
FROM
(
	SELECT DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd HH:mm:ss') as capture_date
		   ,user_name
	       ,strsrc_ip
	       ,src_port
	       ,CONCAT(1024 + (FLOOR((src_port - 1024) / 126)) * 126,'-',1024 + (FLOOR((src_port - 1024) / 126)) * 126 + 125) as port_range
	       ,dev_id
	       ,capture_time
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd') as capture_day
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'HH') as capture_hour
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-05'
	--AND insert_hour BETWEEN '00' AND '09'
	--and capture_time >= unix_timestamp('2025-05-05 00:00:00') * 1000
	--and capture_time < unix_timestamp('2025-05-05 01:30:00') * 1000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
	UNION ALL
	SELECT DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd HH:mm:ss') as capture_date
		   ,user_name
	       ,strsrc_ip
	       ,src_port
	       ,CONCAT(1024 + (FLOOR((src_port - 1024) / 126)) * 126,'-',1024 + (FLOOR((src_port - 1024) / 126)) * 126 + 125) as port_range
	       ,dev_id
	       ,capture_time
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd') as capture_day
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'HH') as capture_hour
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-05-05'
	--AND insert_hour BETWEEN '00' AND '09'
	--and capture_time >= unix_timestamp('2025-05-05 00:00:00') * 1000
	--and capture_time < unix_timestamp('2025-05-05 01:30:00') * 1000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
) nf
where EXISTS (
	select 1 from (
	SELECT  internet_ip
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day in ('2025-05-05', '2025-05-04')
	and action in ('Start', 'Stop')
	GROUP BY internet_ip,port_range) radius
	where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
) and user_name like '%.%';