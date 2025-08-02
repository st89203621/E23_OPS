SELECT  *
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-04-28'
	AND insert_hour BETWEEN '00' AND '13'
	and capture_time >= 1745794800000
	and capture_time < 1745838000000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
	GROUP BY  user_name
	         ,strsrc_ip
	         ,src_port
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-04-28'
	AND insert_hour BETWEEN '00' AND '13'
	and capture_time >= 1745794800000
	and capture_time < 1745838000000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
	GROUP BY  user_name
	         ,strsrc_ip
	         ,src_port
) nf
JOIN
(
	SELECT  internet_ip
	       ,calling_station_id
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day = '2025-04-28'
	AND capture_hour BETWEEN '00' AND '12'
	AND action = 'Start'
	GROUP BY  internet_ip
	         ,calling_station_id
	         ,port_range
) radius
ON nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port and user_name not LIKE '2136%';