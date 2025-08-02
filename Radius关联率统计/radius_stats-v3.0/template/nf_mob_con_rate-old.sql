SELECT  'nf移网关联率'                                    AS cal_type
       ,'{cal_day}'                                  AS cal_day
       ,'{start_hour}-{end_hour}'                    AS cal_hour
       ,SUM(if(user_name LIKE '2136%',1,0))          AS connect_account
       ,SUM(1)                                       AS total_account
       ,SUM(if(user_name LIKE '2136%',1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
	GROUP BY  user_name
	         ,strsrc_ip
	         ,src_port
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
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
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND action = 'Start'
	GROUP BY  internet_ip
	         ,calling_station_id
	         ,port_range
) radius
ON nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port;