SELECT  'nf移网关联率'                                    AS cal_type
       ,'2025-05-02'                                  AS cal_day
       ,'00-12'                    AS cal_hour
       ,SUM(if(user_name LIKE '2136%',1,0))          AS connect_account
       ,SUM(1)                                       AS total_account
       ,SUM(if(user_name LIKE '2136%',1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '2025-05-02'
	--WHERE insert_day = '2025-05-02'
	--AND insert_hour BETWEEN '17' AND '18'
	--and capture_time >= 1746201600000
	--and capture_time < 1746205200000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '2025-05-02'
	--WHERE insert_day = '2025-05-02'
	--AND insert_hour BETWEEN '17' AND '18'
	--and capture_time >= 1746201600000
	--and capture_time < 1746205200000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
) nf
where EXISTS (
	select 1 from (
	SELECT  internet_ip
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day in ('2025-05-01', '2025-05-02')
	AND action in ('Start', 'Stop')
	GROUP BY internet_ip,port_range) radius
	where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
);