SELECT  'nf移网关联准确率'                                                        AS cal_type
       ,'2025-07-31'                                                        AS cal_day
       ,'00-23'                                          AS cal_hour
	   ,'230215'                                           AS uparea_id
       ,SUM(if(lower(calling_station_id) = lower(user_name),1,0))          AS connect_account
       ,SUM(1)                                                             AS total_account
       ,SUM(if(lower(calling_station_id) = lower(user_name),1,0)) / SUM(1) AS acc_rate
FROM
(
	SELECT  nf.strsrc_ip
	       ,nf.user_name
	       ,nf.capture_time
	       ,radius.calling_station_id
	       ,radius.capture_time                                                  AS r_capture_time
	       ,(nf.capture_time - (radius.capture_time + 0 * 1000)) AS time_diff
	       ,DENSE_RANK() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY  nf.capture_time - (radius.capture_time + 0 * 1000)) AS rn
	FROM
	(
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		       ,src_port
		FROM v64_deye_dw_ods.ods_nf_other_log_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND user_name LIKE '213%' 
		AND uparea_id = '230215'
		UNION ALL
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		       ,src_port
		FROM v64_deye_dw_ods.ods_nf_url_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND user_name LIKE '213%' 
		AND uparea_id = '230215'
	) nf
	JOIN
	(
		SELECT  internet_ip
		       ,calling_station_id
		       ,cast(split(port_range,'-')[0] AS int) AS start_port
		       ,cast(split(port_range,'-')[1] AS int) AS end_port
		       ,capture_time
		FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND action = 'Start' 
		GROUP BY internet_ip,calling_station_id,port_range,capture_time
	) radius
	ON nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
	WHERE nf.capture_time >= (radius.capture_time + 0 * 1000) 
) tmp
WHERE rn = 1;
