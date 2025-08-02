SELECT  'nf移网关联准确率'                                                        AS cal_type
       ,'{cal_day}'                                                        AS cal_day
       ,'{start_hour}-{end_hour}'                                          AS cal_hour
       ,'{uparea_id}'                                           AS uparea_id
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
	       ,(nf.capture_time - (radius.capture_time + {nf_offline_time} * 1000)) AS time_diff
	       ,DENSE_RANK() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY  nf.capture_time - (radius.capture_time + {nf_offline_time} * 1000)) AS rn
	FROM
	(
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		       ,src_port
		FROM v64_deye_dw_ods.ods_nf_other_log
		WHERE insert_day = '{cal_day}'
		AND insert_hour BETWEEN '{start_hour}' AND '{end_hour}'
		and capture_time >= {start_time}
		and capture_time < {end_time}
		AND user_name LIKE '213%' 
		AND uparea_id = '{uparea_id}'
		UNION ALL
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		       ,src_port
		FROM v64_deye_dw_ods.ods_nf_url
		WHERE insert_day = '{cal_day}'
		AND insert_hour BETWEEN '{start_hour}' AND '{end_hour}'
		and capture_time >= {start_time}
		and capture_time < {end_time}
		AND user_name LIKE '213%' 
		AND uparea_id = '{uparea_id}'
	) nf
	JOIN
	(
		SELECT  internet_ip
		       ,calling_station_id
		       ,cast(split(port_range,'-')[0] AS int) AS start_port
		       ,cast(split(port_range,'-')[1] AS int) AS end_port
		       ,capture_time
		FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
		WHERE capture_day = '{cal_day}'
		AND action = 'Start' 
		GROUP BY internet_ip,calling_station_id,port_range,capture_time
	) radius
	ON nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
	WHERE nf.capture_time >= (radius.capture_time + {nf_offline_time} * 1000) 
) tmp
WHERE rn = 1;
