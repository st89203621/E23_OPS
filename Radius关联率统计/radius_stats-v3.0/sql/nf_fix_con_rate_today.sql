SELECT  'nf固网关联率'                                     AS cal_type
       ,'2025-07-09'                                   AS cal_day
       ,'00-09'                     AS cal_hour
       ,'230215'                                           AS uparea_id
	   ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-07-09'
	AND insert_hour BETWEEN '00' AND '09'
	AND capture_time >= unix_timestamp('2025-07-09 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-07-09 09:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND user_name not LIKE '213%' 
	AND uparea_id = '230215'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-07-09'
	AND insert_hour BETWEEN '00' AND '09'
	AND capture_time >= unix_timestamp('2025-07-09 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-07-09 09:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND user_name not LIKE '213%' 
	AND uparea_id = '230215'
) nf
JOIN
(
	SELECT  ip
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-07-09'
	GROUP BY  ip
) radius
ON radius.ip = nf.strsrc_ip;
