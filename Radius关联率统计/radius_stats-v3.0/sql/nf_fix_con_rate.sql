SELECT  'nf固网关联率'                                     AS cal_type
       ,'2025-08-01'                                   AS cal_day
       ,'00-23'                     AS cal_hour
       ,'230215'                                           AS uparea_id
	   ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND user_name not LIKE '213%'
	AND uparea_id = '230215'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND user_name not LIKE '213%'
	AND uparea_id = '230215'
) nf
JOIN
(
	SELECT  ip 
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-08-01'
	AND ACTION IN ('login', 'logout')
	GROUP BY ip
) radius
ON radius.ip = nf.strsrc_ip;
