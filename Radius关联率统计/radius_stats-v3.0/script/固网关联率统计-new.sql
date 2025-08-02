SELECT  'nf固网关联率'                                     AS cal_type
       ,'2025-05-04'                                   AS cal_day
       ,'00-09'                     AS cal_hour
       ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '2025-04-17'
	--FROM v64_deye_dw_ods.ods_nf_other_log
	--WHERE insert_day = '2025-05-04'
	--AND insert_hour BETWEEN '00' AND '09'
	--and capture_time >= 1746313200000
	--and capture_time < 1746345600000
	AND user_name not LIKE '2136%'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '2025-04-17'
	--FROM v64_deye_dw_ods.ods_nf_url
	--WHERE insert_day = '2025-05-04'
	--AND insert_hour BETWEEN '00' AND '09'
	--and capture_time >= 1746313200000
	--and capture_time < 1746345600000
	AND user_name not LIKE '2136%'
) nf
JOIN
(
	SELECT  ip 
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day in ('2025-04-17')
	AND ACTION IN ('login', 'logout')
	GROUP BY ip
) radius
ON radius.ip = nf.strsrc_ip;