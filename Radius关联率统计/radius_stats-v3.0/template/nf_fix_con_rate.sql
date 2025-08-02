SELECT  'nf固网关联率'                                     AS cal_type
       ,'{cal_day}'                                   AS cal_day
       ,'{start_hour}-{end_hour}'                     AS cal_hour
       ,'{uparea_id}'                                           AS uparea_id
	   ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND user_name not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND user_name not LIKE '213%'
	AND uparea_id = '{uparea_id}'
) nf
JOIN
(
	SELECT  ip 
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '{cal_day}'
	AND ACTION IN ('login', 'logout')
	GROUP BY ip
) radius
ON radius.ip = nf.strsrc_ip;