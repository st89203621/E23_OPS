SELECT  'nf固网关联率'                                     AS cal_type
       ,'{cal_day}'                                   AS cal_day
       ,'{start_hour}-{end_hour}'                     AS cal_hour
       ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '{cal_day}'
	AND insert_hour BETWEEN '{start_hour}' AND '{end_hour}'
	and capture_time >= {start_time}
	and capture_time < {end_time}
	AND user_name not LIKE '2136%'
	GROUP BY  user_name
	         ,strsrc_ip
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '{cal_day}'
	AND insert_hour BETWEEN '{start_hour}' AND '{end_hour}'
	and capture_time >= {start_time}
	and capture_time < {end_time}
	AND user_name not LIKE '2136%'
	GROUP BY  user_name
	         ,strsrc_ip
) nf
JOIN
(
	SELECT  ip
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	GROUP BY  ip
) radius
ON radius.ip = nf.strsrc_ip;