SELECT  'nf固网关联率'                                     AS cal_type
       ,'2025-07-08'                                   AS cal_day
       ,'00-23'                     AS cal_hour
       ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '2025-07-08'
	AND user_name not LIKE '2136%'
	AND dev_id in (
		'78b822bd',
		'484369c1',
		'10411f77',
		'f65c9036',
		'f621e11e',
		'd9347bd0',
		'40ba6628',
		'5d96e70e',
		'1281214e',
		'b010fecd',
		'733b606e',
		'7376a8cf',
		'cd7f1050',
		'8e4f4bff',
		'44f9077d'
	)
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '2025-07-08'
	AND user_name not LIKE '2136%'
	AND dev_id in (
		'78b822bd',
		'484369c1',
		'10411f77',
		'f65c9036',
		'f621e11e',
		'd9347bd0',
		'40ba6628',
		'5d96e70e',
		'1281214e',
		'b010fecd',
		'733b606e',
		'7376a8cf',
		'cd7f1050',
		'8e4f4bff',
		'44f9077d'
	)
) nf
JOIN
(
	SELECT  ip 
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day in ('2025-07-08','2025-07-07')
	AND ACTION IN ('login', 'logout')
	GROUP BY ip
) radius
ON radius.ip = nf.strsrc_ip;
