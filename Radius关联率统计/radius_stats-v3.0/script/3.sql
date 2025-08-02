SELECT  'nf固网关联准确率'                                             AS cal_type
       ,'2025-07-07'                                             AS cal_day
	   ,SUM(IF(lower(user_name) = lower(account),1,0))          AS connect_account
       ,SUM(1)                                                  AS total_account
       ,SUM(IF(lower(user_name) = lower(account),1,0)) / SUM(1) AS acc_rate
FROM
(
	SELECT  nf.strsrc_ip
	       ,nf.user_name
	       ,nf.capture_time
	       ,radius.account
	       ,radius.capture_time                                                  AS r_capture_time
	       ,ROW_NUMBER() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY  nf.capture_time - radius.capture_time) AS rn
	FROM
	(
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		FROM v64_deye_dw_ods.ods_nf_other_log_store
		WHERE capture_day = '2025-07-07'
		AND user_name <> strsrc_ip
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
		       ,capture_time
		FROM v64_deye_dw_ods.ods_nf_url_store
		WHERE capture_day = '2025-07-07'
		AND user_name <> strsrc_ip
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
		       ,account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_fixnet_radius_store
		WHERE capture_day in ('2025-07-07', '2025-07-06')
		AND action = 'login' 
	) radius
	ON nf.strsrc_ip = radius.ip
	WHERE nf.capture_time >= radius.capture_time
) tmp
WHERE rn = 1
