SELECT  'pr固网关联准确率'                                  AS cal_type
       ,'2025-07-31'                                  AS cal_day
       ,'00-23'                    AS cal_hour
	   ,'230215'                                           AS uparea_id
       ,SUM(IF(auth_account = account,1,0))          AS connect_account
       ,SUM(1)                                       AS total_account
       ,SUM(IF(auth_account = account,1,0)) / SUM(1) AS acc_rate
FROM
(
	SELECT  pr.strsrc_ip
	       ,pr.auth_account
	       ,pr.capture_time
	       ,radius.account
	       ,radius.capture_time                                                                                                           AS r_capture_time
	       ,(pr.capture_time - (radius.capture_time + 0 * 1000))                                                                                       AS time_diff
	       ,DENSE_RANK() OVER (PARTITION BY pr.strsrc_ip,pr.auth_account,pr.capture_time ORDER BY  pr.capture_time - (radius.capture_time + 0 * 1000)) AS rn
	FROM
	(
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_http_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_im_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_email_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_remotectrl_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_ftp_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_game_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_p2p_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_telnet_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_vpn_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
		WHERE capture_day = '2025-07-31'
		AND capture_hour BETWEEN '00' AND '23'
		AND auth_account <> '' 
		AND auth_account not LIKE '213%'
		AND uparea_id = '230215'
	) pr
	JOIN
	(
		SELECT  ip
		       ,account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_fixnet_radius_store
		WHERE capture_day = '2025-07-31'
		AND action = 'login' 
		GROUP BY ip,account,capture_time
	) radius
	ON pr.strsrc_ip = radius.ip
	WHERE pr.capture_time >= radius.capture_time + 0 * 1000 
) tmp
WHERE rn = 1
