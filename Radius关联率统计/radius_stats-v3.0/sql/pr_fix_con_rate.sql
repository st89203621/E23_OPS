SELECT  'pr固网关联率'                                                                    AS cal_type
       ,'2025-08-01'                                                                  AS cal_day
       ,'00-23'                                                    AS cal_hour
       ,'210213'                                           AS uparea_id
	   ,SUM(if (pr.auth_account is not null AND pr.auth_account <> '',1,0))          AS connect_account
       ,SUM(1)                                                                       AS total_account
       ,SUM(if (pr.auth_account is not null AND pr.auth_account <> '',1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  ip
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-08-01'
	AND ACTION IN ('login', 'logout')
	GROUP BY  ip
) radius
JOIN
(
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_http_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_im_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_email_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_remotectrl_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_ftp_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_game_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_p2p_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_telnet_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_vpn_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
	WHERE capture_day = '2025-08-01'
	AND capture_hour BETWEEN '00' AND '23'
	AND auth_account not LIKE '213%'
	AND uparea_id = '210213'
) pr
ON radius.ip = pr.strsrc_ip;
