SELECT  'pr固网关联率'                                                                    AS cal_type
       ,'{cal_day}'                                                                  AS cal_day
       ,'{start_hour}-{end_hour}'                                                    AS cal_hour
       ,'{uparea_id}'                                           AS uparea_id
	   ,SUM(if (pr.auth_account is not null AND pr.auth_account <> '',1,0))          AS connect_account
       ,SUM(1)                                                                       AS total_account
       ,SUM(if (pr.auth_account is not null AND pr.auth_account <> '',1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  ip
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '{cal_day}'
	AND ACTION IN ('login', 'logout')
	GROUP BY  ip
) radius
JOIN
(
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_http_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_im_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_email_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_remotectrl_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_ftp_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_game_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_p2p_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_telnet_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_vpn_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND auth_account not LIKE '213%'
	AND uparea_id = '{uparea_id}'
) pr
ON radius.ip = pr.strsrc_ip;