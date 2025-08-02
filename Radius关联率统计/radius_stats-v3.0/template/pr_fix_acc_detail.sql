SELECT  strsrc_ip
       ,auth_account
       ,capture_time
       ,data_type
       ,data_id
       ,account
       ,r_capture_time
FROM
(
	SELECT  pr.strsrc_ip
	       ,pr.auth_account
	       ,pr.capture_time
	       ,pr.data_type
	       ,pr.data_id
	       ,radius.account
	       ,radius.capture_time                                                AS r_capture_time
	       ,(pr.capture_time - radius.capture_time + {pr_offline_time} * 1000) AS time_diff
	       ,DENSE_RANK() OVER (PARTITION BY pr.strsrc_ip,pr.auth_account,pr.capture_time ORDER BY  pr.capture_time - radius.capture_time + {pr_offline_time} * 1000) AS rn
	FROM
	(
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_http_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_im_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_email_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_remotectrl_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_ftp_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_game_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_p2p_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_telnet_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_vpn_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
		WHERE capture_day = '{cal_day}'
		AND auth_account <> ''
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
	) pr
	JOIN
	(
		SELECT  ip
		       ,account
		       ,capture_time
		FROM v64_deye_dw_ods.ods_fixnet_radius_store
		WHERE capture_day = '{cal_day}'
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
		AND action = 'login' 
		GROUP BY ip,account,capture_time
	) radius
	ON pr.strsrc_ip = radius.ip
	WHERE pr.capture_time >= radius.capture_time + {pr_offline_time} * 1000 
) tmp
WHERE rn = 1
AND auth_account <> account