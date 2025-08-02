SELECT  'pr移网关联率'                               AS cal_type
       ,'{cal_day}'                             AS cal_day
       ,'{start_hour}-{end_hour}'               AS cal_hour
	   ,'{uparea_id}'                           AS uparea_id
       ,SUM(if(auth_account LIKE '213%',1,0))         AS connect_account
       ,sum (1)                                 AS total_account
       ,SUM(if(auth_account LIKE '213%',1,0))/sum (1) AS con_rate
FROM
(
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_http_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_im_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_email_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_remotectrl_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_ftp_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_game_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_p2p_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_telnet_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_vpn_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
	UNION ALL
	SELECT  strsrc_ip
	       ,auth_account
	       ,src_port
	FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
	WHERE capture_day = '{cal_day}'
	AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND (auth_account LIKE '213%' or auth_account = '')
	AND uparea_id = '{uparea_id}'
) pr
where EXISTS (
	select 1 from (
	SELECT  internet_ip
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day = '{cal_day}'
	AND action in ('Start', 'Stop')
	GROUP BY internet_ip,port_range) radius
	where pr.strsrc_ip = radius.internet_ip AND pr.src_port >= start_port AND pr.src_port <= end_port
);