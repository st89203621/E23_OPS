SELECT  'nf固网关联率-C1'                                     AS cal_type
       ,'{cal_day}'                                   AS cal_day
       ,'{start_hour}-{end_hour}'                     AS cal_hour
       ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM deye_dw_ods.ods_nf_other_log_his
	WHERE capture_day = '2025-05-02'
	--AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND user_name not LIKE '2136%'
	AND uparea_id = '230215'
	GROUP BY  user_name
	         ,strsrc_ip
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM deye_dw_ods.ods_nf_url_his
	WHERE capture_day = '2025-05-02'
	--AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	AND user_name not LIKE '2136%'
	AND uparea_id = '230215'
	GROUP BY  user_name
	         ,strsrc_ip
) nf
JOIN
(
	SELECT  ip
	FROM deye_dw_ods.ods_radius_his
	WHERE capture_day = '2025-05-02'
	--AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
	GROUP BY  ip
) radius
ON radius.ip = nf.strsrc_ip;