SELECT  'nf固网关联率-C1'                                     AS cal_type
       ,'2025-05-02'                                   AS cal_day
       ,'15-18'                     AS cal_hour
       ,SUM(if (user_name <> strsrc_ip,1,0))          AS connect_account
       ,SUM(1)                                        AS total_account
       ,SUM(if (user_name <> strsrc_ip,1,0)) / SUM(1) AS con_rate
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	--FROM deye_dw_ods.ods_nf_other_log_his
	--WHERE capture_day = '2025-05-03'
	FROM deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-04'
	--AND insert_hour BETWEEN '15' AND '18'
	and capture_time >= unix_timestamp('2025-05-04 00:00:00') * 1000
	and capture_time < unix_timestamp('2025-05-05 00:00:00') * 1000
	AND user_name not LIKE '2136%'
    AND uparea_id = '220214'
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	--FROM deye_dw_ods.ods_nf_url_his
	--WHERE capture_day = '2025-05-03'
	FROM deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-04'
	--AND insert_hour BETWEEN '15' AND '18'
	and capture_time >= unix_timestamp('2025-05-04 00:00:00') * 1000
	and capture_time < unix_timestamp('2025-05-05 00:00:00') * 1000
	AND user_name not LIKE '2136%'
    AND uparea_id = '220214'
) nf
JOIN
(
	SELECT  ip 
	--FROM deye_dw_ods.ods_radius_his
	--WHERE capture_day in ('2025-05-04')
	FROM deye_dw_ods.ods_radius
	WHERE insert_day = '2025-05-04'
	AND action IN ('login', 'logout')
	GROUP BY ip
) radius
ON radius.ip = nf.strsrc_ip;