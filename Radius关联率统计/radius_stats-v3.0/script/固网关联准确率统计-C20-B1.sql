SELECT  'nf固网关联准确率-B1'                                             AS cal_type
       ,'2025-04-24'                                            AS cal_day
       ,'12-13'                                                 AS cal_hour
       ,SUM(IF(lower(user_name) = lower(account),1,0))          AS connect_account
       ,SUM(1)                                                  AS total_account
       ,SUM(IF(lower(user_name) = lower(account),1,0)) / SUM(1) AS acc_rate
FROM
(
	SELECT  nf.strsrc_ip
	       ,nf.user_name
	       ,nf.capture_time     -- 添加了逗号
	       ,radius.account
	       ,radius.capture_time                                                                                                          AS r_capture_time
	       ,(nf.capture_time - (radius.capture_time))                                                                                    AS time_diff
	       ,ROW_NUMBER() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY  nf.capture_time - (radius.capture_time)) AS rn
	FROM
	(
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		FROM deye_dw_ods.ods_nf_other_log_his 
		WHERE capture_day = '2025-05-03'
		AND user_name <> strsrc_ip
		AND user_name not LIKE '2136%' 
		AND uparea_id = '220214'
		UNION ALL
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		FROM deye_dw_ods.ods_nf_url_his 
		WHERE capture_day = '2025-05-03'
		AND user_name <> strsrc_ip
		AND user_name not LIKE '2136%' 
		AND uparea_id = '220214'
	) nf
	JOIN
	(
		SELECT  ip
		       ,account
		       ,capture_time
		FROM deye_dw_ods.ods_radius_his
		WHERE capture_day = '2025-05-03'
		AND action = 'login' 
	) radius
	ON nf.strsrc_ip = radius.ip
	WHERE nf.capture_time >= radius.capture_time 
) tmp
WHERE rn = 1;