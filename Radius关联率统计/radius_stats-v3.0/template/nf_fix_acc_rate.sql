SELECT  'nf固网关联准确率'                                             AS cal_type
       ,'{cal_day}'                                             AS cal_day
       ,'{start_hour}-{end_hour}'                               AS cal_hour
       ,'{uparea_id}'                                           AS uparea_id
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
	       ,(nf.capture_time - (radius.capture_time + {nf_offline_time} * 1000)) AS time_diff
	       ,DENSE_RANK() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY  nf.capture_time - (radius.capture_time + {nf_offline_time} * 1000)) AS rn
	FROM
	(
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		FROM v64_deye_dw_ods.ods_nf_other_log_store
		WHERE capture_day = '{cal_day}'
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
		AND user_name <> strsrc_ip
		AND user_name not LIKE '213%' 
		AND uparea_id = '{uparea_id}'
		UNION ALL
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		FROM v64_deye_dw_ods.ods_nf_url_store
		WHERE capture_day = '{cal_day}'
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}'
		AND user_name <> strsrc_ip
		AND user_name not LIKE '213%' 
		AND uparea_id = '{uparea_id}'
	) nf
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
	ON nf.strsrc_ip = radius.ip
	WHERE nf.capture_time >= radius.capture_time + {nf_offline_time} * 1000 
) tmp
WHERE rn = 1