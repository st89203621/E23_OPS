SELECT  strsrc_ip
       ,user_name
       ,capture_time
       ,data_type
       ,data_id
       ,account
       ,r_capture_time
FROM
(
	SELECT  nf.strsrc_ip
	       ,nf.user_name
	       ,nf.capture_time
	       ,nf.data_type
	       ,nf.data_id
	       ,radius.account
	       ,radius.capture_time                                                AS r_capture_time
	       ,(nf.capture_time - radius.capture_time + {nf_offline_time} * 1000) AS time_diff
	       ,ROW_NUMBER() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY  nf.capture_time - radius.capture_time + {nf_offline_time} * 1000) AS rn
	FROM
	(
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_nf_other_log_store
		WHERE capture_day = '{cal_day}'
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
		UNION ALL
		SELECT  user_name
		       ,strsrc_ip
		       ,capture_time
		       ,data_type
		       ,data_id
		FROM v64_deye_dw_ods.ods_nf_url_store
		WHERE capture_day = '{cal_day}'
		AND capture_hour BETWEEN '{start_hour}' AND '{end_hour}' 
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
	) radius
	ON nf.strsrc_ip = radius.ip
	WHERE nf.capture_time >= radius.capture_time + {nf_offline_time} * 1000 
) tmp
WHERE rn = 1
AND user_name <> account