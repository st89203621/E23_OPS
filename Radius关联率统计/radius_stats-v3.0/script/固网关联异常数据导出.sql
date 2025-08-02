SELECT  *
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-04-28'
	AND insert_hour BETWEEN '00' AND '13'
	and capture_time >= 1746016800000
	and capture_time < 1746020400000
	AND user_name not LIKE '2136%'
	GROUP BY  user_name
	         ,strsrc_ip
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-04-28'
	AND insert_hour BETWEEN '00' AND '13'
	and capture_time >= 1746016800000
	and capture_time < 1746020400000
	AND user_name not LIKE '2136%'
	GROUP BY  user_name
	         ,strsrc_ip
) nf
JOIN
(
	SELECT  ip,account
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-04-28'
	AND capture_hour BETWEEN '00' AND '12'
	GROUP BY  ip,account
) radius
ON radius.ip = nf.strsrc_ip and user_name = strsrc_ip;