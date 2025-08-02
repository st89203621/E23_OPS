SELECT  nf.user_name, nf.strsrc_ip
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-16'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND user_name not LIKE '2136%' 
	AND dev_id in (
		'78b822bd',
		'484369c1',
		'10411f77',
		'f65c9036',
		'f621e11e',
		'd9347bd0',
		'40ba6628',
		'5d96e70e',
		'1281214e',
		'b010fecd',
		'733b606e',
		'7376a8cf',
		'cd7f1050',
		'8e4f4bff',
		'44f9077d'
	)
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-05-16'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND user_name not LIKE '2136%' 
	AND dev_id in (
		'78b822bd',
		'484369c1',
		'10411f77',
		'f65c9036',
		'f621e11e',
		'd9347bd0',
		'40ba6628',
		'5d96e70e',
		'1281214e',
		'b010fecd',
		'733b606e',
		'7376a8cf',
		'cd7f1050',
		'8e4f4bff',
		'44f9077d'
	)
) nf
JOIN
(
	SELECT  ip
	FROM test_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-05-16'
	GROUP BY  ip
) radius
ON radius.ip = nf.strsrc_ip
where nf.user_name = nf.strsrc_ip
group by nf.user_name
limit 1000;


SELECT  user_name
	       ,strsrc_ip,capture_time ,dev_id 
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-16'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND user_name not LIKE '2136%' 
	AND dev_id in (
		'78b822bd',
		'484369c1',
		'10411f77',
		'f65c9036',
		'f621e11e',
		'd9347bd0',
		'40ba6628',
		'5d96e70e',
		'1281214e',
		'b010fecd',
		'733b606e',
		'7376a8cf',
		'cd7f1050',
		'8e4f4bff',
		'44f9077d'
	)
	AND strsrc_ip in('100.72.248.221','100.70.87.10','100.73.204.63','105.100.1.239','105.100.102.56','105.100.108.173','105.100.12.86','105.100.127.93','105.100.130.47','105.100.131.9')
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip,capture_time ,dev_id 
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-05-16'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND user_name not LIKE '2136%' 
	AND dev_id in (
		'78b822bd',
		'484369c1',
		'10411f77',
		'f65c9036',
		'f621e11e',
		'd9347bd0',
		'40ba6628',
		'5d96e70e',
		'1281214e',
		'b010fecd',
		'733b606e',
		'7376a8cf',
		'cd7f1050',
		'8e4f4bff',
		'44f9077d'
	)
	AND strsrc_ip in('100.72.248.221','100.70.87.10','100.73.204.63','105.100.1.239','105.100.102.56','105.100.108.173','105.100.12.86','105.100.127.93','105.100.130.47','105.100.131.9');

SELECT  *
	FROM test_dw_ods.ods_fixnet_radius_store
	WHERE (capture_day = '2025-05-15' OR capture_day = '2025-05-16')
	AND ip in('100.72.248.221','100.70.87.10','100.73.204.63','105.100.1.239','105.100.102.56','105.100.108.173','105.100.12.86','105.100.127.93','105.100.130.47','105.100.131.9');