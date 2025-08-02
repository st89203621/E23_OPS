SELECT  user_name
	       ,strsrc_ip
	       ,src_port,capture_time ,dev_id 
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-14'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-14 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-14 08:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
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
	AND ((strsrc_ip = '154.121.102.100' and src_port >= 1024 + (FLOOR((7957 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((7957 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.101' and src_port >= 1024 + (FLOOR((2351 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((2351 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.104' and src_port >= 1024 + (FLOOR((3104 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((3104 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.105' and src_port >= 1024 + (FLOOR((9847 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((9847 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.109' and src_port >= 1024 + (FLOOR((3052 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((3052 - 1024) / 126)) * 126 + 125))
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port,capture_time ,dev_id 
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-05-14'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-14 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-14 08:00:00') * 1000 + 1 * 60 * 60 * 1000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
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
	AND ((strsrc_ip = '154.121.102.100' and src_port >= 1024 + (FLOOR((7957 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((7957 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.101' and src_port >= 1024 + (FLOOR((2351 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((2351 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.104' and src_port >= 1024 + (FLOOR((3104 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((3104 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.105' and src_port >= 1024 + (FLOOR((9847 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((9847 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.109' and src_port >= 1024 + (FLOOR((3052 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((3052 - 1024) / 126)) * 126 + 125));