SELECT  user_name, strsrc_ip, src_port
FROM
(
        SELECT  user_name
               ,strsrc_ip
               ,src_port
        FROM v64_deye_dw_ods.ods_nf_other_log
        WHERE insert_day = '2025-05-16'
        AND insert_hour BETWEEN '00' AND '08'
        AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
        AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
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
        UNION ALL
        SELECT  user_name
               ,strsrc_ip
               ,src_port
        FROM v64_deye_dw_ods.ods_nf_url
        WHERE insert_day = '2025-05-16'
        AND insert_hour BETWEEN '00' AND '08'
        AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
        AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
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
) nf
where EXISTS (
        select 1 from (
        SELECT  internet_ip
               ,cast(split(port_range,'-')[0] AS int) AS start_port
               ,cast(split(port_range,'-')[1] AS int) AS end_port
        FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
        WHERE capture_day = '2025-05-16'
        AND action in ('Start', 'Stop')
        GROUP BY internet_ip,port_range) radius
        where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
) and user_name = strsrc_ip
limit 2000;


SELECT  user_name
	       ,strsrc_ip
	       ,src_port,capture_time ,dev_id 
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-16'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
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
	AND ((strsrc_ip = '154.121.102.140' and src_port >= 1024 + (FLOOR((65076 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((65076 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.23' and src_port >= 1024 + (FLOOR((17514 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((17514 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.107.159' and src_port >= 1024 + (FLOOR((19678 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((19678 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.107.54' and src_port >= 1024 + (FLOOR((38630 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((38630 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.110.219' and src_port >= 1024 + (FLOOR((49304 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((49304 - 1024) / 126)) * 126 + 125))
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	       ,src_port,capture_time ,dev_id 
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-05-16'
	AND insert_hour BETWEEN '00' AND '08'
	AND capture_time >= unix_timestamp('2025-05-16 00:00:00') * 1000
	AND capture_time < unix_timestamp('2025-05-16 08:00:00') * 1000 + 1 * 60 * 60 * 1000
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
	AND ((strsrc_ip = '154.121.102.140' and src_port >= 1024 + (FLOOR((65076 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((65076 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.102.23' and src_port >= 1024 + (FLOOR((17514 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((17514 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.107.159' and src_port >= 1024 + (FLOOR((19678 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((19678 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.107.54' and src_port >= 1024 + (FLOOR((38630 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((38630 - 1024) / 126)) * 126 + 125)
	OR (strsrc_ip = '154.121.110.219' and src_port >= 1024 + (FLOOR((49304 - 1024) / 126)) * 126 and src_port <= 1024 + (FLOOR((49304 - 1024) / 126)) * 126 + 125))
;

SELECT  *
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE (capture_day = '2025-05-16' or capture_day = '2025-05-15')
	AND action in ('Start', 'Stop')
	AND ((internet_ip = '154.121.102.140' and port_range = CONCAT(1024 + (FLOOR((65076 - 1024) / 126)) * 126,'-',1024 + (FLOOR((65076 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.102.23' and port_range = CONCAT(1024 + (FLOOR((17514 - 1024) / 126)) * 126,'-',1024 + (FLOOR((17514 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.107.159' and port_range = CONCAT(1024 + (FLOOR((19678 - 1024) / 126)) * 126,'-',1024 + (FLOOR((19678 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.107.54' and port_range = CONCAT(1024 + (FLOOR((38630 - 1024) / 126)) * 126,'-',1024 + (FLOOR((38630 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.110.219' and port_range = CONCAT(1024 + (FLOOR((49304 - 1024) / 126)) * 126,'-',1024 + (FLOOR((49304 - 1024) / 126)) * 126 + 125)));