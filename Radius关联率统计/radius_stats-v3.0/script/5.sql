SELECT  *
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE (capture_day = '2025-05-16' or capture_day = '2025-05-15')
	AND action in ('Start', 'Stop')
	AND ((internet_ip = '154.121.102.140' and port_range = CONCAT(1024 + (FLOOR((65076 - 1024) / 126)) * 126,'-',1024 + (FLOOR((65076 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.102.23' and port_range = CONCAT(1024 + (FLOOR((17514 - 1024) / 126)) * 126,'-',1024 + (FLOOR((17514 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.107.159' and port_range = CONCAT(1024 + (FLOOR((19678 - 1024) / 126)) * 126,'-',1024 + (FLOOR((19678 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.107.54' and port_range = CONCAT(1024 + (FLOOR((38630 - 1024) / 126)) * 126,'-',1024 + (FLOOR((38630 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.110.219' and port_range = CONCAT(1024 + (FLOOR((49304 - 1024) / 126)) * 126,'-',1024 + (FLOOR((49304 - 1024) / 126)) * 126 + 125)));