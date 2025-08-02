SELECT  *
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE (capture_day = '2025-05-14' or capture_day = '2025-05-13')
	AND action in ('Start', 'Stop')
	AND ((internet_ip = '154.121.102.100' and port_range = CONCAT(1024 + (FLOOR((7957 - 1024) / 126)) * 126,'-',1024 + (FLOOR((7957 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.102.101' and port_range = CONCAT(1024 + (FLOOR((2351 - 1024) / 126)) * 126,'-',1024 + (FLOOR((2351 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.102.104' and port_range = CONCAT(1024 + (FLOOR((3104 - 1024) / 126)) * 126,'-',1024 + (FLOOR((3104 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.102.105' and port_range = CONCAT(1024 + (FLOOR((9847 - 1024) / 126)) * 126,'-',1024 + (FLOOR((9847 - 1024) / 126)) * 126 + 125))
	OR (internet_ip = '154.121.102.109' and port_range = CONCAT(1024 + (FLOOR((3052 - 1024) / 126)) * 126,'-',1024 + (FLOOR((3052 - 1024) / 126)) * 126 + 125)));