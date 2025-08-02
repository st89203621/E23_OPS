--导入移动网未关联的ip
insert into v64_deye_dw_ods.ods_nf_con_fail_tmp
SELECT  *
FROM
(
	SELECT DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd HH:mm:ss') as capture_date,
		   ,user_name
	       ,strsrc_ip
	       ,src_port
	       ,CONCAT(1024 + (FLOOR((src_port - 1024) / 126)) * 126,'-',1024 + (FLOOR((src_port - 1024) / 126)) * 126 + 125) as port_range
	       ,dev_id
	       ,capture_time
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd') as capture_day
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'HH') as capture_hour
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-05-03'
	AND insert_hour BETWEEN '00' AND '09'
	and capture_time >= 1746226800000
	and capture_time < 1746259200000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
	UNION 
	SELECT DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd HH:mm:ss') as capture_date,
		   ,user_name
	       ,strsrc_ip
	       ,src_port
	       ,CONCAT(1024 + (FLOOR((src_port - 1024) / 126)) * 126,'-',1024 + (FLOOR((src_port - 1024) / 126)) * 126 + 125) as port_range
	       ,dev_id
	       ,capture_time
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'yyyy-MM-dd') as capture_day
	       ,DATE_FORMAT(from_unixtime(cast(capture_time  / 1000 as bigint)), 'HH') as capture_hour
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-05-03'
	AND insert_hour BETWEEN '00' AND '09'
	and capture_time >= 1746226800000
	and capture_time < 1746259200000
	AND (user_name LIKE '2136%' or user_name = strsrc_ip)
) nf
where EXISTS (
	select 1 from (
	SELECT  internet_ip
	       ,cast(split(port_range,'-')[0] AS int) AS start_port
	       ,cast(split(port_range,'-')[1] AS int) AS end_port
	FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
	WHERE capture_day = '2025-05-03'
	GROUP BY internet_ip,port_range) radius
	where nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
) and user_name like '%.%';

--导出指定ip的radius数据明细
select * from ods_fixnet_radius_store where capture_day = '2025-04-30' AND capture_hour BETWEEN '13' AND '15'
	and ip in (
	'197.207.116.97','154.255.134.244','41.105.70.87','105.110.198.78'
) order by capture_time ;

--导出指定ip的nf数据明细
select * from (
SELECT  user_name,strsrc_ip,capture_time,dev_id 
	FROM v64_deye_dw_ods.ods_nf_other_log
	WHERE insert_day = '2025-04-30'
	AND insert_hour BETWEEN '13' AND '15'
	AND capture_time >= 1746016800000
	AND capture_time < 1746024000000
	AND user_name not LIKE '2136%'
	and strsrc_ip in (
		'197.207.116.97','154.255.134.244','41.105.70.87','105.110.198.78'
	)
union all
	SELECT  user_name,strsrc_ip,capture_time,dev_id
	FROM v64_deye_dw_ods.ods_nf_url
	WHERE insert_day = '2025-04-30'
	AND insert_hour BETWEEN '13' AND '15'
	AND capture_time >= 1746016800000
	AND capture_time < 1746024000000
	AND user_name not LIKE '2136%'
	and strsrc_ip in (
		'197.207.116.97','154.255.134.244','41.105.70.87','105.110.198.78'
	)
) tmp order by capture_time ;

--导出指定ip的pr数据明细
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_im_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_http_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_email_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_ftp_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_remotectrl_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_telnet_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_game_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_p2p_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_vpn_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205') 
UNION ALL
SELECT  auth_account
       ,auth_type
       ,strsrc_ip
       ,trace_id
       ,capture_time
       ,data_type
       ,data_id
FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND strsrc_ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205');
--导出指定ip的信令数据明细 
SELECT  *
FROM v64_deye_dw_ods.ods_fixnet_radius_store
WHERE capture_day BETWEEN '2025-04-16' AND '2025-04-18'
AND ip IN ( '1.172.30.80', '1.22.180.245', '1.24.16.254', '1.32.216.205' );

SELECT  pr.strsrc_ip
       ,pr.trace_id
FROM
(
	SELECT  ip
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-04-19'
	AND capture_hour BETWEEN '00' AND '23'
	GROUP BY  ip
) radius
LEFT JOIN
(
	SELECT  strsrc_ip
	       ,auth_account
	       ,trace_id
	FROM
	(
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_http_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_im_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_email_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_remotectrl_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_ftp_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_game_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_p2p_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_telnet_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_vpn_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
		UNION ALL
		SELECT  strsrc_ip
		       ,auth_account
		       ,trace_id
		FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
		WHERE capture_day = '2025-04-19'
		AND capture_hour BETWEEN '00' AND '23'
		GROUP BY  strsrc_ip
		         ,auth_account
		         ,trace_id
	) tmp
	GROUP BY  strsrc_ip
	         ,auth_account
	         ,trace_id
) pr
ON radius.ip = pr.strsrc_ip
WHERE pr.strsrc_ip is not null;

SELECT  count(1)
FROM
(
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_other_log_store
	WHERE capture_day = '2025-04-28'
	AND user_name = strsrc_ip
	GROUP BY  user_name
	         ,strsrc_ip
	UNION ALL
	SELECT  user_name
	       ,strsrc_ip
	FROM v64_deye_dw_ods.ods_nf_url_store
	WHERE capture_day = '2025-04-28'
	AND user_name = strsrc_ip
	GROUP BY  user_name
	         ,strsrc_ip
) nf
JOIN
(
	SELECT  ip
	FROM v64_deye_dw_ods.ods_fixnet_radius_store
	WHERE capture_day = '2025-04-28'
	GROUP BY  ip
) radius
ON radius.ip = nf.strsrc_ip;

select
	capture_hour ,
	count(1)
from
	ods_mobilenet_radius_mobilis_store
where
	capture_day = '2025-04-29'
	and `action` in ('Start','Stop')
group by
	capture_hour
order by
	capture_hour ;

SELECT  strsrc_ip ,user_name ,capture_time
		FROM v64_deye_dw_ods.ods_nf_other_log_store
		WHERE capture_day = '2025-04-25'
		AND capture_hour BETWEEN '13' AND '13'
		AND user_name <> strsrc_ip
		AND strsrc_ip in ('105.100.1.150','105.100.101.3','105.100.108.241','105.100.112.149')
union all
	SELECT  strsrc_ip ,user_name ,capture_time 
		FROM v64_deye_dw_ods.ods_nf_url_store
		WHERE capture_day = '2025-04-25'
		AND capture_hour BETWEEN '13' AND '13'
		AND user_name <> strsrc_ip
		AND strsrc_ip in ('105.100.1.150','105.100.101.3','105.100.108.241','105.100.112.149');
		
	
select * from ods_radius_his where capture_day = '2025-04-25'
	AND capture_hour BETWEEN '13' AND '13'
	and ip in ('105.100.1.150','105.100.101.3','105.100.108.241','105.100.112.149');
	
select * from ods_fixnet_radius_store where capture_day = '2025-04-27'
	and ip in ('105.100.0.176','105.100.0.51','105.100.0.86','105.100.1.145');
		

select 
date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'yyyy-MM-dd') as capture_day, 
date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'HH') as capture_hour,
count(1)
from ods_radius_tmp
where insert_day = '2025-04-25' and insert_hour = '13'
group by date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'yyyy-MM-dd'), 
date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'HH');

select count(1) from ods_radius_his where capture_day = '2025-04-25' and capture_hour = '13';

	SELECT  account
	       ,ip
	FROM v64_deye_dw_ods.ods_radius_his 
	WHERE capture_day = '2025-04-25'
	AND capture_hour BETWEEN '12' AND '13'
	AND action = 'login' ;	


select
	date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'mm') as mm,
	count(1)
from
	ods_mobilenet_radius_mobilis_store
where
	capture_day = '2025-04-24'
	and capture_hour = '13'
	and `action` in ('Start', 'Stop')
	and calling_station_id ='213659266633' 
group by date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'mm')
order by mm
;


select
	calling_station_id,
	count(1) as cnt
from
	ods_mobilenet_radius_mobilis_store
where
	capture_day = '2025-04-24'
	and capture_hour = '13'
	and `action` in ('Start', 'Stop')
group by calling_station_id
order by cnt desc
;

select * from ods_mobilenet_radius_mobilis_store where capture_day = '2025-04-24'
and capture_hour = '13'
and date_format(from_unixtime(cast(capture_time / 1000 as bigint)), 'mm') = '00'
	and `action` in ('Start', 'Stop')
and calling_station_id ='213659266633' 
;

		
select sum(if(internet_ip = '' and calling_station_id <> '', 1, 0)),count(1) from ods_mobilenet_radius_mobilis_store where capture_day = '2025-04-25';


select * from ods_mobilenet_radius_mobilis_store where capture_day = '2025-04-25' and internet_ip = '' and calling_station_id <> '' limit 2000;

select
capture_day,
sum(if(user_name like '%.%' or user_name like '%:%',1,0)) as ip,
sum(if(user_name like '2136%',1,0)) as mobile,
sum(1) - sum(if(user_name like '%.%' or user_name like '%:%',1,0)) - sum(if(user_name like '2136%',1,0)) as fix
from(
select user_name,capture_day from v64_deye_dw_ods.ods_nf_other_log_store where capture_day = '2025-04-28'
union all
select user_name,capture_day  from v64_deye_dw_ods.ods_nf_url_store where capture_day = '2025-04-28'
) tmp
group by capture_day;
