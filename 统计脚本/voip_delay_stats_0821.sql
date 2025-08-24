-- 统计8月21日，ods_pr_voip_store表中动作类型为772，时延超过30分钟，取top100
SELECT 
    from_id,
    to_id,
    strsrc_ip,
    strdst_ip,
    action,
    postal_time,
    capture_time,
    DATE_FORMAT(from_unixtime(cast(capture_time / 1000 as bigint)), 'yyyy-MM-dd HH:mm:ss') as capture_datetime,
    ROUND((capture_time - postal_time) / 60000, 2) as delay_minutes,
    user_nick,
    tool_name,
    data_id
FROM v64_deye_dw_ods.ods_pr_voip_store
WHERE capture_day = '2025-08-21'
AND action = '772'
AND (capture_time - postal_time) > 30 * 60 * 1000  -- 30分钟转毫秒
ORDER BY (capture_time - postal_time) DESC
LIMIT 100;