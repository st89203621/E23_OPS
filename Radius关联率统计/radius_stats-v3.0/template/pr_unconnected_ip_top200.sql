-- 统计昨日没关联上纯IP的TOP200用户（账号为空的IP地址统计）
-- 查询条件：auth_account为空或null的记录，按IP地址分组统计
SELECT 
    strsrc_ip,
    COUNT(*) as record_count,
    COUNT(DISTINCT capture_hour) as active_hours,
    MIN(capture_time) as first_access_time,
    MAX(capture_time) as last_access_time,
    COLLECT_SET(DISTINCT host) as accessed_hosts,
    COLLECT_SET(DISTINCT dst_port) as dst_ports,
    uparea_id
FROM (
    -- HTTP协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_http_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)  -- 昨日数据
    AND (auth_account IS NULL OR auth_account = '')   -- 账号为空，即没关联上
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- IM协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_im_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- EMAIL协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_email_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- 远程控制协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_remotectrl_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- FTP协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_ftp_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- 游戏协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_game_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- P2P协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_p2p_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- TELNET协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_telnet_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- VPN协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_vpn_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
    
    UNION ALL
    
    -- 硬件指纹协议数据
    SELECT 
        strsrc_ip,
        auth_account,
        capture_time,
        capture_hour,
        host,
        dst_port,
        uparea_id
    FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL 
    AND strsrc_ip != ''
) all_pr_data
GROUP BY strsrc_ip, uparea_id
ORDER BY record_count DESC
LIMIT 200;
