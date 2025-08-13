-- 模板版：统计指定日期没关联上纯IP的TOP200
-- 使用参数：{cal_day} - 统计日期
-- 查询条件：auth_account为空或null的记录
SELECT 
    strsrc_ip,
    COUNT(*) as record_count,
    COUNT(DISTINCT CASE 
        WHEN capture_hour IS NOT NULL THEN capture_hour 
        ELSE NULL 
    END) as active_hours,
    uparea_id
FROM (
    -- HTTP协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_http_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- IM协议数据  
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_im_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- EMAIL协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_email_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- 远程控制协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_remotectrl_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- FTP协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_ftp_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- 游戏协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_game_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- P2P协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_p2p_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- TELNET协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_telnet_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- VPN协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_vpn_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    -- 硬件指纹协议数据
    SELECT strsrc_ip, capture_hour, uparea_id
    FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
    WHERE capture_day = '{cal_day}'
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
) all_unconnected_data
GROUP BY strsrc_ip, uparea_id
ORDER BY record_count DESC
LIMIT 200;
