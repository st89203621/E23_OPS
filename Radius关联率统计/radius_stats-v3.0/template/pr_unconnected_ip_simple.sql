-- 简化版：统计昨日没关联上纯IP的TOP200（仅统计IP和记录数）
-- 适用于快速查询，只关注IP地址和访问次数
SELECT 
    strsrc_ip,
    COUNT(*) as record_count
FROM (
    -- 合并所有PR协议表的数据
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_http_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_im_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_email_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_remotectrl_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_ftp_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_game_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_p2p_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_telnet_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_vpn_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
    
    UNION ALL
    
    SELECT strsrc_ip FROM v64_deye_dw_ods.ods_pr_hardwarestring_store
    WHERE capture_day = DATE_SUB(CURRENT_DATE(), 1)
    AND (auth_account IS NULL OR auth_account = '')
    AND strsrc_ip IS NOT NULL AND strsrc_ip != ''
) all_unconnected_ips
GROUP BY strsrc_ip
ORDER BY record_count DESC
LIMIT 200;
