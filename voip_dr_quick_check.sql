-- VOIP灾备数据快速一致性检查
-- 时间段: 2025-08-02 08:10:00 到 2025-08-02 09:00:00
-- 快速对比 dr-voip 和 voip2 的数据一致性

-- =====================================================
-- 1. 基础统计对比
-- =====================================================

SELECT
    'DR-VOIP' AS data_source,
    COUNT(*) AS total_records,
    COUNT(DISTINCT call_id) AS unique_calls,
    MIN(from_unixtime(CAST(capture_time/1000 AS BIGINT))) AS earliest_time,
    MAX(from_unixtime(CAST(capture_time/1000 AS BIGINT))) AS latest_time,
    ROUND(AVG(duration), 2) AS avg_duration_seconds
FROM v64_deye_dw_ods.ods_voip_dr_test_store
WHERE capture_day = '2025-08-02'
  AND capture_hour = '08'
  AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
  AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000

UNION ALL

SELECT
    'VOIP2' AS data_source,
    COUNT(*) AS total_records,
    COUNT(DISTINCT call_id) AS unique_calls,
    MIN(from_unixtime(CAST(capture_time/1000 AS BIGINT))) AS earliest_time,
    MAX(from_unixtime(CAST(capture_time/1000 AS BIGINT))) AS latest_time,
    ROUND(AVG(duration), 2) AS avg_duration_seconds
FROM v64_deye_dw_ods.ods_voip_store
WHERE capture_day = '2025-08-02'
  AND capture_hour = '08'
  AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
  AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000;

-- =====================================================
-- 3. 具体不一致记录示例 (前5条)
-- =====================================================

SELECT
    'inconsistent_records_sample' AS description,
    d.call_id AS call_id,
    from_unixtime(CAST(d.capture_time/1000 AS BIGINT)) AS capture_time,
    CONCAT('DR:', d.a_number, ' | VOIP2:', v.a_number) AS a_number_comparison,
    CONCAT('DR:', d.b_number, ' | VOIP2:', v.b_number) AS b_number_comparison,
    CONCAT('DR:', d.duration, ' | VOIP2:', v.duration) AS duration_comparison,
    CONCAT('DR:', d.action, ' | VOIP2:', v.action) AS action_comparison
FROM (
    SELECT call_id, a_number, b_number, duration, action, capture_time
    FROM v64_deye_dw_ods.ods_voip_dr_test_store
    WHERE capture_day = '2025-08-02'
      AND capture_hour = '08'
      AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
      AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000
) d
JOIN (
    SELECT call_id, a_number, b_number, duration, action, capture_time
    FROM v64_deye_dw_ods.ods_voip_store
    WHERE capture_day = '2025-08-02'
      AND capture_hour = '08'
      AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
      AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000
) v ON d.call_id = v.call_id
WHERE d.a_number != v.a_number 
   OR d.b_number != v.b_number 
   OR d.duration != v.duration 
   OR d.action != v.action
LIMIT 5;

-- =====================================================
-- 4. 一致性结论
-- =====================================================

WITH consistency_check AS (
    SELECT 
        (SELECT COUNT(*) FROM v64_deye_dw_ods.ods_voip_dr_test_store
         WHERE capture_day = '2025-08-02' AND capture_hour = '08'
           AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
           AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000) AS dr_count,
        
        (SELECT COUNT(*) FROM v64_deye_dw_ods.ods_voip_store
         WHERE capture_day = '2025-08-02' AND capture_hour = '08'
           AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
           AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000) AS voip2_count,
           
        (SELECT COUNT(*) FROM v64_deye_dw_ods.ods_voip_dr_test_store dr
         WHERE dr.capture_day = '2025-08-02' AND dr.capture_hour = '08'
           AND dr.capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
           AND dr.capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000
           AND EXISTS (
               SELECT 1 FROM v64_deye_dw_ods.ods_voip_store v2
               WHERE v2.call_id = dr.call_id
                 AND v2.capture_day = '2025-08-02' AND v2.capture_hour = '08'
                 AND v2.capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
                 AND v2.capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000
           )) AS common_count
)

SELECT
    'consistency_conclusion' AS check_result,
    dr_count AS dr_records,
    voip2_count AS voip2_records,
    common_count AS common_records,
    ROUND(common_count * 100.0 / GREATEST(dr_count, voip2_count), 2) AS consistency_percentage,
    CASE
        WHEN dr_count = voip2_count AND common_count = dr_count THEN 'PERFECT_MATCH'
        WHEN ABS(dr_count - voip2_count) <= dr_count * 0.01 THEN 'GOOD_MATCH_DIFF_LT_1PCT'
        WHEN ABS(dr_count - voip2_count) <= dr_count * 0.05 THEN 'FAIR_MATCH_DIFF_LT_5PCT'
        ELSE 'SIGNIFICANT_DIFFERENCE'
    END AS consistency_status,
    CASE
        WHEN dr_count = voip2_count AND common_count = dr_count THEN 'DR_data_fully_synced_with_primary'
        WHEN common_count > 0 THEN CONCAT('DR_coverage_rate: ', ROUND(common_count * 100.0 / voip2_count, 2), '%')
        ELSE 'DR_data_may_have_sync_issues'
    END AS recommendation
FROM consistency_check;

-- =====================================================
-- 5. 按分钟统计对比 (可选)
-- =====================================================

SELECT
    'DR-VOIP' AS data_source,
    from_unixtime(CAST(FLOOR(capture_time/60000)*60 AS BIGINT), 'HH:mm') AS minute_interval,
    COUNT(*) AS record_count
FROM v64_deye_dw_ods.ods_voip_dr_test_store
WHERE capture_day = '2025-08-02'
  AND capture_hour = '08'
  AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
  AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000
GROUP BY from_unixtime(CAST(FLOOR(capture_time/60000)*60 AS BIGINT), 'HH:mm')

UNION ALL

SELECT
    'VOIP2' AS data_source,
    from_unixtime(CAST(FLOOR(capture_time/60000)*60 AS BIGINT), 'HH:mm') AS minute_interval,
    COUNT(*) AS record_count
FROM v64_deye_dw_ods.ods_voip_store
WHERE capture_day = '2025-08-02'
  AND capture_hour = '08'
  AND capture_time >= unix_timestamp('2025-08-02 08:10:00') * 1000
  AND capture_time <= unix_timestamp('2025-08-02 09:00:00') * 1000
GROUP BY from_unixtime(CAST(FLOOR(capture_time/60000)*60 AS BIGINT), 'HH:mm')
ORDER BY data_source, minute_interval;
