#!/bin/bash
# NF前天关联准确度统计
source /etc/profile
day_before=$(date -d "2 days ago" +"%Y-%m-%d")
mkdir -p ./output_new/$day_before

# NF移网关联准确度
for uparea_id in 210213 220214 230215; do
    spark-sql --master yarn --name "nf_mob_acc_${day_before}_${uparea_id}" --conf spark.driver.memory=2g --conf spark.executor.memory=12g --conf spark.executor.instances=16 -e "
    SELECT 'nf移网关联准确率' AS cal_type, '$day_before' AS cal_day, '00-23' AS cal_hour, '$uparea_id' AS uparea_id,
           SUM(if(calling_station_id = user_name,1,0)) AS connect_account, SUM(1) AS total_account,
           SUM(if(calling_station_id = user_name,1,0)) / SUM(1) AS acc_rate
    FROM (
        SELECT nf.strsrc_ip, nf.user_name, nf.capture_time, radius.calling_station_id,
               DENSE_RANK() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY nf.capture_time - radius.capture_time) AS rn
        FROM (
            SELECT strsrc_ip, user_name, src_port, capture_time FROM v64_deye_dw_ods.ods_nf_other_log_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND user_name LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, user_name, src_port, capture_time FROM v64_deye_dw_ods.ods_nf_url_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND user_name LIKE '213%' AND uparea_id = '$uparea_id'
        ) nf
        JOIN (
            SELECT internet_ip, calling_station_id, cast(split(port_range,'-')[0] AS int) AS start_port,
                   cast(split(port_range,'-')[1] AS int) AS end_port, capture_time
            FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
            WHERE capture_day = '$day_before' AND action = 'Start'
            GROUP BY internet_ip,calling_station_id,port_range,capture_time
        ) radius ON nf.strsrc_ip = radius.internet_ip AND nf.src_port >= start_port AND nf.src_port <= end_port
        WHERE nf.capture_time >= radius.capture_time
    ) tmp WHERE rn = 1;" >> ./output_new/$day_before/nf_acc_${day_before}_${uparea_id}.csv &
done

# NF固网关联准确度
for uparea_id in 210213 220214 230215; do
    spark-sql --master yarn --name "nf_fix_acc_${day_before}_${uparea_id}" --conf spark.driver.memory=2g --conf spark.executor.memory=12g --conf spark.executor.instances=16 -e "
    SELECT 'nf固网关联准确率' AS cal_type, '$day_before' AS cal_day, '00-23' AS cal_hour, '$uparea_id' AS uparea_id,
           SUM(IF(lower(user_name) = lower(account),1,0)) AS connect_account, SUM(1) AS total_account,
           SUM(IF(lower(user_name) = lower(account),1,0)) / SUM(1) AS acc_rate
    FROM (
        SELECT nf.strsrc_ip, nf.user_name, nf.capture_time, radius.account,
               DENSE_RANK() OVER (PARTITION BY nf.strsrc_ip,nf.user_name,nf.capture_time ORDER BY nf.capture_time - radius.capture_time) AS rn
        FROM (
            SELECT strsrc_ip, user_name, capture_time FROM v64_deye_dw_ods.ods_nf_other_log_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND user_name not LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, user_name, capture_time FROM v64_deye_dw_ods.ods_nf_url_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND user_name not LIKE '213%' AND uparea_id = '$uparea_id'
        ) nf
        JOIN (
            SELECT ip, account, capture_time FROM v64_deye_dw_ods.ods_fixnet_radius_store
            WHERE capture_day = '$day_before' AND ACTION IN ('login', 'logout')
        ) radius ON nf.strsrc_ip = radius.ip
        WHERE nf.capture_time >= radius.capture_time
    ) tmp WHERE rn = 1;" >> ./output_new/$day_before/nf_fix_acc_${day_before}_${uparea_id}.csv &
done

wait
echo "NF前天关联准确度统计完成: $day_before"
