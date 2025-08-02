#!/bin/bash
# PR前天关联准确度统计
source /etc/profile
day_before=$(date -d "2 days ago" +"%Y-%m-%d")
mkdir -p ./output_new/$day_before

# PR移网关联准确度
for uparea_id in 210213 220214 230215; do
    spark-sql --master yarn --name "pr_mob_acc_${day_before}_${uparea_id}" --conf spark.driver.memory=2g --conf spark.executor.memory=12g --conf spark.executor.instances=16 -e "
    SELECT 'pr移网关联准确率' AS cal_type, '$day_before' AS cal_day, '00-23' AS cal_hour, '$uparea_id' AS uparea_id,
           SUM(if(calling_station_id = auth_account,1,0)) AS connect_account, SUM(1) AS total_account,
           SUM(if(calling_station_id = auth_account,1,0)) / SUM(1) AS acc_rate
    FROM (
        SELECT pr.strsrc_ip, pr.auth_account, pr.capture_time, radius.calling_station_id,
               DENSE_RANK() OVER (PARTITION BY pr.strsrc_ip,pr.auth_account,pr.capture_time ORDER BY pr.capture_time - radius.capture_time) AS rn
        FROM (
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_http_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_im_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_mail_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, src_port, capture_time FROM v64_deye_dw_ods.ods_pr_ftp_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account LIKE '213%' AND uparea_id = '$uparea_id'
        ) pr
        JOIN (
            SELECT internet_ip, calling_station_id, cast(split(port_range,'-')[0] AS int) AS start_port,
                   cast(split(port_range,'-')[1] AS int) AS end_port, capture_time
            FROM v64_deye_dw_ods.ods_mobilenet_radius_mobilis_store
            WHERE capture_day = '$day_before' AND action = 'Start'
            GROUP BY internet_ip,calling_station_id,port_range,capture_time
        ) radius ON pr.strsrc_ip = radius.internet_ip AND pr.src_port >= start_port AND pr.src_port <= end_port
        WHERE pr.capture_time >= radius.capture_time
    ) tmp WHERE rn = 1;" >> ./output_new/$day_before/pr_acc_${day_before}_${uparea_id}.csv &
done

# PR固网关联准确度
for uparea_id in 210213 220214 230215; do
    spark-sql --master yarn --name "pr_fix_acc_${day_before}_${uparea_id}" --conf spark.driver.memory=2g --conf spark.executor.memory=12g --conf spark.executor.instances=16 -e "
    SELECT 'pr固网关联准确率' AS cal_type, '$day_before' AS cal_day, '00-23' AS cal_hour, '$uparea_id' AS uparea_id,
           SUM(IF(auth_account = account,1,0)) AS connect_account, SUM(1) AS total_account,
           SUM(IF(auth_account = account,1,0)) / SUM(1) AS acc_rate
    FROM (
        SELECT pr.strsrc_ip, pr.auth_account, pr.capture_time, radius.account,
               DENSE_RANK() OVER (PARTITION BY pr.strsrc_ip,pr.auth_account,pr.capture_time ORDER BY pr.capture_time - radius.capture_time) AS rn
        FROM (
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_http_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_im_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_mail_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
            UNION ALL
            SELECT strsrc_ip, auth_account, capture_time FROM v64_deye_dw_ods.ods_pr_ftp_store
            WHERE capture_day = '$day_before' AND capture_hour BETWEEN '00' AND '23' AND auth_account is not null AND auth_account <> '' AND uparea_id = '$uparea_id'
        ) pr
        JOIN (
            SELECT ip, account, capture_time FROM v64_deye_dw_ods.ods_fixnet_radius_store
            WHERE capture_day = '$day_before' AND ACTION IN ('login', 'logout')
        ) radius ON pr.strsrc_ip = radius.ip
        WHERE pr.capture_time >= radius.capture_time
    ) tmp WHERE rn = 1;" >> ./output_new/$day_before/pr_fix_acc_${day_before}_${uparea_id}.csv &
done

wait
echo "PR前天关联准确度统计完成: $day_before"
